from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import APP_TITLE, DEFAULT_HISTORY_WINDOW, STATIC_DIR, TEMPLATES_DIR
from .db import init_db
from .schemas import ReportCreateRequest, ReportJobResponse, ReportResponse, UploadResponse
from .services.pdf_export import export_html_to_pdf
from .services.reports import (
    company_cards,
    company_quarters,
    create_report,
    create_report_job,
    ensure_report_payload_defaults,
    get_report,
    get_report_job,
    resolve_canonical_report_id,
    update_report_pdf,
)
from .services.uploads import create_upload


app = FastAPI(title=APP_TITLE)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.on_event("startup")
def startup() -> None:
    init_db()


def _asset_version() -> str:
    candidates = [
        STATIC_DIR / "app.css",
        STATIC_DIR / "app.js",
        TEMPLATES_DIR / "home.html",
    ]
    latest = max(int(path.stat().st_mtime) for path in candidates)
    return str(latest)


def _report_context(record: dict[str, Any], show_toolbar: bool) -> dict[str, Any]:
    payload = ensure_report_payload_defaults(record["payload"])
    css_text = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
    return {
        "app_title": APP_TITLE,
        "report_id": record["id"],
        "report": payload,
        "show_toolbar": show_toolbar,
        "embedded_css": css_text,
    }


def render_report_html(record: dict[str, Any], show_toolbar: bool) -> str:
    return templates.env.get_template("report_document.html").render(**_report_context(record, show_toolbar))


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "request": request,
            "app_title": APP_TITLE,
            "companies": company_cards(),
            "default_history_window": DEFAULT_HISTORY_WINDOW,
            "asset_version": _asset_version(),
        },
    )


@app.get("/companies")
def companies_endpoint() -> list[dict[str, Any]]:
    return company_cards()


@app.get("/companies/{company_id}/quarters")
def company_quarters_endpoint(company_id: str, history_window: int = DEFAULT_HISTORY_WINDOW) -> JSONResponse:
    try:
        payload = company_quarters(company_id, history_window)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(payload)


@app.post("/uploads", response_model=UploadResponse)
async def upload_material(file: UploadFile = File(...)) -> UploadResponse:
    raw_bytes = await file.read()
    try:
        payload = create_upload(file.filename or "upload.txt", file.content_type or "application/octet-stream", raw_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return UploadResponse(**payload)


@app.post("/reports", response_model=ReportResponse)
def create_report_endpoint(payload: ReportCreateRequest) -> ReportResponse:
    try:
        record = create_report(
            payload.company_id,
            payload.calendar_quarter,
            payload.history_window,
            payload.manual_transcript_upload_id,
            payload.force_refresh,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    report_payload = record["payload"]
    report_id = record["id"]
    return ReportResponse(
        report_id=report_id,
        company_id=record["company_id"],
        calendar_quarter=record["calendar_quarter"],
        structure_dimension_used=record["structure_dimension_used"],
        coverage_warnings=report_payload["coverage_warnings"],
        preview_url=f"/reports/{report_id}/preview",
        export_pdf_url=f"/reports/{report_id}/export.pdf",
        payload=report_payload,
    )


@app.post("/report-jobs", response_model=ReportJobResponse)
def create_report_job_endpoint(payload: ReportCreateRequest) -> ReportJobResponse:
    try:
        job = create_report_job(
            payload.company_id,
            payload.calendar_quarter,
            payload.history_window,
            payload.manual_transcript_upload_id,
            payload.force_refresh,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ReportJobResponse(**job)


@app.get("/report-jobs/{job_id}", response_model=ReportJobResponse)
def report_job_endpoint(job_id: str) -> ReportJobResponse:
    try:
        job = get_report_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ReportJobResponse(**job)


@app.get("/reports/{report_id}")
def report_endpoint(report_id: str) -> JSONResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        report_id = canonical_id
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(
        {
            "report_id": record["id"],
            "company_id": record["company_id"],
            "calendar_quarter": record["calendar_quarter"],
            "structure_dimension_used": record["structure_dimension_used"],
            "coverage_warnings": record["payload"]["coverage_warnings"],
            "preview_url": f"/reports/{record['id']}/preview",
            "export_pdf_url": f"/reports/{record['id']}/export.pdf",
            "payload": record["payload"],
        }
    )


@app.get("/reports/{report_id}/preview", response_class=HTMLResponse)
def report_preview(request: Request, report_id: str) -> HTMLResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        redirect_url = request.url.replace(path=f"/reports/{canonical_id}/preview")
        return RedirectResponse(str(redirect_url), status_code=307)
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return HTMLResponse(render_report_html(record, show_toolbar=True))


@app.post("/reports/{report_id}/export.pdf")
def export_report(report_id: str) -> JSONResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        report_id = canonical_id
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    html_content = render_report_html(record, show_toolbar=False)
    filename_stem = f"{record['company_id']}-{record['calendar_quarter']}-deep-report"
    try:
        pdf_path = export_html_to_pdf(filename_stem, html_content)
    except Exception as exc:  # pragma: no cover - environment dependent
        raise HTTPException(status_code=500, detail=f"PDF export failed: {exc}") from exc
    update_report_pdf(report_id, pdf_path)
    return JSONResponse(
        {
            "report_id": report_id,
            "pdf_path": pdf_path,
            "download_url": f"/reports/{report_id}/download.pdf",
        }
    )


@app.get("/reports/{report_id}/download.pdf")
def download_report_pdf(report_id: str) -> FileResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        report_id = canonical_id
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    pdf_path = record.get("pdf_path")
    if not pdf_path:
        raise HTTPException(status_code=404, detail="PDF has not been exported yet.")
    path = Path(pdf_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Exported PDF file is missing.")
    return FileResponse(path, media_type="application/pdf", filename=path.name)
