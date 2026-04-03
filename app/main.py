from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import APP_TITLE, DEFAULT_HISTORY_WINDOW, EXPORT_DIR, STATIC_DIR, TEMPLATES_DIR
from .db import init_db
from .schemas import (
    ReportCreateRequest,
    ReportJobResponse,
    ReportResponse,
    SkillDiagnostic,
    SkillReportCreateRequest,
    SkillReportJobResponse,
    SkillReportResponse,
    UploadResponse,
)
from .services.pdf_export import export_html_to_pdf, pdf_export_signature
from .services.local_data import normalize_calendar_quarter_input, resolve_company_reference
from .services.reports import (
    company_cards,
    company_quarters,
    create_report,
    create_report_job,
    ensure_report_payload_defaults,
    get_report,
    get_report_job,
    resolve_canonical_report_id,
    update_report_artifacts,
)
from .services.uploads import create_upload
from .utils import slugify


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


def _report_export_stem(record: dict[str, Any]) -> str:
    return slugify(f"{record['company_id']}-{record['calendar_quarter']}-deep-report")


def _report_html_output_path(record: dict[str, Any]) -> Path:
    return EXPORT_DIR / f"{_report_export_stem(record)}.html"


def _report_pdf_metadata_path(record: dict[str, Any], pdf_path: str | None = None) -> Path:
    candidate = str(pdf_path or record.get("pdf_path") or "").strip()
    if candidate:
        resolved = Path(candidate)
        return resolved.with_name(f"{resolved.name}.meta.json")
    return EXPORT_DIR / f"{_report_export_stem(record)}.pdf.meta.json"


def _report_export_source_hash(html_content: str) -> str:
    return hashlib.sha256(html_content.encode("utf-8")).hexdigest()


def _load_report_export_metadata(record: dict[str, Any], pdf_path: str | None = None) -> dict[str, Any] | None:
    metadata_path = _report_pdf_metadata_path(record, pdf_path)
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_report_export_metadata(
    record: dict[str, Any],
    *,
    html_content: str,
    html_path: str,
    pdf_path: str,
) -> None:
    metadata_path = _report_pdf_metadata_path(record, pdf_path)
    payload = {
        "source_hash": _report_export_source_hash(html_content),
        "pdf_profile_signature": pdf_export_signature(),
        "html_path": html_path,
        "pdf_path": pdf_path,
    }
    metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _can_reuse_report_export(record: dict[str, Any], *, html_content: str, html_path: str, pdf_path: str) -> bool:
    html_file = Path(html_path)
    pdf_file = Path(pdf_path)
    if not html_file.exists() or not pdf_file.exists():
        return False
    try:
        if html_file.read_text(encoding="utf-8") != html_content:
            return False
    except OSError:
        return False
    metadata = _load_report_export_metadata(record, pdf_path)
    if not metadata:
        return False
    return (
        metadata.get("source_hash") == _report_export_source_hash(html_content)
        and metadata.get("pdf_profile_signature") == pdf_export_signature()
        and metadata.get("html_path") == html_path
        and metadata.get("pdf_path") == pdf_path
    )


def _write_report_html(record: dict[str, Any], *, show_toolbar: bool = False) -> str:
    html_content = render_report_html(record, show_toolbar=show_toolbar)
    output_path = _report_html_output_path(record)
    output_path.write_text(html_content, encoding="utf-8")
    return str(output_path)


def _skill_diagnostic(
    code: str,
    stage: str,
    severity: str,
    message: str,
    *,
    recovery_hint: str | None = None,
    suggestions: list[str] | None = None,
) -> dict[str, Any]:
    return SkillDiagnostic(
        code=code,
        stage=stage,
        severity=severity,
        message=message,
        recovery_hint=recovery_hint,
        suggestions=list(suggestions or []),
    ).model_dump()


def _skill_error_detail(message: str, diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "message": message,
        "diagnostics": diagnostics,
    }


def _resolve_skill_inputs(company_value: str, quarter_value: str) -> tuple[dict[str, Any], str]:
    try:
        company = resolve_company_reference(company_value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_skill_error_detail(
                str(exc),
                [
                    _skill_diagnostic(
                        "company_reference_empty",
                        "resolve_input",
                        "error",
                        "公司输入为空，系统无法开始生成报告。",
                        recovery_hint="请提供公司英文名、中文名、股票代码或内部 company_id。",
                    )
                ],
            ),
        ) from exc
    except KeyError as exc:
        raise HTTPException(
            status_code=404,
            detail=_skill_error_detail(
                str(exc),
                [
                    _skill_diagnostic(
                        "company_not_resolved",
                        "resolve_company",
                        "error",
                        f"当前未能解析公司输入“{company_value}”。",
                        recovery_hint="请改用更完整的公司英文名或股票代码（ticker）重试。",
                    )
                ],
            ),
        ) from exc

    try:
        calendar_quarter = normalize_calendar_quarter_input(quarter_value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_skill_error_detail(
                str(exc),
                [
                    _skill_diagnostic(
                        "quarter_not_normalized",
                        "resolve_quarter",
                        "error",
                        f"无法识别季度输入“{quarter_value}”。",
                        recovery_hint="请使用 2025Q4、Q4 2025 或 2025年第4季度 这类格式。",
                        suggestions=["2025Q4", "Q4 2025", "2025年第4季度"],
                    )
                ],
            ),
        ) from exc
    return (company, calendar_quarter)


def _report_generation_http_exception(company: dict[str, Any], exc: Exception, *, history_window: int) -> HTTPException:
    message = str(exc)
    company_id = str(company.get("id") or "")
    quarter_suggestions: list[str] = []
    try:
        quarter_suggestions = list(company_quarters(company_id, history_window).get("supported_quarters") or [])[-6:]
    except Exception:
        quarter_suggestions = []
    if "Quarter" in message or "history window" in message:
        return HTTPException(
            status_code=400,
            detail=_skill_error_detail(
                message,
                [
                    _skill_diagnostic(
                        "quarter_unavailable",
                        "build_report",
                        "error",
                        "目标季度当前不在可生成窗口内，或缺少完整历史窗口。",
                        recovery_hint="请改用项目当前支持的季度，或降低历史窗口要求。",
                        suggestions=quarter_suggestions,
                    )
                ],
            ),
        )
    return HTTPException(
        status_code=400,
        detail=_skill_error_detail(
            message,
            [
                _skill_diagnostic(
                    "report_generation_failed",
                    "build_report",
                    "error",
                    "报告生成过程中出现异常。",
                    recovery_hint="可重试一次；若仍失败，请换一个已覆盖季度或开启 force_refresh。",
                )
            ],
        ),
    )


def _unexpected_skill_http_exception(exc: Exception, *, stage: str) -> HTTPException:
    return HTTPException(
        status_code=500,
        detail=_skill_error_detail(
            str(exc),
            [
                _skill_diagnostic(
                    "unexpected_runtime_error",
                    stage,
                    "error",
                    "运行过程中出现未预期异常。",
                    recovery_hint="请稍后重试；若持续失败，请检查本地缓存目录、依赖环境和官方源抓取能力。",
                )
            ],
        ),
    )


def _skill_payload_diagnostics(report_payload: dict[str, Any], pdf_error: str | None = None) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for warning in list(report_payload.get("coverage_warnings") or [])[:4]:
        diagnostics.append(
            _skill_diagnostic(
                "coverage_warning",
                "coverage",
                "warning",
                str(warning),
                recovery_hint="如需更高完整度，可尝试 force_refresh 或改用覆盖度更高的季度。",
            )
        )
    quality_report = report_payload.get("quality_report") or {}
    status = str(quality_report.get("status") or "pass")
    if status != "pass":
        for issue in list(quality_report.get("issues") or [])[:3]:
            diagnostics.append(
                _skill_diagnostic(
                    str(issue.get("code") or "quality_issue"),
                    "quality",
                    "warning" if str(issue.get("severity") or "") != "critical" else "error",
                    str(issue.get("message") or "报告质量检查发现问题。"),
                    recovery_hint="可根据该诊断重新生成，或检查对应季度官方材料是否完整。",
                )
            )
    if pdf_error:
        diagnostics.append(
            _skill_diagnostic(
                "pdf_export_failed",
                "pdf_export",
                "warning",
                pdf_error,
                recovery_hint="HTML 预览通常仍可使用，稍后可重新触发 PDF 导出。",
            )
        )
    return diagnostics


def _ensure_report_files(record: dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    html_download_url = f"/reports/{record['id']}/download.html"
    pdf_download_url = f"/reports/{record['id']}/download.pdf"
    html_path = str(record.get("html_path") or "").strip()
    existing_pdf_path = str(record.get("pdf_path") or "").strip()
    html_exists = bool(html_path) and Path(html_path).exists()
    pdf_exists = bool(existing_pdf_path) and Path(existing_pdf_path).exists()
    html_content = render_report_html(record, show_toolbar=False)
    if html_exists and pdf_exists and _can_reuse_report_export(
        record,
        html_content=html_content,
        html_path=html_path,
        pdf_path=existing_pdf_path,
    ):
        return (html_download_url, pdf_download_url, None, None)
    generated_html_path: Optional[str] = None
    try:
        html_output_path = _report_html_output_path(record)
        html_output_path.write_text(html_content, encoding="utf-8")
        generated_html_path = str(html_output_path)
        filename_stem = _report_export_stem(record)
        pdf_path = export_html_to_pdf(filename_stem, html_content)
        update_report_artifacts(record["id"], html_path=generated_html_path, pdf_path=pdf_path)
        _write_report_export_metadata(
            record,
            html_content=html_content,
            html_path=generated_html_path,
            pdf_path=pdf_path,
        )
        return (html_download_url, pdf_download_url, generated_html_path, None)
    except Exception as exc:  # pragma: no cover - environment dependent
        if generated_html_path and Path(generated_html_path).exists():
            update_report_artifacts(record["id"], html_path=generated_html_path)
            return (html_download_url, None, generated_html_path, f"PDF export failed: {exc}")
        if html_exists:
            return (html_download_url, None, html_path, f"PDF export failed: {exc}")
        try:
            html_path = _write_report_html(record, show_toolbar=False)
            update_report_artifacts(record["id"], html_path=html_path)
            return (html_download_url, None, html_path, f"PDF export failed: {exc}")
        except Exception as html_exc:  # pragma: no cover - environment dependent
            return (None, None, None, f"HTML/PDF export failed: {html_exc}; PDF export failed: {exc}")


def _skill_job_response_payload(job: dict[str, Any], *, ensure_pdf: bool) -> SkillReportJobResponse:
    company = resolve_company_reference(str(job.get("company_id") or ""))
    html_download_url = None
    pdf_download_url = None
    pdf_error = None
    diagnostics: list[dict[str, Any]] = []
    report_id = job.get("report_id")
    if ensure_pdf and report_id and str(job.get("status") or "") == "completed":
        try:
            record = get_report(str(report_id))
        except KeyError:
            pdf_error = "Report record is missing."
            diagnostics.append(
                _skill_diagnostic(
                    "report_missing",
                    "job_finalize",
                    "error",
                    "后台任务已完成，但找不到对应的报告记录。",
                    recovery_hint="请重新提交任务。",
                )
            )
        else:
            html_download_url, pdf_download_url, _html_path, pdf_error = _ensure_report_files(record)
            diagnostics.extend(_skill_payload_diagnostics(dict(record.get("payload") or {}), pdf_error))
    if str(job.get("status") or "") == "failed":
        diagnostics.append(
            _skill_diagnostic(
                "report_job_failed",
                "job_run",
                "error",
                str(job.get("error") or "后台任务失败。"),
                recovery_hint="请重试一次；若仍失败，请尝试 force_refresh 或更换季度。",
            )
        )
    return SkillReportJobResponse(
        job_id=str(job.get("job_id") or ""),
        company_id=str(job.get("company_id") or ""),
        company_name=str(company.get("name") or ""),
        english_name=str(company.get("english_name") or ""),
        ticker=str(company.get("ticker") or ""),
        calendar_quarter=str(job.get("calendar_quarter") or ""),
        history_window=int(job.get("history_window") or 12),
        status=str(job.get("status") or ""),
        progress=float(job.get("progress") or 0.0),
        stage=str(job.get("stage") or ""),
        message=str(job.get("message") or ""),
        report_id=str(report_id) if report_id else None,
        error=job.get("error"),
        preview_url=job.get("preview_url"),
        html_download_url=html_download_url,
        export_pdf_url=job.get("export_pdf_url"),
        pdf_download_url=pdf_download_url,
        pdf_error=pdf_error,
        diagnostics=diagnostics,
    )


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
        html_download_url=f"/reports/{report_id}/download.html",
        export_pdf_url=f"/reports/{report_id}/export.pdf",
        payload=report_payload,
    )


@app.post("/skill/reports", response_model=SkillReportResponse)
def create_skill_report_endpoint(payload: SkillReportCreateRequest) -> SkillReportResponse:
    company, calendar_quarter = _resolve_skill_inputs(payload.company, payload.quarter)
    try:
        record = create_report(
            str(company["id"]),
            calendar_quarter,
            payload.history_window,
            payload.manual_transcript_upload_id,
            payload.force_refresh,
        )
    except (KeyError, ValueError) as exc:
        raise _report_generation_http_exception(company, exc, history_window=payload.history_window) from exc
    except Exception as exc:
        raise _unexpected_skill_http_exception(exc, stage="build_report") from exc

    report_payload = record["payload"]
    report_id = record["id"]
    html_download_url = None
    html_path = None
    pdf_download_url = None
    pdf_error = None
    try:
        html_download_url, pdf_download_url, html_path, pdf_error = _ensure_report_files(record)
    except Exception as exc:  # pragma: no cover - environment dependent
        pdf_error = str(exc)
    refreshed_record = get_report(report_id)
    diagnostics = _skill_payload_diagnostics(report_payload, pdf_error)

    return SkillReportResponse(
        report_id=report_id,
        company_id=record["company_id"],
        company_name=str(company.get("name") or ""),
        english_name=str(company.get("english_name") or ""),
        ticker=str(company.get("ticker") or ""),
        calendar_quarter=record["calendar_quarter"],
        structure_dimension_used=record["structure_dimension_used"],
        coverage_warnings=report_payload["coverage_warnings"],
        preview_url=f"/reports/{report_id}/preview",
        html_download_url=html_download_url or f"/reports/{report_id}/download.html",
        export_pdf_url=f"/reports/{report_id}/export.pdf",
        html_path=html_path or str(refreshed_record.get("html_path") or "") or None,
        pdf_download_url=pdf_download_url,
        pdf_path=str(refreshed_record.get("pdf_path") or "") or None,
        pdf_error=pdf_error,
        diagnostics=diagnostics,
        payload=report_payload,
    )


@app.post("/skill/report-jobs", response_model=SkillReportJobResponse)
def create_skill_report_job_endpoint(payload: SkillReportCreateRequest) -> SkillReportJobResponse:
    company, calendar_quarter = _resolve_skill_inputs(payload.company, payload.quarter)
    try:
        job = create_report_job(
            str(company["id"]),
            calendar_quarter,
            payload.history_window,
            payload.manual_transcript_upload_id,
            payload.force_refresh,
        )
    except (KeyError, ValueError) as exc:
        raise _report_generation_http_exception(company, exc, history_window=payload.history_window) from exc
    except Exception as exc:
        raise _unexpected_skill_http_exception(exc, stage="queue_job") from exc
    return _skill_job_response_payload(job, ensure_pdf=True)


@app.get("/skill/report-jobs/{job_id}", response_model=SkillReportJobResponse)
def skill_report_job_endpoint(job_id: str) -> SkillReportJobResponse:
    try:
        job = get_report_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _skill_job_response_payload(job, ensure_pdf=True)


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
            "html_download_url": f"/reports/{record['id']}/download.html",
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
    try:
        html_download_url, pdf_download_url, html_path, pdf_error = _ensure_report_files(record)
    except Exception as exc:  # pragma: no cover - environment dependent
        raise HTTPException(status_code=500, detail=f"PDF export failed: {exc}") from exc
    refreshed = get_report(report_id)
    if pdf_error or not refreshed.get("pdf_path"):
        raise HTTPException(status_code=500, detail=pdf_error or "PDF export failed: file was not materialized.")
    return JSONResponse(
        {
            "report_id": report_id,
            "html_path": html_path or str(refreshed.get("html_path") or "") or None,
            "html_download_url": html_download_url,
            "pdf_path": str(refreshed.get("pdf_path") or ""),
            "download_url": pdf_download_url,
        }
    )


@app.post("/reports/{report_id}/export.html")
def export_report_html(report_id: str) -> JSONResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        report_id = canonical_id
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    html_path = _write_report_html(record, show_toolbar=False)
    update_report_artifacts(report_id, html_path=html_path)
    return JSONResponse(
        {
            "report_id": report_id,
            "html_path": html_path,
            "download_url": f"/reports/{report_id}/download.html",
        }
    )


@app.get("/reports/{report_id}/download.html")
def download_report_html(report_id: str) -> FileResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        report_id = canonical_id
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    html_path = str(record.get("html_path") or "").strip()
    path = Path(html_path) if html_path else None
    if path is None or not path.exists():
        html_path = _write_report_html(record, show_toolbar=False)
        update_report_artifacts(report_id, html_path=html_path)
        path = Path(html_path)
    return FileResponse(path, media_type="text/html; charset=utf-8", filename=path.name)


@app.get("/reports/{report_id}/download.pdf")
def download_report_pdf(report_id: str) -> FileResponse:
    canonical_id = resolve_canonical_report_id(report_id)
    if canonical_id and canonical_id != report_id:
        report_id = canonical_id
    try:
        record = get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    pdf_path = str(record.get("pdf_path") or "").strip()
    path = Path(pdf_path) if pdf_path else None
    if path is None or not path.exists():
        _, _, _, pdf_error = _ensure_report_files(record)
        refreshed = get_report(report_id)
        pdf_path = str(refreshed.get("pdf_path") or "").strip()
        path = Path(pdf_path) if pdf_path else None
        if pdf_error or path is None:
            raise HTTPException(status_code=404, detail=pdf_error or "PDF has not been exported yet.")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Exported PDF file is missing.")
    return FileResponse(path, media_type="application/pdf", filename=path.name)
