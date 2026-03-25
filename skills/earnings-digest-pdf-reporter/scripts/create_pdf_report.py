#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import site
import sys
from pathlib import Path


def _diagnostic(
    code: str,
    stage: str,
    severity: str,
    message: str,
    *,
    recovery_hint: str | None = None,
    suggestions: list[str] | None = None,
) -> dict[str, object]:
    return {
        "code": code,
        "stage": stage,
        "severity": severity,
        "message": message,
        "recovery_hint": recovery_hint,
        "suggestions": list(suggestions or []),
    }


def _error_payload(message: str, diagnostics: list[dict[str, object]]) -> dict[str, object]:
    return {
        "message": message,
        "diagnostics": diagnostics,
    }


def _looks_like_project_root(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / "pyproject.toml").exists()
        and (path / "app" / "main.py").exists()
        and (path / "app" / "services" / "reports.py").exists()
    )


def _candidate_roots() -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()

    def add(path: Path | None) -> None:
        if path is None:
            return
        resolved = str(path.expanduser().resolve())
        if resolved in seen:
            return
        seen.add(resolved)
        candidates.append(Path(resolved))

    for env_name in ("EARNINGS_DIGEST_STUDIO_ROOT", "EARNINGS_DIGEST_PDF_REPORTER_ROOT"):
        raw = os.environ.get(env_name)
        if raw:
            add(Path(raw))

    cwd = Path.cwd().resolve()
    add(cwd)
    for parent in cwd.parents:
        add(parent)

    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        add(parent)

    home = Path.home()
    search_roots = [
        home / "Documents",
        home / "Projects",
        home / "workspace",
        home / "work",
        home / "code",
    ]
    repo_names = ("earnings-digest-studio", "Earnings-Digest-Studio")
    for base in search_roots:
        if not base.exists():
            continue
        for repo_name in repo_names:
            add(base / repo_name)
            for candidate in base.glob(f"*/{repo_name}"):
                add(candidate)

    return candidates


def _detect_project_root() -> Path | None:
    for candidate in _candidate_roots():
        if _looks_like_project_root(candidate):
            return candidate
    return None


def _bootstrap_project_imports(project_root: Path) -> None:
    root_text = str(project_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
    venv_lib = project_root / ".venv" / "lib"
    if venv_lib.exists():
        for site_packages in venv_lib.glob("python*/site-packages"):
            site.addsitedir(str(site_packages))


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a local earnings digest PDF report.")
    parser.add_argument("--company", required=True, help="Company name, Chinese name, ticker, or internal id.")
    parser.add_argument("--quarter", required=True, help="Quarter like 2025Q4, Q4 2025, or 2025年第4季度.")
    parser.add_argument("--history-window", type=int, default=12, help="History window length, default 12.")
    parser.add_argument("--manual-transcript-upload-id", default=None, help="Optional upload id for a manual transcript.")
    parser.add_argument("--force-refresh", action="store_true", help="Force refresh official materials.")
    parser.add_argument("--project-root", default="", help="Optional explicit path to the earnings-digest-studio repo root.")
    args = parser.parse_args()

    project_root = Path(args.project_root).expanduser().resolve() if args.project_root else _detect_project_root()
    if project_root is None or not _looks_like_project_root(project_root):
        payload = _error_payload(
            "Unable to locate the earnings-digest-studio project root.",
            [
                _diagnostic(
                    "project_root_not_found",
                    "bootstrap",
                    "error",
                    "脚本未找到本地 earnings-digest-studio 仓库，因此无法导入报告生成代码。",
                    recovery_hint="请在仓库工作目录下运行，或设置 EARNINGS_DIGEST_STUDIO_ROOT=/path/to/Earnings-Digest-Studio，或传入 --project-root。",
                )
            ],
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    _bootstrap_project_imports(project_root)

    try:
        from app.db import init_db
        from app.main import _ensure_report_pdf, _resolve_skill_inputs, _skill_error_detail, _skill_payload_diagnostics
        from app.services.reports import create_report, get_report
    except Exception as exc:
        payload = _error_payload(
            str(exc),
            [
                _diagnostic(
                    "project_import_failed",
                    "bootstrap",
                    "error",
                    "已找到仓库，但导入项目依赖失败。",
                    recovery_hint="请确认仓库已安装依赖，通常需要在项目根目录运行 `pip install -e .`。",
                )
            ],
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    init_db()

    try:
        company, calendar_quarter = _resolve_skill_inputs(args.company, args.quarter)
        record = create_report(
            str(company["id"]),
            calendar_quarter,
            int(args.history_window),
            args.manual_transcript_upload_id,
            bool(args.force_refresh),
        )
    except Exception as exc:
        detail = getattr(exc, "detail", None)
        if isinstance(detail, dict) and "diagnostics" in detail:
            print(json.dumps(detail, ensure_ascii=False, indent=2), file=sys.stderr)
            return 1
        payload = _skill_error_detail(
            str(exc),
            [
                _diagnostic(
                    "script_run_failed",
                    "script",
                    "error",
                    str(exc),
                    recovery_hint="请检查输入、项目依赖和本地环境。",
                )
            ],
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    pdf_download_url, pdf_error = _ensure_report_pdf(record)
    refreshed = get_report(str(record["id"]))
    diagnostics = _skill_payload_diagnostics(dict(refreshed.get("payload") or {}), pdf_error)
    payload = {
        "report_id": str(refreshed["id"]),
        "company_id": str(refreshed["company_id"]),
        "company_name": str(company.get("name") or ""),
        "english_name": str(company.get("english_name") or ""),
        "ticker": str(company.get("ticker") or ""),
        "calendar_quarter": str(refreshed["calendar_quarter"]),
        "preview_url": f"/reports/{refreshed['id']}/preview",
        "pdf_path": str(refreshed.get("pdf_path") or "") or None,
        "pdf_download_url": pdf_download_url,
        "pdf_error": pdf_error,
        "diagnostics": diagnostics,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
