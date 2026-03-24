from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.local_data import get_company_periods, get_supported_quarters, list_companies  # noqa: E402
from app.services.report_quality import evaluate_report_payload  # noqa: E402
from app.services.reports import build_report_payload  # noqa: E402


def _sample_quarters(company_id: str, limit: int, history_window: int) -> list[str]:
    periods = get_supported_quarters(company_id, history_window=history_window, fetch_missing=True)
    if limit <= 1:
        return periods[-1:] if periods else []
    if len(periods) <= limit:
        return periods
    midpoint = len(periods) // 2
    anchors = [periods[limit - 1], periods[midpoint], periods[-1]]
    ordered: list[str] = []
    for item in anchors:
        if item not in ordered:
            ordered.append(item)
    return ordered[:limit]


def _estimated_audit_window_count(company_id: str, history_window: int) -> int:
    periods = get_company_periods(company_id, fetch_missing=False)
    return max(0, len(periods) - max(1, int(history_window)) + 1)


def _company_audit_is_complete(info: dict[str, Any], *, all_quarters: bool) -> bool:
    if not isinstance(info, dict):
        return False
    audited_quarter_count = int(info.get("audited_quarter_count") or 0)
    if not all_quarters:
        return audited_quarter_count > 0 or bool(info.get("quarters"))
    all_window_quarter_count = int(info.get("all_window_quarter_count") or 0)
    return audited_quarter_count >= all_window_quarter_count


def _names(items: list[dict[str, Any]] | None) -> list[str]:
    return [
        str(item.get("name") or item.get("label") or "").strip()
        for item in list(items or [])
        if str(item.get("name") or item.get("label") or "").strip()
    ]


def _select_quarters(
    company_id: str,
    *,
    all_quarters: bool,
    limit: int,
    history_window: int,
    include_unready: bool,
) -> list[str]:
    if all_quarters:
        if include_unready:
            periods = get_company_periods(company_id, fetch_missing=True)
            if len(periods) < history_window:
                return []
            return periods[history_window - 1 :]
        return get_supported_quarters(company_id, history_window=history_window, fetch_missing=True)
    return _sample_quarters(company_id, limit, history_window)


def _legacy_audit_fields(payload: dict[str, Any]) -> dict[str, Any]:
    historical_cube = list(payload.get("historical_cube") or [])
    return {
        "structure_dimension": payload.get("structure_dimension_used"),
        "current_segment_count": len(payload.get("current_segments") or []),
        "current_geography_count": len(payload.get("current_geographies") or []),
        "current_segment_names": _names(payload.get("current_segments")),
        "current_geography_names": _names(payload.get("current_geographies")),
        "historical_missing_segments": [
            item.get("quarter_label")
            for item in historical_cube
            if not item.get("segments")
        ],
        "historical_missing_geographies": [
            item.get("quarter_label")
            for item in historical_cube
            if not item.get("geographies")
        ],
        "management_theme_count": len(payload.get("management_themes") or []),
        "qna_theme_count": len(payload.get("qna_themes") or []),
        "risk_count": len(payload.get("risks") or []),
        "catalyst_count": len(payload.get("catalysts") or []),
        "institutional_view_count": len(payload.get("institutional_views") or []),
        "warnings": list(payload.get("coverage_warnings") or []),
    }


def _quality_result(payload: dict[str, Any], history_window: int, require_full_coverage: bool) -> dict[str, Any]:
    report = evaluate_report_payload(
        payload,
        history_window=history_window,
        require_full_coverage=require_full_coverage,
    )
    return {
        "status": report.get("status"),
        "score": report.get("score"),
        "counts": dict(report.get("counts") or {}),
        "metrics": dict(report.get("metrics") or {}),
        "issues": [dict(item) for item in list(report.get("issues") or [])],
        "summary": report.get("summary"),
    }


def _quarter_rank(result: dict[str, Any]) -> tuple[int, int]:
    if not result.get("ok"):
        return (3, 0)
    status = str(result.get("quality", {}).get("status") or "pass")
    score = int(result.get("quality", {}).get("score") or 0)
    severity_rank = {"pass": 0, "review": 1, "fail": 2}
    return (severity_rank.get(status, 2), 100 - score)


def _count_by_status(results: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"pass": 0, "review": 0, "fail": 0, "error": 0}
    for item in results:
        if not item.get("ok"):
            summary["error"] += 1
            continue
        status = str(item.get("quality", {}).get("status") or "review")
        if status not in summary:
            summary[status] = 0
        summary[status] += 1
    return summary


def _company_issue_summary(quarters: dict[str, Any]) -> dict[str, Any]:
    code_counter: dict[str, int] = {}
    severity_counter = {"critical": 0, "major": 0, "minor": 0}
    for result in quarters.values():
        if not result.get("ok"):
            continue
        for issue in list(result.get("quality", {}).get("issues") or []):
            code = str(issue.get("code") or "unknown")
            severity = str(issue.get("severity") or "minor")
            code_counter[code] = code_counter.get(code, 0) + 1
            if severity not in severity_counter:
                severity_counter[severity] = 0
            severity_counter[severity] += 1
    top_codes = [
        {"code": code, "count": count}
        for code, count in sorted(code_counter.items(), key=lambda item: item[1], reverse=True)[:6]
    ]
    return {
        "issue_counts": severity_counter,
        "top_issue_codes": top_codes,
    }


def _company_info_snapshot(
    *,
    cached_quarters: list[str],
    dynamic_quarters: list[str],
    all_window_quarters: list[str],
    target_quarters: list[str],
    quarter_results: dict[str, Any],
) -> dict[str, Any]:
    ordered_quarter_results = [dict(quarter_results[quarter]) for quarter in target_quarters if quarter in quarter_results]
    company_status_counts = _count_by_status(ordered_quarter_results)
    worst_quarters = sorted(
        [
            {"calendar_quarter": quarter, **result}
            for quarter, result in quarter_results.items()
        ],
        key=_quarter_rank,
        reverse=True,
    )[:5]
    return {
        "cached_supported_quarter_count": len(cached_quarters),
        "dynamic_supported_quarter_count": len(dynamic_quarters),
        "cached_supported_quarter_first": cached_quarters[0] if cached_quarters else None,
        "cached_supported_quarter_last": cached_quarters[-1] if cached_quarters else None,
        "dynamic_supported_quarter_first": dynamic_quarters[0] if dynamic_quarters else None,
        "dynamic_supported_quarter_last": dynamic_quarters[-1] if dynamic_quarters else None,
        "all_window_quarter_count": len(all_window_quarters),
        "excluded_unready_quarter_count": max(0, len(all_window_quarters) - len(target_quarters)),
        "audited_quarter_count": len(quarter_results),
        "status_counts": company_status_counts,
        **_company_issue_summary(quarter_results),
        "worst_quarters": worst_quarters,
        "quarters": dict(quarter_results),
    }


def _render_markdown_summary(rendered_payload: dict[str, Any]) -> str:
    summary = dict(rendered_payload.get("summary") or {})
    status_counts = dict(summary.get("status_counts") or {})
    lines: list[str] = []
    lines.append("# Earnings Quality Audit")
    lines.append("")
    lines.append(f"- Companies audited: {int(summary.get('companies_audited') or 0)}")
    lines.append(f"- Quarters audited: {int(summary.get('quarters_audited') or 0)}")
    lines.append(f"- Full coverage required: {bool(summary.get('require_full_coverage'))}")
    lines.append(f"- Excluded unready quarters: {int(summary.get('excluded_unready_quarter_count') or 0)}")
    lines.append(
        "- Pass/Review/Fail/Error: "
        f"{int(status_counts.get('pass') or 0)}/"
        f"{int(status_counts.get('review') or 0)}/"
        f"{int(status_counts.get('fail') or 0)}/"
        f"{int(status_counts.get('error') or 0)}"
    )
    lines.append(f"- All passed: {bool(summary.get('all_passed'))}")
    lines.append("")
    lines.append("## Focus Companies")
    focus_companies = list(summary.get("focus_companies") or [])
    if not focus_companies:
        lines.append("- None.")
    else:
        for index, item in enumerate(focus_companies, start=1):
            company_id = str(item.get("company_id") or "")
            pressure = int(item.get("pressure") or 0)
            company_status = dict(item.get("status_counts") or {})
            issue_counts = dict(item.get("issue_counts") or {})
            top_codes = list(item.get("top_issue_codes") or [])
            top_code_text = ", ".join(
                f"{str(code.get('code') or '')}({int(code.get('count') or 0)})"
                for code in top_codes[:4]
            )
            lines.append(
                f"{index}. `{company_id}` | pressure={pressure} | "
                f"pass={int(company_status.get('pass') or 0)} "
                f"review={int(company_status.get('review') or 0)} "
                f"fail={int(company_status.get('fail') or 0)} | "
                f"issues(c/m/n)={int(issue_counts.get('critical') or 0)}/"
                f"{int(issue_counts.get('major') or 0)}/"
                f"{int(issue_counts.get('minor') or 0)}"
                + (f" | top: {top_code_text}" if top_code_text else "")
            )
    return "\n".join(lines) + "\n"


def _flatten_company_results(results: dict[str, Any], extra_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for company_id, info in results.items():
        for quarter, result in dict(info.get("quarters") or {}).items():
            key = (str(company_id), str(quarter))
            if key in seen or not isinstance(result, dict):
                continue
            seen.add(key)
            flattened.append(
                {
                    "company_id": str(company_id),
                    "calendar_quarter": str(quarter),
                    **result,
                }
            )
    for item in extra_items:
        company_id = str(item.get("company_id") or "")
        quarter = str(item.get("calendar_quarter") or "")
        key = (company_id, quarter)
        if not company_id or not quarter or key in seen:
            continue
        seen.add(key)
        flattened.append(dict(item))
    return flattened


def _rendered_payload(
    companies: list[str],
    results: dict[str, Any],
    flat_quarter_results: list[dict[str, Any]],
    *,
    history_window: int,
    require_full_coverage: bool,
    include_unready_audited: bool,
) -> dict[str, Any]:
    ordered_results = {company_id: results.get(company_id, {}) for company_id in companies}
    flattened_quarter_results = _flatten_company_results(ordered_results, flat_quarter_results)
    global_status_counts = _count_by_status(flattened_quarter_results)
    failing_or_error_quarters = [
        item
        for item in flattened_quarter_results
        if not item.get("ok") or str(item.get("quality", {}).get("status") or "") == "fail"
    ]
    review_quarters = [
        item
        for item in flattened_quarter_results
        if item.get("ok") and str(item.get("quality", {}).get("status") or "") == "review"
    ]
    summary = {
        "companies_audited": len(
            [
                company_id
                for company_id, info in ordered_results.items()
                if isinstance(info, dict) and (
                    int(info.get("audited_quarter_count") or 0) > 0
                    or bool(info.get("quarters"))
                )
            ]
        ),
        "companies_requested": len(companies),
        "history_window": int(history_window),
        "quarters_audited": len(flattened_quarter_results),
        "require_full_coverage": require_full_coverage,
        "include_unready_audited": include_unready_audited,
        "status_counts": global_status_counts,
        "excluded_unready_quarter_count": sum(
            int(info.get("excluded_unready_quarter_count") or 0)
            for info in ordered_results.values()
        ),
        "fail_or_error_quarter_count": len(failing_or_error_quarters),
        "review_quarter_count": len(review_quarters),
        "all_passed": not failing_or_error_quarters and not review_quarters,
    }
    focus_companies = []
    for company_id, info in ordered_results.items():
        status_counts = dict(info.get("status_counts") or {})
        issue_counts = dict(info.get("issue_counts") or {})
        pressure = (
            int(status_counts.get("fail") or 0) * 1000
            + int(status_counts.get("error") or 0) * 1000
            + int(status_counts.get("review") or 0) * 10
            + int(issue_counts.get("critical") or 0) * 6
            + int(issue_counts.get("major") or 0) * 2
        )
        if pressure <= 0:
            continue
        focus_companies.append(
            {
                "company_id": company_id,
                "pressure": pressure,
                "status_counts": status_counts,
                "issue_counts": issue_counts,
                "top_issue_codes": list(info.get("top_issue_codes") or []),
            }
        )
    focus_companies.sort(key=lambda item: item["pressure"], reverse=True)
    summary["focus_companies"] = focus_companies[:10]
    return {
        "summary": summary,
        "companies": ordered_results,
    }


def _write_ready_map(
    output_path: Path,
    rendered_payload: dict[str, Any],
    *,
    history_window: int,
    require_full_coverage: bool,
    include_unready_audited: bool,
) -> None:
    results = dict(rendered_payload.get("companies") or {})
    ready_map = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "history_window": int(history_window),
        "require_full_coverage": bool(require_full_coverage),
        "include_unready_audited": bool(include_unready_audited),
        "companies": {
            company_id: {
                "ready_quarters": [
                    quarter
                    for quarter, result in dict(info.get("quarters") or {}).items()
                    if result.get("ok") and str(result.get("quality", {}).get("status") or "") == "pass"
                ],
                "audited_quarter_count": int(info.get("audited_quarter_count") or 0),
                "all_window_quarter_count": int(info.get("all_window_quarter_count") or 0),
                "all_quarters_audited": int(info.get("audited_quarter_count") or 0) >= int(info.get("all_window_quarter_count") or 0),
            }
            for company_id, info in results.items()
        },
    }
    for info in ready_map["companies"].values():
        info["ready_quarters"] = sorted(info.get("ready_quarters") or [])
    output_path.write_text(json.dumps(ready_map, ensure_ascii=False, indent=2), encoding="utf-8")


def _persist_outputs(
    *,
    companies: list[str],
    results: dict[str, Any],
    flat_quarter_results: list[dict[str, Any]],
    history_window: int,
    require_full_coverage: bool,
    include_unready_audited: bool,
    output_path: Optional[Path],
    summary_markdown_path: Optional[Path],
    ready_map_path: Optional[Path],
) -> dict[str, Any]:
    rendered_payload = _rendered_payload(
        companies,
        results,
        flat_quarter_results,
        history_window=history_window,
        require_full_coverage=require_full_coverage,
        include_unready_audited=include_unready_audited,
    )
    rendered = json.dumps(rendered_payload, ensure_ascii=False, indent=2)
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    if summary_markdown_path:
        summary_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        summary_markdown_path.write_text(_render_markdown_summary(rendered_payload), encoding="utf-8")
    if ready_map_path:
        ready_map_path.parent.mkdir(parents=True, exist_ok=True)
        _write_ready_map(
            ready_map_path,
            rendered_payload,
            history_window=history_window,
            require_full_coverage=require_full_coverage,
            include_unready_audited=include_unready_audited,
        )
    return rendered_payload


def _load_existing_output(
    output_path: Path,
    *,
    history_window: int,
    require_full_coverage: bool,
    include_unready_audited: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, []
    summary = dict(payload.get("summary") or {})
    if int(summary.get("history_window") or 0) != int(history_window):
        return {}, []
    if bool(summary.get("require_full_coverage")) != bool(require_full_coverage):
        return {}, []
    if bool(summary.get("include_unready_audited")) != bool(include_unready_audited):
        return {}, []
    companies = payload.get("companies")
    if not isinstance(companies, dict):
        return {}, []
    normalized_results: dict[str, Any] = {}
    flat_quarter_results: list[dict[str, Any]] = []
    for company_id, info in companies.items():
        if not isinstance(info, dict):
            continue
        normalized_info = dict(info)
        normalized_results[str(company_id)] = normalized_info
        for quarter, result in dict(normalized_info.get("quarters") or {}).items():
            if not isinstance(result, dict):
                continue
            flat_quarter_results.append(
                {
                    "company_id": str(company_id),
                    "calendar_quarter": str(quarter),
                    **result,
                }
            )
    return normalized_results, flat_quarter_results


def _audit_company(
    company_id: str,
    *,
    history_window: int,
    all_quarters: bool,
    quarters_per_company: int,
    include_unready: bool,
    refresh_source_materials: bool,
    require_full_coverage: bool,
    existing_info: Optional[dict[str, Any]] = None,
    quarter_complete_callback: Optional[Callable[[str, dict[str, Any]], None]] = None,
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    cached_quarters = get_supported_quarters(company_id, history_window=history_window, fetch_missing=False)
    dynamic_quarters = get_supported_quarters(company_id, history_window=history_window, fetch_missing=True)
    target_quarters = _select_quarters(
        company_id,
        all_quarters=all_quarters,
        limit=quarters_per_company,
        history_window=history_window,
        include_unready=include_unready,
    )
    all_window_quarters: list[str] = []
    all_periods = get_company_periods(company_id, fetch_missing=True)
    if len(all_periods) >= history_window:
        all_window_quarters = all_periods[history_window - 1 :]

    existing_quarter_results = {
        str(quarter): dict(result)
        for quarter, result in dict((existing_info or {}).get("quarters") or {}).items()
        if isinstance(result, dict)
    }
    quarter_results: dict[str, Any] = {
        quarter: result
        for quarter, result in existing_quarter_results.items()
        if quarter in target_quarters
    }
    pending_quarters = [quarter for quarter in reversed(target_quarters) if quarter not in quarter_results]
    for quarter in pending_quarters:
        try:
            payload = build_report_payload(
                company_id,
                quarter,
                history_window=history_window,
                refresh_source_materials=refresh_source_materials,
                require_full_coverage=require_full_coverage,
            )
            quality = _quality_result(payload, history_window, require_full_coverage)
            result = {
                "ok": True,
                "quality": quality,
                **_legacy_audit_fields(payload),
            }
        except Exception as exc:
            result = {
                "ok": False,
                "error": str(exc),
            }
        quarter_results[quarter] = result
        if quarter_complete_callback is not None:
            quarter_complete_callback(
                company_id,
                _company_info_snapshot(
                    cached_quarters=cached_quarters,
                    dynamic_quarters=dynamic_quarters,
                    all_window_quarters=all_window_quarters,
                    target_quarters=target_quarters,
                    quarter_results=quarter_results,
                ),
            )

    ordered_quarter_results = [dict(quarter_results[quarter]) for quarter in target_quarters if quarter in quarter_results]
    flat_quarter_results = [
        {
            "company_id": company_id,
            "calendar_quarter": quarter,
            **quarter_results[quarter],
        }
        for quarter in target_quarters
        if quarter in quarter_results
    ]
    info = _company_info_snapshot(
        cached_quarters=cached_quarters,
        dynamic_quarters=dynamic_quarters,
        all_window_quarters=all_window_quarters,
        target_quarters=target_quarters,
        quarter_results=quarter_results,
    )
    return company_id, info, flat_quarter_results


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit on-demand report generation quality across companies and quarters.")
    parser.add_argument("--company", action="append", dest="companies", help="Company id to audit. Repeatable.")
    parser.add_argument("--history-window", type=int, default=12, help="History window size.")
    parser.add_argument("--quarters-per-company", type=int, default=3, help="Sample size when not using --all-quarters.")
    parser.add_argument("--all-quarters", action="store_true", help="Audit every available quarter for each company.")
    parser.add_argument(
        "--include-unready",
        action="store_true",
        help="When used with --all-quarters, include quarters that are currently excluded from product selectors.",
    )
    parser.add_argument("--refresh-source-materials", action="store_true", help="Refresh official source materials before auditing.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Number of companies to audit in parallel.",
    )
    parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Allow fallback-style reports during audit (disables full-coverage hard rules).",
    )
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when audit contains fail/error quarters.")
    parser.add_argument("--output", help="Optional path to write JSON results.")
    parser.add_argument("--summary-markdown", help="Optional path to write markdown summary.")
    parser.add_argument("--write-ready-map", help="Optional path to write per-company ready quarter map.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from an existing --output JSON file and skip companies that already have quarter results.",
    )
    args = parser.parse_args()

    require_full_coverage = not bool(args.allow_fallback)
    if args.write_ready_map and not args.all_quarters:
        raise SystemExit("Writing a ready-map requires --all-quarters so the audit can cover the full history window.")
    effective_include_unready = bool(args.include_unready or (args.write_ready_map and args.all_quarters))
    output_path = Path(args.output) if args.output else None
    summary_markdown_path = Path(args.summary_markdown) if args.summary_markdown else None
    ready_map_path = Path(args.write_ready_map) if args.write_ready_map else None
    if require_full_coverage and os.environ.get("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH") == "1":
        raise SystemExit(
            "Full coverage audit requires official source fetch. "
            "Unset EARNINGS_DIGEST_DISABLE_SOURCE_FETCH or use --allow-fallback."
        )
    companies = args.companies or [str(item["id"]) for item in list_companies()]
    max_workers = max(1, int(args.max_workers or 1))
    results: dict[str, Any] = {}
    flat_quarter_results: list[dict[str, Any]] = []
    if args.resume:
        if output_path is None:
            raise SystemExit("--resume requires --output so completed-company checkpoints can be loaded.")
        loaded_results, loaded_flat_results = _load_existing_output(
            output_path,
            history_window=args.history_window,
            require_full_coverage=require_full_coverage,
            include_unready_audited=effective_include_unready,
        )
        results.update(loaded_results)
        flat_quarter_results.extend(loaded_flat_results)
    completed_companies = {
        company_id
        for company_id, info in results.items()
        if _company_audit_is_complete(info, all_quarters=bool(args.all_quarters))
    }
    pending_companies = [company_id for company_id in companies if company_id not in completed_companies]
    pending_companies.sort(key=lambda company_id: (_estimated_audit_window_count(company_id, args.history_window), company_id))
    persist_lock = threading.Lock()

    def _quarter_complete(company_id: str, partial_info: dict[str, Any]) -> None:
        with persist_lock:
            results[company_id] = partial_info
            _persist_outputs(
                companies=companies,
                results=results,
                flat_quarter_results=flat_quarter_results,
                history_window=args.history_window,
                require_full_coverage=require_full_coverage,
                include_unready_audited=effective_include_unready,
                output_path=output_path,
                summary_markdown_path=summary_markdown_path,
                ready_map_path=ready_map_path,
            )

    if max_workers == 1 or len(pending_companies) <= 1:
        for company_id in pending_companies:
            audited_company_id, info, flat_items = _audit_company(
                company_id,
                history_window=args.history_window,
                all_quarters=args.all_quarters,
                quarters_per_company=args.quarters_per_company,
                include_unready=effective_include_unready,
                refresh_source_materials=args.refresh_source_materials,
                require_full_coverage=require_full_coverage,
                existing_info=results.get(company_id),
                quarter_complete_callback=_quarter_complete,
            )
            results[audited_company_id] = info
            flat_quarter_results = [
                item
                for item in flat_quarter_results
                if str(item.get("company_id") or "") != audited_company_id
            ]
            flat_quarter_results.extend(flat_items)
            with persist_lock:
                _persist_outputs(
                    companies=companies,
                    results=results,
                    flat_quarter_results=flat_quarter_results,
                    history_window=args.history_window,
                    require_full_coverage=require_full_coverage,
                    include_unready_audited=effective_include_unready,
                    output_path=output_path,
                    summary_markdown_path=summary_markdown_path,
                    ready_map_path=ready_map_path,
                )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _audit_company,
                    company_id,
                    history_window=args.history_window,
                    all_quarters=args.all_quarters,
                    quarters_per_company=args.quarters_per_company,
                    include_unready=effective_include_unready,
                    refresh_source_materials=args.refresh_source_materials,
                    require_full_coverage=require_full_coverage,
                    existing_info=results.get(company_id),
                    quarter_complete_callback=_quarter_complete,
                ): company_id
                for company_id in pending_companies
            }
            for future in as_completed(futures):
                company_id = futures[future]
                try:
                    audited_company_id, info, flat_items = future.result()
                    with persist_lock:
                        results[audited_company_id] = info
                        flat_quarter_results = [
                            item
                            for item in flat_quarter_results
                            if str(item.get("company_id") or "") != audited_company_id
                        ]
                        flat_quarter_results.extend(flat_items)
                except Exception as exc:
                    with persist_lock:
                        results[company_id] = {
                            "cached_supported_quarter_count": 0,
                            "dynamic_supported_quarter_count": 0,
                            "cached_supported_quarter_first": None,
                            "cached_supported_quarter_last": None,
                            "dynamic_supported_quarter_first": None,
                            "dynamic_supported_quarter_last": None,
                            "all_window_quarter_count": 0,
                            "excluded_unready_quarter_count": 0,
                            "audited_quarter_count": 0,
                            "status_counts": {"pass": 0, "review": 0, "fail": 0, "error": 1},
                            "issue_counts": {"critical": 0, "major": 0, "minor": 0},
                            "top_issue_codes": [],
                            "worst_quarters": [{"calendar_quarter": "n/a", "ok": False, "error": str(exc)}],
                            "quarters": {},
                        }
                        flat_quarter_results.append(
                            {
                                "company_id": company_id,
                                "calendar_quarter": "n/a",
                                "ok": False,
                                "error": str(exc),
                            }
                        )
                with persist_lock:
                    _persist_outputs(
                        companies=companies,
                        results=results,
                        flat_quarter_results=flat_quarter_results,
                        history_window=args.history_window,
                        require_full_coverage=require_full_coverage,
                        include_unready_audited=effective_include_unready,
                        output_path=output_path,
                        summary_markdown_path=summary_markdown_path,
                        ready_map_path=ready_map_path,
                    )

    rendered_payload = _persist_outputs(
        companies=companies,
        results=results,
        flat_quarter_results=flat_quarter_results,
        history_window=args.history_window,
        require_full_coverage=require_full_coverage,
        include_unready_audited=effective_include_unready,
        output_path=output_path,
        summary_markdown_path=summary_markdown_path,
        ready_map_path=ready_map_path,
    )
    if output_path is None:
        print(json.dumps(rendered_payload, ensure_ascii=False, indent=2))

    summary = dict(rendered_payload.get("summary") or {})
    if args.strict and int(summary.get("fail_or_error_quarter_count") or 0) > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
