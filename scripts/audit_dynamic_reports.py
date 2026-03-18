from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.local_data import get_supported_quarters, list_companies  # noqa: E402
from app.services.reports import build_report_payload  # noqa: E402


def _sample_quarters(company_id: str, limit: int, history_window: int) -> list[str]:
    periods = get_supported_quarters(company_id, history_window=history_window, fetch_missing=True)
    if len(periods) <= limit:
        return periods
    midpoint = len(periods) // 2
    anchors = [periods[limit - 1], periods[midpoint], periods[-1]]
    ordered: list[str] = []
    for item in anchors:
        if item not in ordered:
            ordered.append(item)
    return ordered[:limit]


def _audit_payload(payload: dict[str, Any]) -> dict[str, Any]:
    historical_cube = list(payload.get("historical_cube") or [])
    warnings = list(payload.get("coverage_warnings") or [])
    return {
        "structure_dimension": payload.get("structure_dimension_used"),
        "current_segment_count": len(payload.get("current_segments") or []),
        "current_geography_count": len(payload.get("current_geographies") or []),
        "historical_missing_segments": [
            item.get("quarter_label")
            for item in historical_cube
            if not item.get("segments")
        ],
        "management_theme_count": len(payload.get("management_themes") or []),
        "qna_theme_count": len(payload.get("qna_themes") or []),
        "risk_count": len(payload.get("risks") or []),
        "catalyst_count": len(payload.get("catalysts") or []),
        "institutional_view_count": len(payload.get("institutional_views") or []),
        "warnings": warnings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit on-demand report generation quality across companies and quarters.")
    parser.add_argument("--company", action="append", dest="companies", help="Company id to audit. Repeatable.")
    parser.add_argument("--history-window", type=int, default=12, help="History window size.")
    parser.add_argument("--quarters-per-company", type=int, default=3, help="How many sample quarters to audit per company.")
    parser.add_argument("--refresh-source-materials", action="store_true", help="Refresh official source materials before auditing.")
    parser.add_argument("--output", help="Optional path to write JSON results.")
    args = parser.parse_args()

    companies = args.companies or [str(item["id"]) for item in list_companies()]
    results: dict[str, Any] = {}
    for company_id in companies:
        quarter_results: dict[str, Any] = {}
        for quarter in _sample_quarters(company_id, args.quarters_per_company, args.history_window):
            try:
                payload = build_report_payload(
                    company_id,
                    quarter,
                    history_window=args.history_window,
                    refresh_source_materials=args.refresh_source_materials,
                )
                quarter_results[quarter] = {
                    "ok": True,
                    **_audit_payload(payload),
                }
            except Exception as exc:
                quarter_results[quarter] = {
                    "ok": False,
                    "error": str(exc),
                }
        results[company_id] = quarter_results

    rendered = json.dumps(results, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
