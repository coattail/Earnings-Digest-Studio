from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.local_data import get_company_series, list_companies  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm quarterly series cache for remote-provider companies.")
    parser.add_argument("--company", action="append", dest="companies", help="Specific company id to warm. Repeatable.")
    args = parser.parse_args()

    selected = set(str(item).strip() for item in list(args.companies or []) if str(item).strip())
    remote_company_ids = [
        str(company["id"])
        for company in list_companies()
        if company.get("data_provider") != "local" and (not selected or str(company["id"]) in selected)
    ]

    failures: list[str] = []
    for company_id in remote_company_ids:
        try:
            periods, _ = get_company_series(company_id)
            print(f"[ok] {company_id}: {len(periods)} periods cached")
        except Exception as exc:
            failures.append(f"{company_id}: {exc}")
            print(f"[error] {company_id}: {exc}", file=sys.stderr)

    if failures:
        print("Quarterly series cache warm failed:", file=sys.stderr)
        for item in failures:
            print(f" - {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
