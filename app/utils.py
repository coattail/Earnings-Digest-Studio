from __future__ import annotations

import json
import re
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def json_dumps(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return slug.strip("-") or "report"


def quarter_sort_key(quarter: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", quarter)
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))
