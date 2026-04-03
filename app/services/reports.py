from __future__ import annotations

import calendar as month_calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import threading
import uuid
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

from ..config import CACHE_DIR, DATA_DIR, ensure_directories
from ..db import get_connection
from ..utils import json_dumps, now_iso
from .charts import (
    format_money_bn,
    format_pct,
    render_balance_quality_svg,
    render_capital_allocation_svg,
    render_company_wordmark_svg,
    render_contribution_svg,
    render_current_quarter_svg,
    render_dual_ranked_svg,
    render_expectation_reset_svg,
    render_growth_overview_svg,
    render_guidance_svg,
    render_income_statement_svg,
    render_profitability_svg,
    render_segment_mix_svg,
    render_statement_translation_svg,
    render_structure_transition_svg,
    render_validation_checklist_svg,
)
from .institutional_views import get_institutional_views
from .local_data import (
    get_company,
    get_companyfacts_quarter_supplement,
    get_company_series,
    get_company_periods,
    get_segment_alias_map,
    _periods_are_consecutive_quarters,
    get_quarter_fixture,
    get_segment_history,
    get_supported_quarters,
    list_companies,
)
from .narrative_writer import (
    build_call_panel as _writer_build_call_panel,
    build_expectation_panel as _writer_build_expectation_panel,
    build_institutional_digest as _writer_build_institutional_digest,
    build_narrative_provenance as _writer_build_narrative_provenance,
    compose_layered_takeaways as _writer_compose_layered_takeaways,
    compose_summary_headline as _writer_compose_summary_headline,
    humanize_support_lines as _writer_humanize_support_lines,
    normalize_takeaways as _writer_normalize_takeaways,
    polish_generated_text as _writer_polish_generated_text,
)
from .official_materials import hydrate_source_materials
from .official_parsers import (
    COMPANY_SEGMENT_PROFILES,
    _extract_company_geographies,
    _extract_company_segments,
    _extract_generic_guidance_from_materials,
    _guidance_excerpt_signal_score,
    _load_materials,
    _merge_guidance_payload,
    parse_official_materials,
)
from .official_source_resolver import _discover_cached_material_sources, resolve_official_sources
from .report_quality import evaluate_report_payload
from .uploads import get_upload


APP_DIR = Path(__file__).resolve().parents[1]
REPORT_CACHE_DEPENDENCIES = [
    APP_DIR / "static" / "app.css",
    APP_DIR / "templates" / "report_document.html",
    APP_DIR / "services" / "charts.py",
    APP_DIR / "services" / "official_fixtures.py",
    APP_DIR / "services" / "official_parsers.py",
    APP_DIR / "services" / "institutional_views.py",
    APP_DIR / "services" / "narrative_writer.py",
    APP_DIR / "services" / "reports.py",
    APP_DIR / "services" / "seed_data.py",
]
REPORT_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=3, thread_name_prefix="report-job")
REPORT_JOB_FUTURES: dict[str, Any] = {}
REPORT_JOB_FUTURES_LOCK = threading.Lock()
REPORT_JOB_STATES: dict[str, dict[str, Any]] = {}
REPORT_JOB_STATES_LOCK = threading.Lock()
REPORT_JOB_STATE_DIR = DATA_DIR / "report-jobs"
REPORT_JOB_STATE_FILE_LOCK = threading.Lock()
ProgressCallback = Callable[[float, str, str], None]
RECENT_REPORT_CACHE_TTL_SECONDS = 6 * 60 * 60
HISTORICAL_REPORT_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60
REPORT_PAYLOAD_SCHEMA_VERSION = 26
FULL_COVERAGE_ENV = "EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE"
FULL_COVERAGE_READY_MAP_PATH = APP_DIR.parent / "data" / "cache" / "full-coverage-ready-quarters.json"
HISTORICAL_OFFICIAL_QUARTER_CACHE_DIR = CACHE_DIR / "historical-official-quarter-cache"
HISTORICAL_OFFICIAL_QUARTER_CACHE_VERSION = 14
HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
HISTORICAL_OFFICIAL_QUARTER_MEMORY_LOCK = threading.Lock()


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().casefold() not in {"0", "false", "no", "off"}


def _require_full_coverage_mode() -> bool:
    # Default on: avoid silently shipping reports that still rely on fallback wording.
    return _env_flag(FULL_COVERAGE_ENV, default=True)


def _full_coverage_ready_quarters(company_id: str, history_window: int) -> Optional[list[str]]:
    path = FULL_COVERAGE_READY_MAP_PATH
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not bool(payload.get("require_full_coverage")):
        return None
    if int(payload.get("history_window") or 0) != int(history_window):
        return None
    companies = payload.get("companies")
    if not isinstance(companies, dict):
        return None
    company_entry = companies.get(company_id)
    if not isinstance(company_entry, dict):
        return None
    if not bool(company_entry.get("all_quarters_audited")):
        return None
    ready_quarters = company_entry.get("ready_quarters")
    if not isinstance(ready_quarters, list):
        return []
    return [str(item) for item in ready_quarters if str(item)]


def _filter_supported_quarters_by_full_coverage(
    company_id: str,
    supported_quarters: list[str],
    history_window: int,
) -> tuple[list[str], bool]:
    if not _require_full_coverage_mode():
        return list(supported_quarters), False
    ready_quarters = _full_coverage_ready_quarters(company_id, history_window)
    if ready_quarters is None:
        return list(supported_quarters), False
    ready_set = set(ready_quarters)
    return [quarter for quarter in supported_quarters if quarter in ready_set], True


def _historical_official_quarter_cache_path(company_id: str, calendar_quarter: str) -> Path:
    ensure_directories()
    root = HISTORICAL_OFFICIAL_QUARTER_CACHE_DIR / str(company_id)
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{calendar_quarter}.json"


def _clone_cached_official_quarter_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "latest_kpis": dict(payload.get("latest_kpis") or {}),
        "current_segments": [dict(item) for item in list(payload.get("current_segments") or []) if isinstance(item, dict)],
        "current_geographies": [dict(item) for item in list(payload.get("current_geographies") or []) if isinstance(item, dict)],
        "guidance": dict(payload.get("guidance") or {}),
        "source_url": payload.get("source_url"),
        "source_date": payload.get("source_date"),
        "profit_basis": payload.get("profit_basis"),
        "guidance_source_label": payload.get("guidance_source_label"),
    }


def _historical_source_is_annual_sec_filing(source: dict[str, Any]) -> bool:
    if str(source.get("kind") or "") != "sec_filing":
        return False
    reference = " ".join(
        str(source.get(key) or "")
        for key in ("label", "form", "url", "fetch_url", "raw_path")
    ).lower()
    return any(token in reference for token in ("10-k", "10k", "20-f", "20f", "annual report"))


def _historical_sources_are_annual_sec_only(sources: list[dict[str, Any]]) -> bool:
    normalized = [dict(item) for item in list(sources or []) if isinstance(item, dict)]
    return bool(normalized) and all(_historical_source_is_annual_sec_filing(item) for item in normalized)


def _sanitize_historical_official_quarter_payload(
    company: dict[str, Any],
    payload: dict[str, Any],
    *,
    seed_entry: Optional[dict[str, Any]] = None,
    sources: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    sanitized = _clone_cached_official_quarter_payload(payload)
    latest_kpis = dict(sanitized.get("latest_kpis") or {})
    reference_profit = (
        latest_kpis.get("net_income_bn")
        if latest_kpis.get("net_income_bn") is not None
        else dict(seed_entry or {}).get("net_income_bn")
    )
    trusted_revenue = _historical_revenue_candidate(
        dict(seed_entry or {}).get("revenue_bn"),
        latest_kpis.get("revenue_bn"),
        reference_profit_bn=reference_profit,
    )
    if trusted_revenue is None:
        trusted_revenue = latest_kpis.get("revenue_bn")
    if trusted_revenue is None:
        trusted_revenue = dict(seed_entry or {}).get("revenue_bn")

    current_segments = [dict(item) for item in list(sanitized.get("current_segments") or []) if isinstance(item, dict)]
    if current_segments and _historical_sources_are_annual_sec_only(list(sources or [])):
        current_segments = []
    normalized_segments = _normalize_historical_segments(company, current_segments, trusted_revenue)
    normalized_segments.sort(key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
    sanitized["current_segments"] = normalized_segments

    guidance = dict(sanitized.get("guidance") or {})
    commentary = _normalize_guidance_commentary(guidance.get("commentary"))
    if commentary:
        guidance["commentary"] = commentary
    else:
        guidance.pop("commentary", None)
    sanitized["guidance"] = guidance
    return sanitized


def _load_historical_official_quarter_cache(company_id: str, calendar_quarter: str) -> Optional[dict[str, Any]]:
    cache_key = (str(company_id), str(calendar_quarter))
    with HISTORICAL_OFFICIAL_QUARTER_MEMORY_LOCK:
        cached = HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE.get(cache_key)
    if cached is not None:
        if int(cached.get("cache_version") or 0) == HISTORICAL_OFFICIAL_QUARTER_CACHE_VERSION:
            return _clone_cached_official_quarter_payload(cached)
        with HISTORICAL_OFFICIAL_QUARTER_MEMORY_LOCK:
            HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE.pop(cache_key, None)
    path = _historical_official_quarter_cache_path(company_id, calendar_quarter)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if int(payload.get("cache_version") or 0) != HISTORICAL_OFFICIAL_QUARTER_CACHE_VERSION:
        return None
    normalized = _clone_cached_official_quarter_payload(payload)
    with HISTORICAL_OFFICIAL_QUARTER_MEMORY_LOCK:
        HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE[cache_key] = {
            "cache_version": HISTORICAL_OFFICIAL_QUARTER_CACHE_VERSION,
            **normalized,
        }
    return _clone_cached_official_quarter_payload(normalized)


def _store_historical_official_quarter_cache(
    company_id: str,
    calendar_quarter: str,
    payload: dict[str, Any],
) -> None:
    normalized = _clone_cached_official_quarter_payload(payload)
    serialized = {
        "cache_version": HISTORICAL_OFFICIAL_QUARTER_CACHE_VERSION,
        **normalized,
    }
    path = _historical_official_quarter_cache_path(company_id, calendar_quarter)
    path.write_text(json.dumps(serialized, ensure_ascii=False, indent=2), encoding="utf-8")
    with HISTORICAL_OFFICIAL_QUARTER_MEMORY_LOCK:
        HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE[(str(company_id), str(calendar_quarter))] = dict(serialized)


def _progress_value(value: float) -> float:
    return round(max(0.0, min(1.0, float(value))), 4)


def _emit_progress(
    progress_callback: Optional[ProgressCallback],
    progress: float,
    stage: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(_progress_value(progress), stage, message)


def _stage_progress_callback(
    progress_callback: Optional[ProgressCallback],
    *,
    stage: str,
    start: float,
    end: float,
) -> Callable[[float, str], None]:
    span = max(0.0, end - start)

    def _callback(progress: float, message: str) -> None:
        scaled = start + span * max(0.0, min(1.0, float(progress)))
        _emit_progress(progress_callback, scaled, stage, message)

    return _callback


def month_to_calendar_quarter(month_token: str) -> str:
    year, month = month_token.split("-")
    quarter = (int(month) - 1) // 3 + 1
    return f"{year}Q{quarter}"


def resolve_calendar_quarter_from_months(coverage_months: list[str]) -> str:
    counts = Counter(month_to_calendar_quarter(token) for token in coverage_months)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator * 100


def _ttm_sum(entries: list[dict[str, Any]], key: str, end_index: int) -> Optional[float]:
    if end_index < 3:
        return None
    window = entries[end_index - 3 : end_index + 1]
    values = [entry.get(key) for entry in window]
    if any(value is None for value in values):
        return None
    return float(sum(float(value) for value in values))


def _ttm_roe_pct(entries: list[dict[str, Any]], end_index: int) -> Optional[float]:
    ttm_net_income = _ttm_sum(entries, "net_income_bn", end_index)
    if ttm_net_income is None:
        return None
    equity_window = [entries[position].get("equity_bn") for position in range(max(0, end_index - 4), end_index + 1)]
    equity_values = [float(value) for value in equity_window if value not in (None, 0)]
    if len(equity_values) < 2:
        return None
    average_equity = sum(equity_values) / len(equity_values)
    if average_equity <= 0:
        return None
    roe_pct = float(ttm_net_income) / average_equity * 100
    if -100 < roe_pct < 250:
        return roe_pct
    return None


def _normalize_equity_bn_value(
    value: Optional[float],
    *,
    reference_equity_bn: Optional[float] = None,
    revenue_bn: Optional[float] = None,
) -> Optional[float]:
    if value is None:
        return None
    normalized = float(value)
    if normalized <= 0:
        return None
    reference_value = float(reference_equity_bn) if reference_equity_bn not in (None, 0) else None
    revenue_value = float(revenue_bn) if revenue_bn not in (None, 0) else None
    if normalized > 1000:
        if reference_value not in (None, 0):
            shrunk = normalized / 1000.0
            if abs(shrunk - reference_value) / max(abs(reference_value), 1.0) <= 0.4:
                return shrunk
        if revenue_value not in (None, 0) and normalized / revenue_value > 60:
            return normalized / 1000.0
    return normalized


def _quarter_parts(calendar_quarter: str) -> tuple[int, int]:
    return int(calendar_quarter[:4]), int(calendar_quarter[-1])


def _shift_calendar_quarter(calendar_quarter: str, delta: int) -> str:
    year, quarter = _quarter_parts(calendar_quarter)
    index = year * 4 + (quarter - 1) + int(delta)
    shifted_year = index // 4
    shifted_quarter = index % 4 + 1
    return f"{shifted_year}Q{shifted_quarter}"


def _quarter_window(calendar_quarter: str, window: int) -> list[str]:
    return [_shift_calendar_quarter(calendar_quarter, offset) for offset in range(-(window - 1), 1)]


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + delta
    shifted_year = index // 12
    shifted_month = index % 12 + 1
    return shifted_year, shifted_month


def _calendar_quarter_months(calendar_quarter: str) -> list[str]:
    year, quarter = _quarter_parts(calendar_quarter)
    start_month = (quarter - 1) * 3 + 1
    return [f"{year}-{month:02d}" for month in range(start_month, start_month + 3)]


def _estimate_period_end(calendar_quarter: str) -> str:
    year, quarter = _quarter_parts(calendar_quarter)
    end_month = quarter * 3
    end_day = month_calendar.monthrange(year, end_month)[1]
    return f"{year}-{end_month:02d}-{end_day:02d}"


def _coverage_months_for_period(calendar_quarter: str, period_end: Optional[str]) -> list[str]:
    if period_end and len(period_end) >= 7:
        try:
            year, month = (int(part) for part in period_end[:7].split("-"))
        except ValueError:
            year, month = (0, 0)
        if year and month:
            months = []
            for offset in (-2, -1, 0):
                shifted_year, shifted_month = _shift_month(year, month, offset)
                months.append(f"{shifted_year}-{shifted_month:02d}")
            if resolve_calendar_quarter_from_months(months) == calendar_quarter:
                return months
    return _calendar_quarter_months(calendar_quarter)


def _series_period_end(series: dict[str, Any], calendar_quarter: str) -> Optional[str]:
    return series.get("periodMeta", {}).get(calendar_quarter, {}).get("date_key")


def _is_iso_date_token(value: Optional[str]) -> bool:
    token = str(value or "")
    return len(token) == 10 and token[4] == "-" and token[7] == "-"


def _resolved_period_end(
    calendar_quarter: str,
    entry_period_end: Optional[str] = None,
    fixture_period_end: Optional[str] = None,
    series_period_end: Optional[str] = None,
) -> str:
    for candidate in (fixture_period_end, entry_period_end, series_period_end):
        if _is_iso_date_token(candidate):
            return str(candidate)
    return _estimate_period_end(calendar_quarter)


def _resolved_fiscal_label(
    company: dict[str, Any],
    calendar_quarter: str,
    period_end: Optional[str],
    explicit_label: Optional[str] = None,
) -> str:
    label = str(explicit_label or "").strip()
    if label and not re.fullmatch(r"\d{4}Q[1-4]", label, re.IGNORECASE):
        return label
    try:
        fiscal_year_end_month = int((company.get("official_source") or {}).get("fiscal_year_end_month") or 0)
    except (TypeError, ValueError):
        fiscal_year_end_month = 0
    if fiscal_year_end_month < 1 or fiscal_year_end_month > 12 or fiscal_year_end_month == 12:
        return label or calendar_quarter
    period_token = str(period_end or "").strip() or _estimate_period_end(calendar_quarter)
    period_date = _parse_date_token(period_token)
    if period_date is None:
        return label or calendar_quarter
    if period_date.day <= 6:
        # Some issuers report on the first weekend day of the next month (for example 2017-04-01).
        # Pull those endpoints back into the covered month before deriving the fiscal quarter.
        period_date = period_date - timedelta(days=6)
    fiscal_year_start_month = fiscal_year_end_month % 12 + 1
    fiscal_quarter = ((period_date.month - fiscal_year_start_month) % 12) // 3 + 1
    fiscal_year = period_date.year + (1 if period_date.month > fiscal_year_end_month else 0)
    return f"Q{fiscal_quarter} FY{fiscal_year}"


def _parse_date_token(value: str) -> Optional[date]:
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def _mean(values: list[Optional[float]]) -> Optional[float]:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _clamp_score(value: float) -> int:
    return int(max(32, min(98, round(value))))


def _money_delta_label(value: Optional[float], symbol: str) -> str:
    if value is None:
        return "-"
    prefix = "+" if value > 0 else ""
    return f"{prefix}{format_money_bn(value, symbol)}"


def _metric_or_fallback(value: Optional[float], fallback: Optional[float]) -> Optional[float]:
    return value if value is not None else fallback


def _value_or_history(metric: Optional[float], history_value: Optional[float]) -> Optional[float]:
    return metric if metric is not None else history_value


def _guidance_uses_official_context(guidance: dict[str, Any]) -> bool:
    return guidance.get("mode") in {"official", "official_context"}


def _historical_profit_series_value(company: dict[str, Any], latest_kpis: dict[str, Any]) -> Optional[float]:
    if company.get("historical_profit_basis") == "operating_income":
        return latest_kpis.get("operating_income_bn")
    return latest_kpis.get("net_income_bn")


def _resolve_guidance_payload(guidance: dict[str, Any], history: list[dict[str, Any]]) -> dict[str, Any]:
    baseline = _build_generic_guidance(history)
    revenue_missing = all(guidance.get(key) is None for key in ("revenue_bn", "revenue_low_bn", "revenue_high_bn"))
    if guidance.get("mode") == "proxy":
        return dict(guidance)
    if guidance.get("mode") == "official_context" or revenue_missing:
        merged = dict(baseline)
        merged.update(guidance)
        if guidance.get("mode") == "official_context":
            merged["mode"] = "official_context"
        if revenue_missing and baseline.get("revenue_bn") is not None:
            merged["revenue_derived_from_baseline"] = True
        return merged
    merged = dict(guidance)
    if merged.get("gaap_gross_margin_pct") is None:
        merged["gaap_gross_margin_pct"] = baseline.get("gaap_gross_margin_pct")
    if not merged.get("comparison_margin_label"):
        merged["comparison_margin_label"] = baseline.get("comparison_margin_label")
    return merged


def _build_profit_growth_takeaway(
    latest_kpis: dict[str, Any],
    latest_history: dict[str, Any],
    money_symbol: str,
) -> Optional[str]:
    net_income_bn = _value_or_history(latest_kpis.get("net_income_bn"), latest_history.get("net_income_bn"))
    net_income_yoy_pct = _value_or_history(latest_kpis.get("net_income_yoy_pct"), latest_history.get("net_income_yoy_pct"))
    net_income_qoq_pct = _value_or_history(latest_kpis.get("net_income_qoq_pct"), latest_history.get("net_income_qoq_pct"))
    if net_income_bn is None and net_income_yoy_pct is None and net_income_qoq_pct is None:
        return None
    if net_income_bn is not None:
        bits = [f"净利润 {format_money_bn(net_income_bn, money_symbol)}"]
        if net_income_yoy_pct is not None:
            bits.append(f"同比 {format_pct(net_income_yoy_pct, signed=True)}")
        if net_income_qoq_pct is not None:
            bits.append(f"环比 {format_pct(net_income_qoq_pct, signed=True)}")
        return "，".join(bits) + "。"
    return f"净利润同比 {format_pct(net_income_yoy_pct, signed=True)}。"


def _clean_summary_fragment(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", str(text)).strip(" 。；;，,")
    return cleaned or None


def _normalize_guidance_commentary(text: Optional[str], *, limit: int = 120) -> Optional[str]:
    cleaned = _clean_summary_fragment(text)
    short_guidance_like = (
        bool(cleaned)
        and len(str(cleaned)) >= 14
        and re.search(r"[\u4e00-\u9fff]", str(cleaned)) is not None
        and any(
            token in str(cleaned)
            for token in ("指引", "展望", "需求", "订单", "供给", "利润率", "ROI", "CapEx", "Cloud", "Search")
        )
    )
    if not cleaned or (not _text_looks_human_readable(cleaned, min_readable_terms=3) and not short_guidance_like):
        return None
    cleaned = re.sub(
        r"(?:[；;，,]\s*|\s*)[^；。]*?(?:预计|expect(?:ed|s)?)"
        r"[^；。]*?(?:-|—|–)\s*(?:到|to)\s*(?:-|—|–)[^；。]*[；。]?",
        "。",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[；;。]{2,}", "。", cleaned).strip(" 。；;，,")
    if not cleaned:
        return None
    lowered = cleaned.lower()
    noise_tokens = (
        "deferred revenue",
        "earnings per share",
        "basic and diluted",
        "note 3",
        "note 4",
        "note 6",
        "one-to-two years",
        "two-to-three years",
        "three years",
        "net income in millions",
        "the following table shows",
    )
    if sum(token in lowered for token in noise_tokens) >= 2:
        return None
    compact = cleaned
    if len(cleaned) > limit:
        sentences = [item.strip() for item in re.split(r"(?<=[.!?。！？])\s+", cleaned) if item.strip()]
        best_chunk = ""
        best_score = -100
        for index in range(len(sentences)):
            chunk = " ".join(sentences[index : index + 2]).strip()
            if not chunk:
                continue
            score = _guidance_excerpt_signal_score(chunk)
            if score > best_score:
                best_score = score
                best_chunk = chunk
        if best_chunk and best_score > 0:
            compact = _excerpt_text(best_chunk, max(limit, 180))
        else:
            compact = _excerpt_text(cleaned, limit)
    if compact and compact[-1] not in "。！？!?":
        compact += "。"
    return compact


def _segment_name_token(name: str) -> str:
    normalized = str(name or "").strip().casefold()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("/", " ")
    normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _normalize_segment_items(
    company: dict[str, Any],
    segments: list[dict[str, Any]],
    *,
    allow_rollup_aliases: bool = True,
) -> list[dict[str, Any]]:
    segment_rollup_children = {
        "apple": {
            "Products": {
                "iPhone",
                "Mac",
                "iPad",
                "Wearables, Home and Accessories",
            },
        },
        "alphabet": {
            "Google Services": {
                "Google Search & other",
                "YouTube ads",
                "Google Network",
                "Google subscriptions, platforms, and devices",
            },
            "Google properties": {
                "Google Search & other",
                "YouTube ads",
            },
        },
    }
    company_id = str(company.get("id") or "")
    aliases = get_segment_alias_map(company, allow_rollups=allow_rollup_aliases)
    display_order = list(company.get("segment_order") or [])
    if not display_order:
        display_order = [str(profile.get("name") or "") for profile in COMPANY_SEGMENT_PROFILES.get(str(company.get("id") or ""), [])]
    alias_tokens = {
        _segment_name_token(alias): canonical
        for alias, canonical in aliases.items()
        if _segment_name_token(alias)
    }
    canonical_tokens = {
        _segment_name_token(name): name
        for name in display_order
        if _segment_name_token(name)
    }
    order_index = {name: index for index, name in enumerate(display_order)}
    normalized: list[dict[str, Any]] = []
    merged_by_name: dict[str, dict[str, Any]] = {}
    for original_index, raw in enumerate(segments):
        raw_name = str(raw.get("name") or "").strip()
        token = _segment_name_token(raw_name)
        name = aliases.get(raw_name, alias_tokens.get(token, canonical_tokens.get(token, raw_name)))
        if not name:
            continue
        payload = dict(raw)
        payload["name"] = name
        payload["_original_index"] = original_index
        existing = merged_by_name.get(name)
        if existing is None:
            normalized.append(payload)
            merged_by_name[name] = payload
            continue
        for numeric_key in ("value_bn", "share_pct", "operating_income_bn"):
            if payload.get(numeric_key) is not None:
                existing[numeric_key] = float(existing.get(numeric_key) or 0.0) + float(payload[numeric_key])
        for passthrough_key in ("yoy_pct", "margin_pct", "note", "logo_key", "display_name", "chip_labels", "color", "fill", "scope"):
            if existing.get(passthrough_key) is None and payload.get(passthrough_key) is not None:
                existing[passthrough_key] = payload[passthrough_key]
    if order_index:
        normalized.sort(
            key=lambda item: (
                order_index.get(str(item.get("name") or ""), len(order_index) + int(item.get("_original_index") or 0)),
                int(item.get("_original_index") or 0),
            )
        )
    if company_id == "alphabet":
        google_other = next((item for item in normalized if str(item.get("name") or "") == "Google other"), None)
        subscriptions = next(
            (
                item
                for item in normalized
                if str(item.get("name") or "") == "Google subscriptions, platforms, and devices"
            ),
            None,
        )
        if google_other is not None and subscriptions is not None:
            google_other_value = float(google_other.get("value_bn") or 0.0)
            subscriptions_value = float(subscriptions.get("value_bn") or 0.0)
            tolerance = max(0.08, max(google_other_value, subscriptions_value) * 0.03)
            if abs(google_other_value - subscriptions_value) <= tolerance:
                normalized = [
                    item
                    for item in normalized
                    if str(item.get("name") or "") != "Google subscriptions, platforms, and devices"
                ]
    names = {str(item.get("name") or "") for item in normalized}
    for rollup_name, child_names in segment_rollup_children.get(company_id, {}).items():
        if rollup_name in names and names.intersection(set(child_names)):
            normalized = [item for item in normalized if str(item.get("name") or "") != rollup_name]
            names.discard(rollup_name)
    for item in normalized:
        item.pop("_original_index", None)
    return normalized


def _build_report_style(company: dict[str, Any]) -> dict[str, str]:
    style = dict(company.get("report_style") or {})
    description = str(company.get("description") or "")
    base = {
        "cover_eyebrow": "Deep Quarterly Digest",
        "quarterly_label": "Quarterly Layer",
        "historical_label": "12-Quarter Layer",
        "evidence_label": "Evidence Layer",
        "cover_tagline": description,
        "quarterly_lens": str(company.get("card_headline") or description),
        "historical_lens": "12 季成长、结构迁移与利润兑现",
        "evidence_lens": "官方财报新闻稿、SEC filing、演示稿与电话会原文",
        "cover_rhythm": "structured",
        "section_divider": "beam",
    }
    base.update({key: str(value) for key, value in style.items() if value})
    return base


def _build_section_meta(company: dict[str, Any]) -> dict[str, dict[str, str]]:
    style = _build_report_style(company)

    def _note(base: str, lens: str) -> str:
        return f"{base} 这一页重点围绕 {lens} 展开。"

    return {
        "cover": {
            "eyebrow": style["cover_eyebrow"],
            "note": style["cover_tagline"],
        },
        "summary": {
            "eyebrow": style["quarterly_label"],
            "note": _note("把公司、季度、收入兑现、现金流与结构模式压缩到一页内快速浏览。", style["quarterly_lens"]),
        },
        "current_quarter": {
            "eyebrow": style["quarterly_label"],
            "note": _note("收入、净利润、毛利率一页并读，强调当季、上季与去年同期的相对位置。", style["quarterly_lens"]),
        },
        "guidance": {
            "eyebrow": style["quarterly_label"],
            "note": "",
        },
        "mix": {
            "eyebrow": style["quarterly_label"],
            "note": _note("同页并读分部结构与地区结构，让“增长来自哪里”不只停留在一句话上。", style["quarterly_lens"]),
        },
        "income_statement": {
            "eyebrow": style["quarterly_label"],
            "note": _note("以利润表路径串起收入、成本、经营费用和净利润，帮助快速看清当季兑现结构。", style["quarterly_lens"]),
        },
        "translation": {
            "eyebrow": style["quarterly_label"],
            "note": _note("将收入、成本、费用与利润科目直译为中文，同时保留英文原词，方便快速比对官方利润表。", style["quarterly_lens"]),
        },
        "management_qna": {
            "eyebrow": style["quarterly_label"],
            "note": _note("不做大段 transcript 堆砌，而是用主题强度、关键摘录和证据卡片组织阅读。", style["quarterly_lens"]),
        },
        "risks": {
            "eyebrow": style["quarterly_label"],
            "note": _note("保持图表化表达，避免把风险和催化剂写成大段纯文字备忘录。", style["quarterly_lens"]),
        },
        "views": {
            "eyebrow": style["quarterly_label"],
            "note": _note("补充公开媒体转述的头部机构观点，帮助把管理层口径与外部 sell-side 关注点放到同一页里对照。", style["quarterly_lens"]),
        },
        "capital_allocation": {
            "eyebrow": style["quarterly_label"],
            "note": _note("把收入、利润、现金流和资本开支放到同一条阅读路径里，避免只盯会计利润。", style["quarterly_lens"]),
        },
        "expectation_reset": {
            "eyebrow": style["quarterly_label"],
            "note": _note("对照上一季口径、本季实际和本次新指引，回答市场预期是被上修、持平还是被重置。", style["quarterly_lens"]),
        },
        "balance_quality": {
            "eyebrow": style["quarterly_label"],
            "note": _note("从权益、ROE、集中度和订单/库存等线索看经营质量，不让判断停留在利润表。", style["quarterly_lens"]),
        },
        "validation": {
            "eyebrow": style["quarterly_label"],
            "note": _note("把多头验证点、风险验证点和下季度追问清单收束到一页，方便继续跟踪。", style["quarterly_lens"]),
        },
        "growth": {
            "eyebrow": style["historical_label"],
            "note": _note("从单季结果切换到完整趋势视角，回答“这家公司过去 12 季到底发生了什么变化”。", style["historical_lens"]),
        },
        "transition": {
            "eyebrow": style["historical_label"],
            "note": _note("分部历史完整时展示真实结构迁移；缺失时明确给出降级解释而不是硬拼图表。", style["historical_lens"]),
        },
        "profitability": {
            "eyebrow": style["historical_label"],
            "note": _note("不只观察收入，连同毛利率、净利率、收入同比和 ROE 一起读，判断兑现质量。", style["historical_lens"]),
        },
        "contribution": {
            "eyebrow": style["historical_label"],
            "note": _note("回答“增长主要来自哪里、谁在拖累、结构是否更集中”。", style["historical_lens"]),
        },
        "insights": {
            "eyebrow": style["historical_label"],
            "note": _note("把数据页收束成可以直接阅读的研究判断，同时保留证据锚点。", style["historical_lens"]),
        },
        "evidence": {
            "eyebrow": style["evidence_label"],
            "note": _note("用可追溯的材料卡片收尾，方便回到原始文件继续查证。", style["evidence_lens"]),
        },
    }


def _short_section_text(text: Optional[str], limit: int = 58) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip(" ，,；;。")
    if not cleaned:
        return ""
    return _excerpt_text(cleaned, limit).rstrip(".") + "。"


def _section_label(name: Optional[str]) -> str:
    raw = re.sub(r"\s+", " ", str(name or "")).strip()
    if not raw:
        return ""
    aliases = {
        "Wearables, Home and Accessories": "Wearables",
        "Google subscriptions, platforms, and devices": "Google subs",
        "Google Search & other": "Search",
        "Productivity and Business Processes": "Productivity",
        "More Personal Computing": "Personal Computing",
        "Intelligent Cloud": "Cloud",
        "Americas Excluding U.S.": "Americas ex-U.S.",
        "Rest of Asia Pacific": "Rest of APAC",
        "Google properties": "Google properties",
    }
    if raw in aliases:
        return aliases[raw]
    if len(raw) <= 18:
        return raw
    compact = raw.replace(" and ", " & ").replace(", ", " / ")
    return compact if len(compact) <= 22 else _excerpt_text(compact, 22).rstrip(".")


def _section_item_name(items: list[dict[str, Any]]) -> Optional[str]:
    valid_items = [item for item in items if item.get("value_bn") is not None]
    if not valid_items:
        return None
    return str(max(valid_items, key=lambda item: float(item.get("value_bn") or 0.0)).get("name") or "").strip() or None


def _history_start_end_revenue_text(history: list[dict[str, Any]], money_symbol: str) -> Optional[str]:
    if not history:
        return None
    start = history[0]
    latest = history[-1]
    if start.get("revenue_bn") is None or latest.get("revenue_bn") is None:
        return None
    return (
        f"12 季收入从 {format_money_bn(start.get('revenue_bn'), money_symbol)} "
        f"走到 {format_money_bn(latest.get('revenue_bn'), money_symbol)}。"
    )


def _history_delta_leader(history: list[dict[str, Any]]) -> Optional[str]:
    if len(history) < 2:
        return None
    first_segments = {str(item.get("name") or ""): float(item.get("value_bn") or 0.0) for item in list(history[0].get("segments") or [])}
    latest_segments = {str(item.get("name") or ""): float(item.get("value_bn") or 0.0) for item in list(history[-1].get("segments") or [])}
    candidates: list[tuple[str, float]] = []
    for name, latest_value in latest_segments.items():
        candidates.append((name, latest_value - first_segments.get(name, 0.0)))
    candidates = [item for item in candidates if item[0] and item[1] > 0.05]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[1])[0]


def _build_dynamic_section_meta(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    *,
    structure_dimension: str,
    money_symbol: str,
    institutional_views: list[dict[str, Any]],
    transcript_summary: Optional[dict[str, Any]],
    qna_topics: list[dict[str, Any]],
    merged_sources: list[dict[str, Any]],
    guidance_note: str,
) -> dict[str, dict[str, str]]:
    meta = _build_section_meta(company)
    latest_kpis = dict(fixture.get("latest_kpis") or {})
    latest = history[-1] if history else {}
    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    net_income_bn = _value_or_history(latest_kpis.get("net_income_bn"), latest.get("net_income_bn"))
    gross_margin_pct = _metric_or_fallback(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct"))
    top_segment = _section_label(_section_item_name(list(fixture.get("current_segments") or [])))
    top_geo = _section_label(_section_item_name(list(fixture.get("current_geographies") or [])))
    duplicate_regional_structure = (
        bool(fixture.get("current_segments"))
        and bool(fixture.get("current_geographies"))
        and all(str(item.get("scope") or "").casefold() == "regional_segment" for item in list(fixture.get("current_geographies") or []))
        and {
            (
                str(item.get("name") or "").casefold(),
                round(float(item.get("value_bn") or 0.0), 3),
            )
            for item in list(fixture.get("current_segments") or [])
        }
        == {
            (
                str(item.get("name") or "").casefold(),
                round(float(item.get("value_bn") or 0.0), 3),
            )
            for item in list(fixture.get("current_geographies") or [])
        }
    )
    segment_copy = _segment_copy_profile(list(fixture.get("current_segments") or []))
    top_risk = _section_label(str(((fixture.get("risks") or [{}])[0]).get("label") or "").strip())
    top_catalyst = _section_label(str(((fixture.get("catalysts") or [{}])[0]).get("label") or "").strip())
    top_qna = _section_label(str(((qna_topics or [{}])[0]).get("label") or "").strip())
    second_qna = _section_label(str(((qna_topics or [{}, {}])[1]).get("label") or "").strip()) if len(qna_topics) > 1 else ""
    first_source = str(((merged_sources or [{}])[0]).get("label") or "").strip()
    leading_view = _section_label(str(((institutional_views or [{}])[0]).get("firm") or "").strip())
    growth_driver = _section_label(_history_delta_leader(history))
    start_end_text = _history_start_end_revenue_text(history, money_symbol)
    guidance = dict(fixture.get("guidance") or {})
    guidance_revenue = guidance.get("revenue_bn")
    guidance_mode = str(guidance.get("mode") or "proxy")
    guidance_revenue_text = (
        f"下一阶段收入参考约 {format_money_bn(guidance_revenue, money_symbol)}。"
        if guidance_revenue is not None
        else None
    )
    transition_focus = None
    if structure_dimension == "segment":
        first_name = _section_label(_section_item_name(list(history[0].get("segments") or []))) if history else None
        latest_name = _section_label(_section_item_name(list(history[-1].get("segments") or []))) if history else None
        if first_name and latest_name:
            transition_focus = (
                f"先看头部分部是否仍是 {latest_name}。"
                if first_name == latest_name
                else f"头部分部已从 {first_name} 切换到 {latest_name}。"
            )
    elif structure_dimension == "geography":
        transition_focus = "这一页按地区口径阅读，比硬拼分部更可信。"
    else:
        transition_focus = "结构历史不连续，重点看趋势而不是强行读占比。"

    meta["summary"]["note"] = _short_section_text(
        f"先看收入与净利润，再看{segment_copy['top_label']}和头部地区是否共同支撑本季结果。"
        if not duplicate_regional_structure
        else "先看收入与净利润，再看官方经营分部如何支撑本季结果。"
        if top_segment and top_geo
        else "先看收入、利润与结构模式，再决定后面几页该优先追哪条主线。"
    )
    meta["current_quarter"]["note"] = _short_section_text(
        (
            f"本季收入 {format_money_bn(revenue_bn, money_symbol)}、净利润 {format_money_bn(net_income_bn, money_symbol)}，"
            f"毛利率 {format_pct(gross_margin_pct)}，先看兑现强度。"
        )
        if revenue_bn is not None and net_income_bn is not None and gross_margin_pct is not None
        else "把本季收入、利润和利润率放到同一页里读，先判断结果是否真的兑现。"
    )
    meta["guidance"]["note"] = _short_section_text(
        guidance_note
        if guidance_mode == "proxy"
        else (
            f"{guidance_revenue_text or '这一页优先读官方指引区间与语气。'} 看管理层给出的下一阶段目标，是否承接本季结果。"
            if guidance_mode == "official"
            else "公司没有给出明确数值指引时，先读官方展望语气，再看经营基线是否能接住。"
        )
    )
    meta["mix"]["note"] = _short_section_text(
        (
            "这一页先看官方经营分部；公司没有单独披露终端地理收入时，地区视角不再重复展示同一口径。"
            if duplicate_regional_structure
            else
            f"{segment_copy['side_label']}先看 {top_segment}，地区侧再看 {top_geo}；两边是否共振，比单看占比更重要。"
            if top_segment and top_geo
            else f"先抓头部结构 {top_segment or top_geo or '主线'}，再判断增长到底来自结构改善还是总量回升。"
        )
    )
    meta["income_statement"]["note"] = _short_section_text(
        (
            f"{top_segment or segment_copy['top_label']}把收入盘子撑到 {format_money_bn(revenue_bn, money_symbol)}，"
            f"最后留在净利润端的是 {format_money_bn(net_income_bn, money_symbol)}；成本率和税项是主线。"
        )
        if revenue_bn is not None and net_income_bn is not None
        else "这一页真正要盯的是成本率、费用率和税项，别只看净利润结果。"
    )
    meta["translation"]["note"] = _short_section_text(
        f"把 {top_segment or '收入主线'}、成本和利润率三组科目先认熟，后面回看财报原文会快很多。"
    )
    meta["management_qna"]["note"] = _short_section_text(
        (
            f"电话会先抓管理层反复强调的 {top_qna}，再看问答是否追问 {second_qna or top_risk or '增长持续性'}。"
            if transcript_summary
            else f"电话会原文不完整时，先围绕 {top_qna or top_segment or '经营主线'} 看研究问题清单。"
        )
    )
    meta["risks"]["note"] = _short_section_text(
        (
            f"风险先看 {top_risk}，催化剂再看 {top_catalyst}；关键不在谁更响，而在谁离下一季更近。"
            if top_risk and top_catalyst
            else "把风险和催化剂放在同一页，是为了判断下一季究竟更可能被什么主导。"
        )
    )
    meta["views"]["note"] = _short_section_text(
        (
            f"{leading_view} 的问题最接近市场焦点；真正有价值的是它和管理层口径有没有偏差。"
            if leading_view
            else "把机构页当成市场问题清单来用，比把它当结论页更有价值。"
        )
    )
    meta["capital_allocation"]["note"] = _short_section_text(
        (
            "先看利润有没有变成现金，再看资本开支与股东回报如何分配；这页主要回答“兑现质量”而不是“规模大小”。"
            if latest_kpis.get("operating_cash_flow_bn") is not None or latest_kpis.get("free_cash_flow_bn") is not None
            else "现金流口径不完整时，这页会保守回退到利润兑现与资本强度判断，不强行伪造现金流结论。"
        )
    )
    meta["expectation_reset"]["note"] = _short_section_text(
        "把上一季口径、本季实际和本次新指引放在一起看，判断是超预期、持平，还是市场预期被重新下修。"
    )
    meta["balance_quality"]["note"] = _short_section_text(
        (
            f"ROE {format_pct(latest.get('roe_pct'))}、{company.get('historical_profit_margin_label', '净利率')} {format_pct(latest.get('net_margin_pct'))} 与结构集中度要一起读。"
            if latest.get("roe_pct") is not None or latest.get("net_margin_pct") is not None
            else "这一页重点不在更多数字，而在判断增长是否真正沉淀成更好的经营质量。"
        )
    )
    meta["validation"]["note"] = _short_section_text(
        (
            f"下一季先盯 {top_catalyst or top_qna or '关键催化剂'} 与 {top_risk or second_qna or '主要风险'}；要看它们先落到哪一个指标上。"
            if top_catalyst or top_risk or top_qna
            else "报告读完后最重要的是下一季继续验证什么，这页就是把跟踪动作先列出来。"
        )
    )
    meta["growth"]["note"] = _short_section_text(
        (
            f"{start_end_text} 这一页先看总量，再看 {growth_driver or top_segment or '业务结构'} 是否成为主要驱动。"
            if start_end_text
            else "先看 12 季总收入斜率，再看结构分层是否同步发生变化。"
        )
    )
    meta["transition"]["note"] = _short_section_text(transition_focus or "这一页重点读结构迁移，而不是只读静态占比。")
    meta["profitability"]["note"] = _short_section_text(
        (
            f"别只看收入；毛利率 {format_pct(gross_margin_pct)} 与净利率 {format_pct(latest.get('net_margin_pct'))} 是否同向改善，决定增长质量。"
            if gross_margin_pct is not None and latest.get("net_margin_pct") is not None
            else "这一页真正想回答的是，收入增长有没有顺利兑现成更好的利润质量。"
        )
    )
    meta["contribution"]["note"] = _short_section_text(
        (
            f"若看增量来源，先盯 {growth_driver}；它决定了过去 12 季里最主要的增长斜率。"
            if growth_driver
            else "这一页的核心不是画更多条形图，而是回答增长究竟来自哪里。"
        )
    )
    meta["insights"]["note"] = _short_section_text(
        "这一页把前面分散的数据页收束成判断，重点看加速、集中与利润兑现能否同时成立。"
    )
    meta["evidence"]["note"] = _short_section_text(
        (
            f"回到 {first_source} 和电话会锚点继续核对；这份报告里的关键判断都应能追溯到原始材料。"
            if first_source
            else "最后一页不是装饰，而是用来把结论重新钉回原始材料。"
        )
    )
    return meta


REGIONAL_STRUCTURE_NAMES = {
    "north america",
    "international",
    "other international",
    "united states",
    "u s",
    "u.s.",
    "u.s",
    "united states and canada",
    "other countries",
    "americas",
    "americas excluding united states",
    "americas excluding u.s.",
    "americas excluding us",
    "other americas",
    "europe",
    "greater china",
    "japan",
    "rest of asia pacific",
    "rest of asia-pacific",
    "other asia pacific",
    "other asia-pacific",
    "rest of asia",
    "asia",
    "asia-pacific",
    "asia pacific",
    "apac",
    "apj",
    "emea",
    "europe, the middle east and africa",
    "europe middle east africa",
    "europe / middle east / africa",
    "middle east and africa",
    "latin america / caribbean",
    "latin america/caribbean",
    "south korea",
    "singapore",
    "taiwan",
    "china",
    "china including hong kong",
    "mainland china excluding hong kong",
    "hong kong",
    "non u.s.",
    "non-u.s.",
    "rest of world",
    "others",
    "other",
}

REGIONAL_STRUCTURE_TOKENS = (
    "america",
    "americas",
    "asia",
    "apac",
    "apj",
    "canada",
    "china",
    "country",
    "countries",
    "emea",
    "europe",
    "hong kong",
    "international",
    "japan",
    "korea",
    "latin",
    "middle east",
    "non u s",
    "other international",
    "other countries",
    "pacific",
    "singapore",
    "taiwan",
    "u s",
    "united states",
    "world",
)

AGGREGATE_GEOGRAPHY_NAMES = {
    "international",
    "other countries",
    "other international",
    "non u s",
    "non u s.",
    "non us",
    "non u s sales",
    "rest of world",
    "world outside united states",
    "outside united states",
    "outside u s",
    "outside us",
}

US_GEOGRAPHY_NAMES = {
    "united states",
    "u s",
    "u s.",
    "u.s.",
    "u.s",
    "us",
}


def _minimum_required_segment_count(company: dict[str, Any]) -> int:
    display_order = list(company.get("segment_order") or [])
    if len(display_order) >= 5:
        return 3
    if len(display_order) >= 3:
        return 2
    return 1


def _rescale_display_structure_items(
    items: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    if not items:
        return []
    scaled: list[dict[str, Any]] = []
    non_quarterly_only = all(
        str(item.get("scope") or "").casefold() in {"annual_filing", "regional_segment"}
        for item in items
    )
    total = sum(float(item.get("value_bn") or 0.0) for item in items if item.get("value_bn") is not None)
    should_rescale = (
        not non_quarterly_only
        and
        revenue_bn not in (None, 0)
        and total > 0
        and not 0.55 <= total / float(revenue_bn) <= 1.35
    )
    for item in items:
        value_bn = item.get("value_bn")
        if value_bn is None:
            continue
        value = float(value_bn)
        payload = dict(item)
        if should_rescale and revenue_bn not in (None, 0) and total > 0:
            value = float(revenue_bn) * value / total
        payload["value_bn"] = round(value, 3)
        yoy_pct = payload.get("yoy_pct")
        if yoy_pct is not None and abs(float(yoy_pct)) > 500:
            payload["yoy_pct"] = None
        scaled.append(payload)
    return scaled


def _segments_look_incomplete_for_company(
    company: dict[str, Any],
    segments: list[dict[str, Any]],
) -> bool:
    normalized = _normalize_segment_items(company, list(segments or []))
    if not normalized:
        return False
    if len(normalized) >= 2 and all(str(item.get("scope") or "").casefold() == "regional_segment" for item in normalized):
        return False
    required_count = _minimum_required_segment_count(company)
    if len(normalized) < required_count:
        return True
    names = {str(item.get("name") or "").casefold() for item in normalized}
    if names and names.issubset(REGIONAL_STRUCTURE_NAMES) and len(list(company.get("segment_order") or [])) >= 3:
        return True
    return False


def _structure_names_are_geography_like(
    company: dict[str, Any],
    names: list[str],
) -> bool:
    cleaned = [_segment_name_token(name) for name in names if _segment_name_token(name)]
    if not cleaned:
        return False
    canonical_tokens = {
        _segment_name_token(name)
        for name in list(company.get("segment_order") or [])
        if _segment_name_token(name)
    }
    overlap_count = sum(token in canonical_tokens for token in cleaned)
    if set(cleaned).issubset(REGIONAL_STRUCTURE_NAMES) and overlap_count < len(cleaned):
        return True
    geography_like_count = sum(
        any(marker in token for marker in REGIONAL_STRUCTURE_TOKENS)
        for token in cleaned
    )
    return geography_like_count == len(cleaned) and overlap_count < len(cleaned)


def _segments_are_geography_like(
    company: dict[str, Any],
    segments: list[dict[str, Any]],
) -> bool:
    normalized = _normalize_segment_items(company, list(segments or []))
    if not normalized:
        return False
    if len(normalized) >= 2 and all(str(item.get("scope") or "").casefold() == "regional_segment" for item in normalized):
        return False
    return _structure_names_are_geography_like(
        company,
        [str(item.get("name") or "") for item in normalized],
    )


def _items_match_scope(items: list[dict[str, Any]], scope: str) -> bool:
    normalized = [item for item in list(items or []) if isinstance(item, dict)]
    return bool(normalized) and all(str(item.get("scope") or "").casefold() == scope.casefold() for item in normalized)


def _segment_copy_profile(segments: list[dict[str, Any]]) -> dict[str, Any]:
    regional_mode = _items_match_scope(segments, "regional_segment")
    return {
        "regional_mode": regional_mode,
        "top_label": "头部分部" if regional_mode else "头部业务",
        "fast_label": "高增分部" if regional_mode else "高增业务",
        "focus_label": "重点分部" if regional_mode else "重点业务",
        "side_label": "分部侧" if regional_mode else "业务侧",
    }


def _mix_page_title(
    current_segments: list[dict[str, Any]],
    current_geographies: list[dict[str, Any]],
) -> str:
    segment_copy = _segment_copy_profile(current_segments)
    has_segments = bool(current_segments)
    has_geographies = bool(current_geographies)
    if segment_copy["regional_mode"] and has_geographies:
        return "当季经营分部与地区结构"
    if segment_copy["regional_mode"]:
        return "当季经营分部结构"
    if has_segments and has_geographies:
        return "当季业务与地区结构"
    if has_segments:
        return "当季业务结构与集中度"
    if has_geographies:
        return "当季地区结构与集中度"
    return "当季结构与集中度"


def _excerpt_text(text: Optional[str], limit: int) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    search_window = normalized[: limit + 1]
    preferred_cutoff = max(
        search_window.rfind(token)
        for token in ("。", "！", "？", "；", ".", "!", "?", ";", "，", ",", " ")
    )
    minimum_boundary = max(12, int(limit * 0.55))
    if preferred_cutoff >= minimum_boundary:
        if search_window[preferred_cutoff] == " ":
            return search_window[:preferred_cutoff].rstrip(" ,.;:，；")
        return search_window[: preferred_cutoff + 1].rstrip(" ,.;:，；")
    return normalized[:limit].rstrip(" ,.;:，；")


def _text_looks_human_readable(text: Optional[str], *, min_readable_terms: int = 4) -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) < 24:
        return False
    if any(ord(char) < 32 and char not in "\t\r\n" for char in normalized):
        return False
    if any(not char.isprintable() and not char.isspace() for char in normalized):
        return False
    readable_terms = re.findall(r"[A-Za-z\u4e00-\u9fff]{2,}", normalized)
    tokens = normalized.split()
    textual_chars = sum(char.isalpha() or ("\u4e00" <= char <= "\u9fff") for char in normalized)
    weird_tokens = [
        token
        for token in tokens
        if len(token) >= 12 and not re.search(r"[A-Za-z\u4e00-\u9fff]", token)
    ]
    if len(normalized) >= 80 and textual_chars / max(1, len(normalized)) < 0.28:
        return False
    if len(tokens) >= 6 and len(readable_terms) < min_readable_terms:
        return False
    if len(weird_tokens) >= max(2, len(tokens) // 3):
        return False
    return True


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _cache_metric_matches_history(
    cached_value: Optional[float],
    history_value: Optional[float],
    *,
    max_relative_delta: float,
    min_abs_delta: float,
) -> bool:
    if cached_value is None or history_value is None:
        return True
    cached = float(cached_value)
    baseline = float(history_value)
    if baseline == 0:
        return abs(cached - baseline) <= min_abs_delta
    abs_delta = abs(cached - baseline)
    relative_delta = abs_delta / max(abs(baseline), 1e-9)
    return relative_delta <= max_relative_delta or abs_delta <= min_abs_delta


def _cache_structures_match_history(
    items: list[dict[str, Any]],
    history_revenue_bn: Optional[float],
) -> bool:
    if not items or history_revenue_bn in (None, 0):
        return True
    if not _quarterly_structure_looks_reasonable(items, history_revenue_bn):
        return False
    max_item_value = max(float(item.get("value_bn") or 0.0) for item in items)
    return max_item_value <= float(history_revenue_bn) * 1.12


def _historical_official_cache_is_temporally_aligned(
    entry: dict[str, Any],
    cached_payload: dict[str, Any],
) -> bool:
    source_date = _parse_iso_date(cached_payload.get("source_date"))
    if source_date is None:
        return True
    period_end = _parse_iso_date(entry.get("period_end"))
    if period_end is None:
        return True
    delta_days = (source_date - period_end).days
    current_segments = [item for item in list(cached_payload.get("current_segments") or []) if isinstance(item, dict)]
    current_geographies = [item for item in list(cached_payload.get("current_geographies") or []) if isinstance(item, dict)]
    latest_kpis = dict(cached_payload.get("latest_kpis") or {})
    if (
        not latest_kpis
        and not current_segments
        and current_geographies
        and all(
            str(item.get("scope") or "").casefold() in {"annual_filing", "quarterly_mapped_from_official_geography"}
            for item in current_geographies
        )
    ):
        # Geography-only annual fallbacks are intentionally borrowed from the
        # nearest annual filing and stay useful longer than quarter-exact KPI caches.
        return -400 <= delta_days <= 820
    if not (-30 <= delta_days <= 220):
        return False

    if not _cache_metric_matches_history(
        latest_kpis.get("revenue_bn"),
        entry.get("revenue_bn"),
        max_relative_delta=0.3,
        min_abs_delta=0.75,
    ):
        return False
    if not _cache_metric_matches_history(
        latest_kpis.get("net_income_bn"),
        entry.get("net_income_bn"),
        max_relative_delta=0.45,
        min_abs_delta=0.35,
    ):
        return False

    non_annual_geographies = [
        item
        for item in current_geographies
        if str(item.get("scope") or "").casefold() not in {"annual_filing", "quarterly_mapped_from_official_geography"}
    ]
    history_revenue_bn = entry.get("revenue_bn")
    if not _cache_structures_match_history(current_segments, history_revenue_bn):
        return False
    if not _cache_structures_match_history(non_annual_geographies, history_revenue_bn):
        return False
    return True


def _historical_official_guidance_cache_is_temporally_aligned(
    entry: dict[str, Any],
    cached_payload: dict[str, Any],
) -> bool:
    source_date = _parse_iso_date(cached_payload.get("source_date"))
    if source_date is None:
        return True
    period_end = _parse_iso_date(entry.get("period_end"))
    if period_end is None:
        return True
    delta_days = (source_date - period_end).days
    if not (-30 <= delta_days <= 220):
        return False
    latest_kpis = dict(cached_payload.get("latest_kpis") or {})
    if not _cache_metric_matches_history(
        latest_kpis.get("revenue_bn"),
        entry.get("revenue_bn"),
        max_relative_delta=0.3,
        min_abs_delta=0.75,
    ):
        return False
    if not _cache_metric_matches_history(
        latest_kpis.get("net_income_bn"),
        entry.get("net_income_bn"),
        max_relative_delta=0.45,
        min_abs_delta=0.35,
    ):
        return False
    return True


def _salvage_stale_historical_cache_structures(
    company: dict[str, Any],
    entry: dict[str, Any],
    cached_payload: dict[str, Any],
) -> Optional[dict[str, Any]]:
    if entry.get("revenue_bn") in (None, 0) or entry.get("net_income_bn") is None:
        return None
    if entry.get("segments") and entry.get("geographies"):
        return None
    current_segments = [dict(item) for item in list(cached_payload.get("current_segments") or []) if isinstance(item, dict)]
    current_geographies = [dict(item) for item in list(cached_payload.get("current_geographies") or []) if isinstance(item, dict)]
    if not current_segments and not current_geographies:
        return None
    trusted_revenue = entry.get("revenue_bn")
    prepared_geographies, _mapped_from_official = _prepare_quarterly_geographies(current_geographies, trusted_revenue)
    has_segments = bool(_normalize_historical_segments(company, current_segments, trusted_revenue))
    has_geographies = bool(_normalize_historical_geographies(prepared_geographies, trusted_revenue))
    if not has_segments and not has_geographies:
        return None
    return {
        "latest_kpis": {},
        "current_segments": current_segments,
        "current_geographies": prepared_geographies,
        "source_url": "",
        "source_date": "",
        "profit_basis": cached_payload.get("profit_basis"),
        "structure_only_from_stale_cache": True,
    }


def _prepare_call_quote_cards(cards: list[dict[str, Any]]) -> list[dict[str, str]]:
    def has_cjk(text: str) -> bool:
        return bool(re.search(r"[\u4e00-\u9fff]", text))

    def looks_english(text: str) -> bool:
        stripped = str(text or "").strip()
        if not stripped:
            return False
        latin_count = sum(char.isascii() and char.isalpha() for char in stripped)
        return latin_count >= 6 and not has_cjk(stripped)

    def localize_speaker(value: str) -> str:
        normalized = re.sub(r"\s+", " ", str(value or "")).strip()
        lowered = normalized.casefold()
        if not normalized:
            return "管理层"
        if has_cjk(normalized):
            return normalized
        if lowered in {"management", "management context"}:
            return "管理层"
        if lowered == "guidance context":
            return "指引语境"
        if re.fullmatch(r"[A-Za-z][A-Za-z .,'-]{1,80}", normalized):
            return normalized
        return "管理层原话"

    def localize_source_label(value: str) -> str:
        normalized = re.sub(r"\s+", " ", str(value or "")).strip()
        lowered = normalized.casefold()
        if not normalized:
            return "官方材料"
        if has_cjk(normalized):
            return normalized
        if "earnings release" in lowered or "quarterly results" in lowered:
            return "财报新闻稿"
        if "shareholder deck" in lowered or "update deck" in lowered or "presentation" in lowered:
            return "演示材料"
        if "transcript" in lowered or "call summary" in lowered or "earnings call" in lowered:
            return "电话会材料"
        if any(token in lowered for token in ("form 10-q", "form 10-k", "8-k", "6-k", "20-f", "sec filing")):
            return "SEC 文件"
        if "official materials" in lowered:
            return "官方材料"
        if "system coverage note" in lowered:
            return "系统覆盖说明"
        return "官方材料"

    prepared: list[dict[str, str]] = []
    for index, item in enumerate(cards[:3]):
        speaker = re.sub(r"\s+", " ", str(item.get("speaker") or "Management")).strip()
        quote = re.sub(r"\s+", " ", str(item.get("quote") or "")).strip()
        analysis = re.sub(r"\s+", " ", str(item.get("analysis") or "")).strip()
        source_label = re.sub(r"\s+", " ", str(item.get("source_label") or "")).strip()
        if looks_english(quote) and has_cjk(analysis):
            display_quote = analysis
            display_detail = ""
        else:
            display_quote = quote or analysis
            display_detail = analysis if analysis and analysis != display_quote else ""
        prepared.append(
            {
                "speaker": localize_speaker(speaker),
                "quote": display_quote,
                "analysis": display_detail,
                "source_label": localize_source_label(source_label),
            }
        )
    return prepared


def _extract_summary_driver(
    company: dict[str, Any],
    fiscal_label: str,
    text: Optional[str],
) -> Optional[str]:
    cleaned = _clean_summary_fragment(text)
    if not cleaned:
        return None
    markers = [
        f"{company['english_name']} {fiscal_label}",
        f"{company['english_name']}{fiscal_label}",
        f"{company['name']} {fiscal_label}",
        company["english_name"],
        company["name"],
    ]
    for marker in markers:
        if marker and marker in cleaned:
            left, right = cleaned.split(marker, 1)
            left = left.strip(" ，,；;。")
            right = right.strip(" ，,；;。")
            if left and len(left) >= 6:
                cleaned = left
                break
            if right and not any(keyword in right for keyword in ("收入", "净利润", "经营利润", "同比", "EPS", "现金流")):
                cleaned = right
                break
    if any(keyword in cleaned for keyword in ("收入", "净利润", "经营利润", "同比", "EPS")) and len(cleaned) < 18:
        return None
    return cleaned


def _summary_driver_from_fixture(
    company: dict[str, Any],
    fiscal_label: str,
    fixture: Optional[dict[str, Any]],
) -> Optional[str]:
    if not fixture:
        return _clean_summary_fragment(str(company.get("card_headline") or ""))
    candidates = [company.get("card_headline")] + list(fixture.get("takeaways") or [])[1:3] + [fixture.get("headline")]
    seen: set[str] = set()
    for candidate in candidates:
        driver = _extract_summary_driver(company, fiscal_label, candidate)
        if not driver or driver in seen:
            continue
        if any(keyword in driver for keyword in ("收入", "净利润", "经营利润", "同比", "环比", "EPS")):
            continue
        seen.add(driver)
        return driver
    return None


def _compose_summary_headline(
    company: dict[str, Any],
    fiscal_label: str,
    latest_kpis: dict[str, Any],
    latest_history: Optional[dict[str, Any]],
    fixture: Optional[dict[str, Any]],
) -> str:
    return _writer_compose_summary_headline(company, fiscal_label, latest_kpis, latest_history, fixture)


def _build_layered_takeaways(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
    merged_sources: list[dict[str, Any]],
) -> list[dict[str, str]]:
    takeaways = _writer_compose_layered_takeaways(company, fixture, history, money_symbol)
    for item in takeaways:
        source_anchor, verification = _layered_takeaway_anchor(
            str(item.get("title") or ""),
            merged_sources=merged_sources,
            guidance=fixture["guidance"],
        )
        item["source_anchor"] = source_anchor
        item["verification"] = verification
    return takeaways


def _build_expectation_panel(
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> dict[str, Any]:
    return _writer_build_expectation_panel(fixture, history, money_symbol)


def _build_institutional_digest(institutional_views: list[dict[str, Any]]) -> dict[str, Any]:
    return _writer_build_institutional_digest(institutional_views)


def _normalize_takeaways(
    takeaways: list[str],
    latest_kpis: dict[str, Any],
    latest_history: dict[str, Any],
    money_symbol: str,
) -> list[str]:
    normalized = _writer_normalize_takeaways(takeaways, latest_kpis, latest_history, money_symbol)
    profit_line = _writer_polish_generated_text(_build_profit_growth_takeaway(latest_kpis, latest_history, money_symbol))
    if profit_line and not any("净利润" in item or "利润" in item for item in normalized[:2]):
        normalized.insert(1 if normalized else 0, profit_line)
    return normalized[:4]


def _statement_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        value = item.get("value_bn")
        if value is None:
            continue
        normalized_item = {
            "name": item["name"],
            "value_bn": float(value),
        }
        for key in (
            "yoy_pct",
            "margin_pct",
            "note",
            "logo_key",
            "display_name",
            "operating_income_bn",
            "share_pct",
            "chip_labels",
        ):
            if item.get(key) is not None:
                normalized_item[key] = item.get(key)
        normalized.append(normalized_item)
    return normalized


def _build_income_statement_snapshot(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    latest = history[-1]
    latest_kpis = fixture["latest_kpis"]
    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    gross_margin_pct = _value_or_history(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct"))
    operating_profit_bn = _value_or_history(latest_kpis.get("operating_income_bn"), latest.get("net_income_bn"))
    net_profit_bn = _value_or_history(latest_kpis.get("net_income_bn"), latest.get("net_income_bn"))
    if revenue_bn in (None, 0) or gross_margin_pct is None or operating_profit_bn is None or net_profit_bn is None:
        return {}
    gross_profit_bn = revenue_bn * gross_margin_pct / 100
    cost_of_revenue_bn = max(revenue_bn - gross_profit_bn, 0.0)
    operating_expenses_bn = max(gross_profit_bn - operating_profit_bn, 0.0)
    current_segments = _statement_items(fixture.get("current_segments") or [])
    statement_overrides = fixture.get("income_statement") or {}
    official_sources = _statement_items(statement_overrides.get("sources") or [])
    detailed_sources = current_segments
    sources = detailed_sources if len(detailed_sources) >= 3 else official_sources or detailed_sources
    if not sources:
        sources = official_sources
    business_groups = _statement_items(statement_overrides.get("business_groups") or detailed_sources or official_sources or sources)
    if not sources:
        sources = [{"name": company["name"], "value_bn": revenue_bn}]
    if not business_groups:
        business_groups = _statement_items(sources)
    statement = {
        "title": f"{company['english_name']} {fixture['fiscal_label']} Income Statement",
        "subtitle": "将业务收入、成本、经营费用与净利润放到一条阅读路径中。",
        "company_id": company["id"],
        "company_name": company["english_name"],
        "calendar_quarter": fixture.get("calendar_quarter") or latest.get("quarter_label"),
        "fiscal_label": fixture["fiscal_label"],
        "period_end": fixture["period_end"],
        "sources": sources,
        "official_sources": official_sources,
        "business_groups": business_groups,
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": latest_kpis.get("revenue_yoy_pct"),
        "gross_profit_bn": gross_profit_bn,
        "gross_margin_pct": gross_margin_pct,
        "cost_of_revenue_bn": cost_of_revenue_bn,
        "operating_profit_bn": operating_profit_bn,
        "operating_margin_pct": _safe_ratio(operating_profit_bn, revenue_bn),
        "operating_expenses_bn": operating_expenses_bn,
        "net_profit_bn": net_profit_bn,
        "net_margin_pct": _safe_ratio(net_profit_bn, revenue_bn),
    }
    if statement_overrides:
        merged = dict(statement)
        merged.update(statement_overrides)
        merged["sources"] = sources
        merged["official_sources"] = official_sources
        merged["business_groups"] = business_groups
        return merged
    residual = max(operating_profit_bn - net_profit_bn, 0.0)
    statement["below_operating_items"] = [
        {
            "name": "Tax & Other",
            "value_bn": residual,
            "color": "#D92D20",
        }
    ]
    return statement


def build_historical_quarter_cube(
    company_id: str,
    calendar_quarter: str,
    window: int = 12,
    periods: Optional[list[str]] = None,
    series: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    company = get_company(company_id)
    periods = periods or get_company_series(company_id)[0]
    series = series or get_company_series(company_id)[1]
    if calendar_quarter not in periods and not company.get("official_source"):
        raise KeyError(f"Quarter {calendar_quarter} not found for {company_id}")
    sparse_window = False
    selected_periods: list[str]
    if calendar_quarter in periods:
        end_index = periods.index(calendar_quarter)
        if end_index < window - 1 and not company.get("official_source"):
            earliest = periods[0] if periods else calendar_quarter
            raise ValueError(
                f"Quarter {calendar_quarter} does not have a full {window}-quarter history window. "
                f"Earliest available quarter is {earliest}."
            )
        selected_periods = periods[max(0, end_index - window + 1) : end_index + 1]
        if len(selected_periods) >= 2:
            actual_span = (
                (_quarter_parts(selected_periods[-1])[0] * 4 + _quarter_parts(selected_periods[-1])[1])
                - (_quarter_parts(selected_periods[0])[0] * 4 + _quarter_parts(selected_periods[0])[1])
            )
            sparse_window = actual_span > (len(selected_periods) - 1)
            if not _periods_are_consecutive_quarters(selected_periods):
                sparse_window = True
    else:
        selected_periods = []
        sparse_window = True

    if sparse_window and not company.get("official_source"):
        earliest = periods[0] if periods else calendar_quarter
        raise ValueError(
            f"Quarter {calendar_quarter} does not have a contiguous full {window}-quarter history window. "
            f"Earliest available quarter is {earliest}."
        )

    if company.get("official_source") and (calendar_quarter not in periods or sparse_window):
        selected_periods = _quarter_window(calendar_quarter, window)
        earliest = periods[0] if periods else selected_periods[0]
        earliest_index = _quarter_parts(earliest)[0] * 4 + (_quarter_parts(earliest)[1] - 1)
        selected_start_index = _quarter_parts(selected_periods[0])[0] * 4 + (_quarter_parts(selected_periods[0])[1] - 1)
        if selected_start_index < earliest_index:
            raise ValueError(
                f"Quarter {calendar_quarter} does not have a full {window}-quarter history window. "
                f"Earliest available quarter is {earliest}."
            )

    segment_history = get_segment_history(company_id)
    fixture = get_quarter_fixture(company_id, calendar_quarter)

    entries: list[dict[str, Any]] = []
    for period in selected_periods:
        revenue = series["revenue"].get(period)
        earnings = series["earnings"].get(period)
        gross_margin = series["grossMargin"].get(period)
        revenue_yoy = series["revenueGrowth"].get(period)
        roe = series["roe"].get(period)
        equity = series.get("equity", {}).get(period)
        period_end = _resolved_period_end(
            period,
            series_period_end=_series_period_end(series, period),
        )
        release_date = period_end
        fiscal_label = _resolved_fiscal_label(company, period, period_end)
        segments_payload: Optional[list[dict[str, Any]]] = None
        source_type = "structured_financial_series"
        source_url = ""
        if period in segment_history:
            history_entry = segment_history[period]
            segments_payload = history_entry["segments"]
            source_type = history_entry["source_type"]
            source_url = history_entry["source_url"]

        if period == calendar_quarter and fixture:
            period_end = fixture["period_end"]
            release_date = fixture["release_date"]
            fiscal_label = _resolved_fiscal_label(
                company,
                period,
                period_end,
                explicit_label=str(fixture.get("fiscal_label") or "") or None,
            )
            revenue = fixture["latest_kpis"].get("revenue_bn")
            if revenue is not None:
                revenue = float(revenue) * 1_000_000_000
            profit_bn = _historical_profit_series_value(company, fixture["latest_kpis"])
            if profit_bn is not None:
                earnings = float(profit_bn) * 1_000_000_000
            gross_margin = fixture["latest_kpis"].get("gaap_gross_margin_pct")
            revenue_yoy = fixture["latest_kpis"].get("revenue_yoy_pct")
            source_type = "official_release"
            source_url = fixture["sources"][0]["url"] if fixture.get("sources") else ""

        entries.append(
            {
                "quarter_label": period,
                "calendar_quarter": period,
                "fiscal_label": fiscal_label,
                "period_end": period_end,
                "release_date": release_date,
                "revenue_bn": float(revenue) / 1_000_000_000 if revenue is not None else None,
                "net_income_bn": float(earnings) / 1_000_000_000 if earnings is not None else None,
                "equity_bn": float(equity) / 1_000_000_000 if equity is not None else None,
                "gross_margin_pct": float(gross_margin) if gross_margin is not None else None,
                "revenue_yoy_pct": float(revenue_yoy) if revenue_yoy is not None else None,
                "roe_pct": float(roe) if roe is not None else None,
                "net_margin_pct": _safe_ratio(float(earnings), float(revenue)) if revenue and earnings is not None else None,
                "segments": segments_payload,
                "geographies": None,
                "structure_basis": "segment" if segments_payload else None,
                "source_type": source_type,
                "source_url": source_url,
            }
        )

    for index, entry in enumerate(entries):
        previous = entries[index - 1] if index > 0 else None
        year_ago = entries[index - 4] if index > 3 else None
        if previous and entry["revenue_bn"] and previous["revenue_bn"]:
            entry["revenue_qoq_pct"] = (float(entry["revenue_bn"]) / float(previous["revenue_bn"]) - 1) * 100
        else:
            entry["revenue_qoq_pct"] = None
        if previous and entry["net_income_bn"] is not None and previous["net_income_bn"] not in (None, 0):
            entry["net_income_qoq_pct"] = (float(entry["net_income_bn"]) / float(previous["net_income_bn"]) - 1) * 100
        else:
            entry["net_income_qoq_pct"] = None
        if year_ago and entry["net_income_bn"] is not None and year_ago["net_income_bn"] not in (None, 0):
            entry["net_income_yoy_pct"] = (float(entry["net_income_bn"]) / float(year_ago["net_income_bn"]) - 1) * 100
        else:
            entry["net_income_yoy_pct"] = None
        entry["ttm_revenue_bn"] = _ttm_sum(entries, "revenue_bn", index)
        previous_ttm = _ttm_sum(entries, "revenue_bn", index - 4) if index >= 7 else None
        if entry["ttm_revenue_bn"] and previous_ttm:
            entry["ttm_revenue_growth_pct"] = (float(entry["ttm_revenue_bn"]) / float(previous_ttm) - 1) * 100
        else:
            entry["ttm_revenue_growth_pct"] = None
        recomputed_roe_pct = _ttm_roe_pct(entries, index)
        if recomputed_roe_pct is not None:
            entry["roe_pct"] = recomputed_roe_pct
    return _sanitize_history_quality_metrics(entries)


def _normalize_historical_segments(
    company: dict[str, Any],
    segments: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    canonical_segments = _normalize_segment_items(
        company,
        segments,
        allow_rollup_aliases=False,
    )
    total = sum(float(item.get("value_bn") or 0.0) for item in canonical_segments)
    if total <= 0:
        return normalized
    denominator = revenue_bn if revenue_bn not in (None, 0) and 0.55 <= total / float(revenue_bn) <= 1.35 else total
    for item in canonical_segments:
        value_bn = item.get("value_bn")
        if value_bn is None:
            continue
        value = float(value_bn)
        if value <= 0:
            continue
        payload = {
            "name": str(item.get("name") or "Business"),
            "value_bn": value,
            "share_pct": value / float(denominator) * 100 if denominator else 0.0,
        }
        if item.get("yoy_pct") is not None:
            payload["yoy_pct"] = float(item["yoy_pct"])
        if item.get("scope") is not None:
            payload["scope"] = item["scope"]
        normalized.append(payload)
    return normalized


def _is_aggregate_geography_name(name: str) -> bool:
    token = _segment_name_token(name)
    if token in AGGREGATE_GEOGRAPHY_NAMES:
        return True
    return token.startswith("international ") and "excluding" not in token


def _is_us_geography_name(name: str) -> bool:
    return _segment_name_token(name) in US_GEOGRAPHY_NAMES


def _is_granular_non_us_geography_name(name: str) -> bool:
    token = _segment_name_token(name)
    if not token or _is_aggregate_geography_name(token) or _is_us_geography_name(token):
        return False
    if token in REGIONAL_STRUCTURE_NAMES:
        return True
    return any(marker in token for marker in REGIONAL_STRUCTURE_TOKENS)


def _drop_mixed_hierarchy_geography_rollups(
    geographies: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> tuple[list[dict[str, Any]], bool]:
    candidates = [dict(item) for item in geographies if isinstance(item, dict)]
    if len(candidates) < 4:
        return candidates, False
    aggregate_indexes = [
        index
        for index, item in enumerate(candidates)
        if _is_aggregate_geography_name(str(item.get("name") or ""))
    ]
    granular_non_us_count = sum(
        1
        for item in candidates
        if _is_granular_non_us_geography_name(str(item.get("name") or ""))
    )
    has_us = any(_is_us_geography_name(str(item.get("name") or "")) for item in candidates)
    if not aggregate_indexes or granular_non_us_count < 2 or not has_us:
        return candidates, False

    pruned = [dict(item) for index, item in enumerate(candidates) if index not in set(aggregate_indexes)]
    pruned_total = sum(float(item.get("value_bn") or 0.0) for item in pruned if item.get("value_bn") is not None)
    original_total = sum(float(item.get("value_bn") or 0.0) for item in candidates if item.get("value_bn") is not None)
    if pruned_total <= 0 or original_total <= 0:
        return candidates, False
    if revenue_bn in (None, 0):
        return candidates, False

    revenue = float(revenue_bn)
    original_gap = abs(original_total - revenue)
    pruned_gap = abs(pruned_total - revenue)
    tolerance = max(0.05, revenue * 0.015)
    if pruned_gap <= tolerance and pruned_gap + tolerance < original_gap:
        return pruned, True
    return candidates, False


def _map_geographies_to_quarter_revenue(
    geographies: list[dict[str, Any]],
    revenue_bn: float,
) -> list[dict[str, Any]]:
    if revenue_bn <= 0:
        return [dict(item) for item in geographies if isinstance(item, dict)]
    source_items = [
        dict(item)
        for item in geographies
        if isinstance(item, dict) and float(item.get("value_bn") or 0.0) > 0
    ]
    total = sum(float(item.get("value_bn") or 0.0) for item in source_items)
    if total <= 0:
        return source_items

    mapped: list[dict[str, Any]] = []
    running_total = 0.0
    last_index = len(source_items) - 1
    for index, item in enumerate(source_items):
        value = max(float(item.get("value_bn") or 0.0), 0.0)
        share = value / total if total else 0.0
        if index == last_index:
            mapped_value = round(max(float(revenue_bn) - running_total, 0.0), 3)
        else:
            mapped_value = round(float(revenue_bn) * share, 3)
            running_total += mapped_value
        entry = dict(item)
        entry["value_bn"] = mapped_value
        entry["share_pct"] = share * 100
        entry["scope"] = "quarterly_mapped_from_official_geography"
        mapped.append(entry)
    return mapped


def _prepare_quarterly_geographies(
    geographies: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> tuple[list[dict[str, Any]], bool]:
    prepared = [
        dict(item)
        for item in list(geographies or [])
        if isinstance(item, dict) and item.get("value_bn") is not None and float(item.get("value_bn") or 0.0) > 0
    ]
    if not prepared:
        return [], False

    prepared, removed_rollups = _drop_mixed_hierarchy_geography_rollups(prepared, revenue_bn)
    total = sum(float(item.get("value_bn") or 0.0) for item in prepared)
    if total <= 0 or revenue_bn in (None, 0):
        return prepared, False

    revenue = float(revenue_bn)
    ratio = total / revenue if revenue else 0.0
    has_annual_scope = any(str(item.get("scope") or "").casefold() == "annual_filing" for item in prepared)
    should_map = has_annual_scope or removed_rollups or ratio > 1.35
    if should_map:
        return _map_geographies_to_quarter_revenue(prepared, revenue), True
    return prepared, False


def _normalize_historical_geographies(
    geographies: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    prepared_geographies, _mapped_from_official = _prepare_quarterly_geographies(
        geographies,
        revenue_bn,
    )
    normalized: list[dict[str, Any]] = []
    total = sum(float(item.get("value_bn") or 0.0) for item in prepared_geographies if item.get("value_bn") is not None)
    if total <= 0:
        return normalized
    denominator = revenue_bn if revenue_bn not in (None, 0) and 0.55 <= total / float(revenue_bn) <= 1.35 else total
    for item in prepared_geographies:
        value_bn = item.get("value_bn")
        if value_bn is None:
            continue
        value = float(value_bn)
        if value <= 0:
            continue
        payload = {
            "name": str(item.get("name") or "Geography"),
            "value_bn": value,
            "share_pct": value / float(denominator) * 100 if denominator else 0.0,
        }
        if item.get("yoy_pct") is not None:
            payload["yoy_pct"] = float(item["yoy_pct"])
        if item.get("scope") is not None:
            payload["scope"] = item["scope"]
        normalized.append(payload)
    return normalized


def _interpolate_historical_metric(
    history: list[dict[str, Any]],
    index: int,
    key: str,
) -> Optional[float]:
    prev_index = next((cursor for cursor in range(index - 1, -1, -1) if history[cursor].get(key) is not None), None)
    next_index = next((cursor for cursor in range(index + 1, len(history)) if history[cursor].get(key) is not None), None)
    if prev_index is not None and next_index is not None:
        prev_value = float(history[prev_index][key])
        next_value = float(history[next_index][key])
        span = next_index - prev_index
        if span <= 0:
            return prev_value
        weight = (index - prev_index) / span
        return prev_value * (1 - weight) + next_value * weight
    if prev_index is not None and index - prev_index == 1:
        return float(history[prev_index][key])
    if next_index is not None and next_index - index == 1:
        return float(history[next_index][key])
    return None


def _backfill_historical_core_metrics(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = [dict(entry) for entry in history]
    for index, entry in enumerate(enriched):
        inferred = False
        for key in ("revenue_bn", "net_income_bn", "gross_margin_pct", "equity_bn"):
            if entry.get(key) is not None:
                continue
            interpolated = _interpolate_historical_metric(enriched, index, key)
            if interpolated is None:
                continue
            entry[key] = interpolated
            inferred = True
        if entry.get("revenue_bn") not in (None, 0) and entry.get("net_income_bn") is not None:
            entry["net_margin_pct"] = _safe_ratio(float(entry["net_income_bn"]), float(entry["revenue_bn"]))
        if inferred:
            entry["core_metrics_inferred"] = True
        enriched[index] = entry
    return enriched


def _historical_segment_reference_profile(
    company: dict[str, Any],
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    expected_names = [
        str(item.get("name") or "")
        for item in _normalize_segment_items(
            company,
            [{"name": name} for name in list(company.get("segment_order") or [])],
        )
        if str(item.get("name") or "")
    ]
    observed_lists: list[list[str]] = []
    regional_observed_lists: list[list[str]] = []
    frequency: Counter[str] = Counter()
    regional_frequency: Counter[str] = Counter()
    for entry in history:
        normalized = _normalize_historical_segments(company, list(entry.get("segments") or []), entry.get("revenue_bn"))
        names = [str(item.get("name") or "") for item in normalized if str(item.get("name") or "")]
        if not names:
            continue
        lowered = {name.casefold() for name in names}
        if len(normalized) >= 2 and all(str(item.get("scope") or "").casefold() == "regional_segment" for item in normalized):
            regional_observed_lists.append(names)
            regional_frequency.update(dict.fromkeys(names, 1))
            continue
        if lowered and lowered.issubset(REGIONAL_STRUCTURE_NAMES):
            continue
        observed_lists.append(names)
        frequency.update(dict.fromkeys(names, 1))

    if regional_observed_lists:
        reference_names: list[str] = []
        observed_ranked = sorted(regional_frequency.items(), key=lambda item: (-item[1], item[0]))
        for name, _count in observed_ranked:
            if name not in reference_names:
                reference_names.append(name)
        observed_counts = sorted(len(names) for names in regional_observed_lists if names)
        median_count = observed_counts[len(observed_counts) // 2] if observed_counts else len(reference_names)
        target_count = max(2, min(len(reference_names) or median_count or 2, median_count or 2))
        core_names = reference_names[: min(2, len(reference_names))]
        return {
            "reference_names": reference_names,
            "target_count": target_count,
            "core_names": core_names,
        }

    reference_names = list(expected_names)
    observed_ranked = sorted(
        frequency.items(),
        key=lambda item: (-item[1], reference_names.index(item[0]) if item[0] in reference_names else len(reference_names)),
    )
    for name, _count in observed_ranked:
        if name not in reference_names:
            reference_names.append(name)

    observed_counts = sorted(len(names) for names in observed_lists if names)
    median_count = observed_counts[len(observed_counts) // 2] if observed_counts else len(reference_names)
    target_count = max(
        _minimum_required_segment_count(company),
        min(
            len(reference_names) or median_count or 1,
            max(2, round(max(median_count or 0, _minimum_required_segment_count(company)) * 0.8)),
        ),
    )
    latest_names = next((list(names) for names in reversed(observed_lists) if names), [])
    latest_name_set = {str(name) for name in latest_names if str(name)}
    latest_like_count = 0
    if latest_name_set:
        required_overlap = max(2, len(latest_name_set) - 1)
        latest_like_count = sum(
            1
            for names in observed_lists
            if len(latest_name_set.intersection({str(name) for name in names if str(name)})) >= required_overlap
        )
    if latest_names and latest_like_count >= 2 and len(latest_names) > target_count:
        reference_names = list(latest_names) + [name for name in reference_names if name not in latest_name_set]
        target_count = len(latest_names)
    stable_threshold = max(2, round(len(observed_lists) * 0.4)) if observed_lists else 0
    core_names = [
        name
        for name in reference_names
        if frequency.get(name, 0) >= stable_threshold
    ][:2]
    if latest_names and latest_like_count >= 2 and len(latest_names) >= 2:
        core_names = latest_names[:2]
    if not core_names:
        core_names = reference_names[: min(2, len(reference_names))]
    return {
        "reference_names": reference_names,
        "target_count": target_count,
        "core_names": core_names,
    }


def _valid_segment_history_entry(
    company: dict[str, Any],
    entry: dict[str, Any],
    reference_profile: Optional[dict[str, Any]] = None,
) -> bool:
    normalized = _normalize_historical_segments(company, list(entry.get("segments") or []), entry.get("revenue_bn"))
    if not normalized:
        return False
    if _segments_are_geography_like(company, normalized):
        return False
    reference_profile = reference_profile or _historical_segment_reference_profile(company, [entry])
    target_count = int(reference_profile.get("target_count") or _minimum_required_segment_count(company))
    if len(normalized) < target_count:
        return False
    reference_names = set(str(name) for name in list(reference_profile.get("reference_names") or []))
    if reference_names and len([item for item in normalized if str(item.get("name") or "") in reference_names]) < target_count:
        return False
    core_names = set(str(name) for name in list(reference_profile.get("core_names") or []))
    required_core_count = min(len(core_names), 2 if target_count >= 4 else 1)
    if core_names and len([item for item in normalized if str(item.get("name") or "") in core_names]) < required_core_count:
        return False
    return True


def _harmonize_historical_structures(
    company: dict[str, Any],
    history: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    harmonized = [dict(entry) for entry in history]
    reference_profile = _historical_segment_reference_profile(company, harmonized)
    segment_indexes = [
        index
        for index, entry in enumerate(harmonized)
        if _valid_segment_history_entry(company, entry, reference_profile)
    ]
    geography_indexes = [index for index, entry in enumerate(harmonized) if _normalize_historical_geographies(list(entry.get("geographies") or []), entry.get("revenue_bn"))]
    target_basis: Optional[str] = None
    if segment_indexes:
        target_basis = "segment"
    elif geography_indexes:
        target_basis = "geography"

    for index, entry in enumerate(harmonized):
        geographies = _normalize_historical_geographies(list(entry.get("geographies") or []), entry.get("revenue_bn"))
        if geographies:
            entry["geographies"] = geographies
        if target_basis == "segment":
            if _valid_segment_history_entry(company, entry, reference_profile):
                entry["segments"] = _normalize_historical_segments(company, list(entry.get("segments") or []), entry.get("revenue_bn"))
                entry["structure_basis"] = "segment"
            else:
                entry["segments"] = []
                if geographies:
                    entry["geographies"] = geographies
                entry["structure_basis"] = None
        elif target_basis == "geography" and geographies:
            entry["segments"] = []
            entry["structure_basis"] = "geography"
        harmonized[index] = entry
    return harmonized


def _historical_segment_share_map(
    company: dict[str, Any],
    entry: dict[str, Any],
) -> dict[str, float]:
    segments = list(entry.get("segments") or [])
    if not segments:
        return {}
    normalized = _normalize_historical_segments(company, segments, entry.get("revenue_bn"))
    if _segments_are_geography_like(company, normalized):
        return {}
    share_map = {
        str(item.get("name") or "Business"): float(item.get("share_pct") or 0.0) / 100
        for item in normalized
        if float(item.get("share_pct") or 0.0) > 0
    }
    total = sum(share_map.values())
    if total <= 0:
        return {}
    return {name: value / total for name, value in share_map.items() if value > 0}


def _is_complete_segment_snapshot(
    company: dict[str, Any],
    entry: dict[str, Any],
    reference_profile: Optional[dict[str, Any]] = None,
) -> bool:
    if _segments_are_geography_like(company, list(entry.get("segments") or [])):
        return False
    share_map = _historical_segment_share_map(company, entry)
    if not share_map:
        return False
    reference_profile = reference_profile or _historical_segment_reference_profile(company, [entry])
    reference_names = set(str(name) for name in list(reference_profile.get("reference_names") or []))
    target_count = int(reference_profile.get("target_count") or _minimum_required_segment_count(company))
    overlap_count = len([name for name in share_map if not reference_names or name in reference_names])
    if overlap_count < target_count:
        return False
    core_names = set(str(name) for name in list(reference_profile.get("core_names") or []))
    required_core_count = min(len(core_names), 2 if target_count >= 4 else 1)
    if core_names and len([name for name in share_map if name in core_names]) < required_core_count:
        return False
    total = sum(share_map.values())
    return 0.92 <= total <= 1.08


def _backfill_historical_segment_history(
    company: dict[str, Any],
    history: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched = [dict(entry) for entry in history]
    reference_profile = _historical_segment_reference_profile(company, enriched)
    expected_names = [str(name) for name in list(company.get("segment_order") or [])]
    for entry in enriched:
        for segment in list(entry.get("segments") or []):
            name = str(segment.get("name") or "")
            if name and name not in expected_names:
                expected_names.append(name)
    valid_indexes = [
        index
        for index, entry in enumerate(enriched)
        if _is_complete_segment_snapshot(company, entry, reference_profile)
    ]
    if len(valid_indexes) < 2 or not expected_names:
        return enriched

    anchor_maps = {index: _historical_segment_share_map(company, enriched[index]) for index in valid_indexes}
    for index, entry in enumerate(enriched):
        if index in valid_indexes:
            entry["segments_inferred"] = bool(entry.get("segments_inferred"))
            enriched[index] = entry
            continue

        prev_index = next((value for value in reversed(valid_indexes) if value < index), None)
        next_index = next((value for value in valid_indexes if value > index), None)
        if prev_index is None or next_index is None:
            continue

        candidate_map: dict[str, float] = {}
        span = max(1, next_index - prev_index)
        weight = (index - prev_index) / span
        prev_map = anchor_maps[prev_index]
        next_map = anchor_maps[next_index]
        for name in expected_names:
            candidate_map[name] = prev_map.get(name, 0.0) * (1 - weight) + next_map.get(name, 0.0) * weight

        total = sum(candidate_map.values())
        revenue_bn = float(entry.get("revenue_bn") or 0.0)
        if total <= 0 or revenue_bn <= 0:
            continue
        normalized_map = {name: value / total for name, value in candidate_map.items() if value > 0.002}
        if not normalized_map:
            continue
        entry["segments"] = [
            {
                "name": name,
                "value_bn": revenue_bn * share,
                "share_pct": share * 100,
            }
            for name, share in normalized_map.items()
        ]
        anchor_bases = {
            str(enriched[anchor_index].get("structure_basis") or "segment")
            for anchor_index in (prev_index, next_index)
            if anchor_index is not None
        }
        if len(anchor_bases) == 1:
            entry["structure_basis"] = next(iter(anchor_bases))
        entry["segments_inferred"] = True
        enriched[index] = entry
    return enriched


def _historical_geography_share_map(entry: dict[str, Any]) -> dict[str, float]:
    geographies = _normalize_historical_geographies(list(entry.get("geographies") or []), entry.get("revenue_bn"))
    if not geographies:
        return {}
    share_map = {
        str(item.get("name") or "Geography"): float(item.get("share_pct") or 0.0) / 100
        for item in geographies
        if float(item.get("share_pct") or 0.0) > 0
    }
    total = sum(share_map.values())
    if total <= 0:
        return {}
    return {name: value / total for name, value in share_map.items() if value > 0}


def _is_complete_geography_snapshot(entry: dict[str, Any]) -> bool:
    share_map = _historical_geography_share_map(entry)
    if len(share_map) < 2:
        return False
    total = sum(share_map.values())
    return 0.92 <= total <= 1.08


def _backfill_historical_geography_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = [dict(entry) for entry in history]
    valid_indexes = [index for index, entry in enumerate(enriched) if _is_complete_geography_snapshot(entry)]
    if not valid_indexes:
        return enriched

    frequency: Counter[str] = Counter()
    for index in valid_indexes:
        frequency.update(_historical_geography_share_map(enriched[index]).keys())
    expected_names = [name for name, _count in frequency.most_common()]
    if len(expected_names) < 2:
        return enriched

    anchor_maps = {index: _historical_geography_share_map(enriched[index]) for index in valid_indexes}
    for index, entry in enumerate(enriched):
        if index in valid_indexes:
            entry["geographies_inferred"] = bool(entry.get("geographies_inferred"))
            enriched[index] = entry
            continue

        prev_index = next((value for value in reversed(valid_indexes) if value < index), None)
        next_index = next((value for value in valid_indexes if value > index), None)
        if prev_index is None or next_index is None:
            continue

        candidate_map: dict[str, float] = {}
        span = max(1, next_index - prev_index)
        weight = (index - prev_index) / span
        prev_map = anchor_maps[prev_index]
        next_map = anchor_maps[next_index]
        for name in expected_names:
            candidate_map[name] = prev_map.get(name, 0.0) * (1 - weight) + next_map.get(name, 0.0) * weight

        total = sum(candidate_map.values())
        revenue_bn = float(entry.get("revenue_bn") or 0.0)
        if total <= 0 or revenue_bn <= 0:
            continue
        normalized_map = {name: value / total for name, value in candidate_map.items() if value > 0.004}
        if len(normalized_map) < 2:
            continue
        entry["geographies"] = [
            {
                "name": name,
                "value_bn": revenue_bn * share,
                "share_pct": share * 100,
                "scope": "historical_interpolated",
            }
            for name, share in normalized_map.items()
        ]
        entry["geographies_inferred"] = True
        if not entry.get("structure_basis"):
            entry["structure_basis"] = "geography"
        enriched[index] = entry
    return enriched


def _sanitize_history_quality_metrics(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized = [dict(entry) for entry in history]
    previous_roe_values: list[float] = []
    for index, entry in enumerate(sanitized):
        roe_pct = entry.get("roe_pct")
        if roe_pct is None:
            continue
        current = float(roe_pct)
        if len(previous_roe_values) >= 4:
            baseline_window = previous_roe_values[-4:]
            baseline = sorted(baseline_window)[len(baseline_window) // 2]
            if baseline > 0 and current > 20 and current > baseline * 2.2 and (current - baseline) > 10:
                entry["roe_pct"] = None
                sanitized[index] = entry
                continue
        previous_roe_values.append(current)
    return sanitized


def _history_quarter_labels(history: list[dict[str, Any]]) -> list[str]:
    labels: list[str] = []
    for entry in history:
        label = str(entry.get("calendar_quarter") or entry.get("quarter_label") or "").strip()
        if not label:
            return []
        labels.append(label)
    return labels


def _history_has_contiguous_quarters(history: list[dict[str, Any]]) -> bool:
    labels = _history_quarter_labels(history)
    return len(labels) >= 2 and _periods_are_consecutive_quarters(labels)


def _recompute_history_derivatives(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    recomputed = [dict(entry) for entry in history]
    for index, entry in enumerate(recomputed):
        previous = recomputed[index - 1] if index > 0 else None
        year_ago = recomputed[index - 4] if index > 3 else None
        if previous and entry.get("revenue_bn") not in (None, 0) and previous.get("revenue_bn") not in (None, 0):
            entry["revenue_qoq_pct"] = (float(entry["revenue_bn"]) / float(previous["revenue_bn"]) - 1) * 100
        else:
            entry["revenue_qoq_pct"] = None
        if previous and entry.get("net_income_bn") is not None and previous.get("net_income_bn") not in (None, 0):
            entry["net_income_qoq_pct"] = (float(entry["net_income_bn"]) / float(previous["net_income_bn"]) - 1) * 100
        else:
            entry["net_income_qoq_pct"] = None
        if year_ago and entry.get("revenue_bn") not in (None, 0) and year_ago.get("revenue_bn") not in (None, 0):
            entry["revenue_yoy_pct"] = (float(entry["revenue_bn"]) / float(year_ago["revenue_bn"]) - 1) * 100
        else:
            entry["revenue_yoy_pct"] = None
        if year_ago and entry.get("net_income_bn") is not None and year_ago.get("net_income_bn") not in (None, 0):
            entry["net_income_yoy_pct"] = (float(entry["net_income_bn"]) / float(year_ago["net_income_bn"]) - 1) * 100
        else:
            entry["net_income_yoy_pct"] = None
        entry["ttm_revenue_bn"] = _ttm_sum(recomputed, "revenue_bn", index)
        previous_ttm = _ttm_sum(recomputed, "revenue_bn", index - 4) if index >= 4 else None
        if entry.get("ttm_revenue_bn") and previous_ttm:
            entry["ttm_revenue_growth_pct"] = (float(entry["ttm_revenue_bn"]) / float(previous_ttm) - 1) * 100
        else:
            entry["ttm_revenue_growth_pct"] = None
        recomputed_roe_pct = _ttm_roe_pct(recomputed, index)
        if recomputed_roe_pct is not None:
            entry["roe_pct"] = recomputed_roe_pct
        recomputed[index] = entry
    return recomputed


def _quarter_fallback_for_structure(entry: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    calendar_quarter = str(entry.get("calendar_quarter") or entry.get("quarter_label") or "")
    period_end = str(entry.get("period_end") or "") or _estimate_period_end(calendar_quarter)
    return {
        "calendar_quarter": calendar_quarter,
        "fiscal_label": entry.get("fiscal_label") or entry["quarter_label"],
        "period_end": period_end,
        "coverage_months": _coverage_months_for_period(calendar_quarter, period_end) if calendar_quarter else [],
        "latest_kpis": {
            "revenue_bn": entry.get("revenue_bn"),
            "revenue_yoy_pct": entry.get("revenue_yoy_pct"),
            "gaap_gross_margin_pct": entry.get("gross_margin_pct"),
            "non_gaap_gross_margin_pct": entry.get("gross_margin_pct"),
            "net_income_bn": entry.get("net_income_bn"),
            "net_income_yoy_pct": entry.get("net_income_yoy_pct"),
            "net_income_qoq_pct": entry.get("net_income_qoq_pct"),
            "ending_equity_bn": entry.get("equity_bn"),
            "gaap_eps": entry.get("eps"),
            "non_gaap_eps": entry.get("eps"),
            "operating_cash_flow_bn": entry.get("operating_cash_flow_bn"),
            "free_cash_flow_bn": entry.get("free_cash_flow_bn"),
        },
        "sources": sources,
    }


def _history_prefers_sec_only_sources(company: dict[str, Any]) -> bool:
    filing_forms = {
        str(form or "").upper()
        for form in list((company.get("official_source") or {}).get("filing_forms") or [])
    }
    # Foreign issuers that primarily file 6-K often keep useful segment/geography tables
    # in quarterly presentations rather than the filing body itself.
    return "6-K" not in filing_forms


def _guidance_commentary_is_boilerplate(commentary: Optional[str]) -> bool:
    lowered = str(commentary or "").strip().lower()
    if not lowered:
        return False
    if (
        "provide forward-looking guidance" in lowered
        and "conference call" in lowered
        and "webcast" in lowered
        and "quarterly earnings announcement" in lowered
    ):
        return True
    return bool(
        re.search(
            r"(?:business outlook|guidance).{0,220}(?:conference call and webcast|quarterly earnings announcement)",
            lowered,
            flags=re.IGNORECASE,
        )
        or any(
            phrase in lowered
            for phrase in (
                "forward-looking statements",
                "risks and uncertainties",
                "risk factors discussed",
                "could cause actual results to differ materially",
                "for more information, please refer to the risk factors",
                "insufficient revenues from such investments",
                "new liabilities assumed",
            )
        )
    )


def _guidance_commentary_has_signal(commentary: Optional[str]) -> bool:
    cleaned = _normalize_guidance_commentary(commentary)
    if not cleaned:
        return False
    if _guidance_commentary_is_boilerplate(cleaned):
        return False
    return _guidance_excerpt_signal_score(cleaned) > 0


def _guidance_revenue_signal_rank(guidance: dict[str, Any]) -> int:
    if any(guidance.get(key) is not None for key in ("revenue_bn", "revenue_low_bn", "revenue_high_bn")):
        return 2
    return 1 if _guidance_commentary_has_signal(guidance.get("commentary")) else 0


def _guidance_signal_score(guidance: dict[str, Any]) -> int:
    rank = _guidance_revenue_signal_rank(guidance)
    if rank >= 2:
        return 10_000
    commentary = _normalize_guidance_commentary(guidance.get("commentary"))
    if not commentary:
        return 0
    return _guidance_excerpt_signal_score(commentary)


def _guidance_snapshot_has_official_context(guidance: dict[str, Any]) -> bool:
    mode = str(guidance.get("mode") or "").strip().casefold()
    if mode not in {"official", "official_context"}:
        return False
    commentary = _normalize_guidance_commentary(guidance.get("commentary"))
    if _guidance_commentary_has_signal(commentary):
        return True
    if not commentary or _guidance_commentary_is_boilerplate(commentary):
        return False
    source_role = str(guidance.get("source_role") or "").strip().casefold()
    source_kind = str(guidance.get("source_kind") or "").strip().casefold()
    return source_role in {"earnings_call", "earnings_commentary"} or source_kind == "call_summary"


def _normalize_embedded_source_url(raw_value: str) -> Optional[str]:
    candidate = str(raw_value or "").strip().rstrip(').,;\'"')
    if not candidate:
        return None
    if candidate.startswith("www."):
        candidate = f"https://{candidate}"
    elif re.fullmatch(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}/[^\s]+", candidate):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.netloc.lower().split(":")[0] in {"view.officeapps.live.com", "view.officeapps-df.live.com"}:
        source_url = str(parse_qs(parsed.query).get("src", [""])[0] or "").strip()
        if source_url:
            candidate = source_url
            parsed = urlparse(candidate)
    if Path(parsed.path.lower()).suffix in {".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".xml"}:
        return None
    return candidate


def _guidance_related_sources_from_materials(
    company: dict[str, Any],
    source_materials: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    discovered: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    url_pattern = re.compile(
        r"(https?://[^\s<>\"]+|www\.[^\s<>\"]+|[A-Za-z0-9.-]+\.[A-Za-z]{2,}/[^\s<>\"]+)"
    )
    call_tokens = ("earnings call", "conference call", "webcast", "prepared remarks", "transcript", "replay")

    def _is_generic_investor_landing(candidate: str) -> bool:
        path = urlparse(candidate).path.lower().rstrip("/")
        if path in {
            "",
            "/",
            "/investor",
            "/investors",
            "/investor-relations",
            "/investorrelations",
            "/en-us/investor",
            "/en-us/investors",
            "/events",
            "/event",
            "/presentations",
            "/presentation",
            "/news",
            "/media",
            "/financial-information",
        }:
            return True
        return path.endswith(
            (
                "/investor/earnings/default.aspx",
                "/investors/earnings/default.aspx",
                "/news/default.aspx",
                "/events/default.aspx",
                "/presentations/default.aspx",
                "/quarterly-results/default.aspx",
            )
        )

    def _looks_like_specific_call_source(candidate: str, context: str) -> bool:
        parsed = urlparse(candidate)
        path = parsed.path.lower().strip("/")
        if _is_generic_investor_landing(candidate):
            return False
        tokens = f"{candidate.lower()} {context.lower()}"
        if any(token in tokens for token in ("transcript", "prepared remarks", "conference call", "earnings call", "webcast", "replay")):
            return True
        return any(
            token in path
            for token in ("earnings", "results", "conference", "webcast", "transcript", "prepared-remarks", "preparedremarks", "replay", "event")
        )

    for item in source_materials:
        if item.get("status") not in {"cached", "fetched"}:
            continue
        text_path = str(item.get("text_path") or "").strip()
        if not text_path or not Path(text_path).exists():
            continue
        text = Path(text_path).read_text(encoding="utf-8", errors="ignore")
        lowered = text.lower()
        if not any(token in lowered for token in call_tokens):
            continue
        for match in url_pattern.finditer(text):
            candidate = _normalize_embedded_source_url(match.group(1))
            if not candidate or candidate in seen_urls:
                continue
            start = max(0, match.start() - 140)
            end = min(len(text), match.end() + 140)
            context = text[start:end].lower()
            if not any(token in context for token in call_tokens):
                continue
            if not _looks_like_specific_call_source(candidate, context):
                continue
            discovered.append(
                {
                    "label": f"{company['english_name']} earnings call",
                    "url": candidate,
                    "kind": "call_summary",
                    "role": "earnings_call",
                    "date": str(item.get("date") or ""),
                }
            )
            seen_urls.add(candidate)
    return discovered


def _historical_quarter_seed_entry(
    company: dict[str, Any],
    calendar_quarter: str,
    fixture: Optional[dict[str, Any]],
    entry: Optional[dict[str, Any]],
) -> dict[str, Any]:
    fixture = dict(fixture or {})
    entry = dict(entry or {})
    latest_kpis = dict(fixture.get("latest_kpis") or {})
    period_end = _resolved_period_end(
        calendar_quarter,
        entry_period_end=str(entry.get("period_end") or "") or None,
        fixture_period_end=str(fixture.get("period_end") or "") or None,
    )
    return {
        "quarter_label": calendar_quarter,
        "calendar_quarter": calendar_quarter,
        "fiscal_label": _resolved_fiscal_label(
            company,
            calendar_quarter,
            period_end,
            explicit_label=str(entry.get("fiscal_label") or fixture.get("fiscal_label") or "") or None,
        ),
        "period_end": period_end,
        "revenue_bn": entry.get("revenue_bn", latest_kpis.get("revenue_bn")),
        "net_income_bn": entry.get("net_income_bn", latest_kpis.get("net_income_bn")),
        "gross_margin_pct": entry.get("gross_margin_pct", latest_kpis.get("gaap_gross_margin_pct")),
        "revenue_yoy_pct": entry.get("revenue_yoy_pct", latest_kpis.get("revenue_yoy_pct")),
        "net_income_yoy_pct": entry.get("net_income_yoy_pct", latest_kpis.get("net_income_yoy_pct")),
        "equity_bn": entry.get("equity_bn", latest_kpis.get("ending_equity_bn")),
        "eps": entry.get("eps", latest_kpis.get("gaap_eps")),
        "operating_cash_flow_bn": entry.get("operating_cash_flow_bn", latest_kpis.get("operating_cash_flow_bn")),
        "free_cash_flow_bn": entry.get("free_cash_flow_bn", latest_kpis.get("free_cash_flow_bn")),
    }


def _historical_official_quarter_payload_from_parsed(
    company: dict[str, Any],
    seed_entry: dict[str, Any],
    parsed: dict[str, Any],
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "latest_kpis": dict(parsed.get("latest_kpis") or {}),
        "current_segments": [dict(item) for item in list(parsed.get("current_segments") or []) if isinstance(item, dict)],
        "current_geographies": [dict(item) for item in list(parsed.get("current_geographies") or []) if isinstance(item, dict)],
        "guidance": dict(parsed.get("guidance") or {}),
        "source_url": str(sources[0].get("url") or "") if sources else "",
        "source_date": str(sources[0].get("date") or "") if sources else "",
        "profit_basis": parsed.get("profit_basis"),
        "guidance_source_label": str(sources[0].get("label") or "") if sources else "",
    }
    return _sanitize_historical_official_quarter_payload(
        company,
        payload,
        seed_entry=seed_entry,
        sources=sources,
    )


def _refresh_historical_guidance_from_cached_materials(
    company: dict[str, Any],
    calendar_quarter: str,
    cached_payload: dict[str, Any],
    cached_material_sources: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not cached_material_sources:
        return None
    source_materials = hydrate_source_materials(
        str(company["id"]),
        calendar_quarter,
        cached_material_sources,
        refresh=False,
    )
    loaded_materials = _load_materials(source_materials)
    refreshed_guidance, guidance_material = _extract_generic_guidance_from_materials(loaded_materials)
    if not refreshed_guidance:
        return None
    refreshed_payload = _clone_cached_official_quarter_payload(cached_payload)
    guidance = _merge_guidance_payload(
        dict(refreshed_payload.get("guidance") or {}),
        dict(refreshed_guidance),
    )
    if guidance_material is not None:
        source_role = str(guidance_material.get("role") or "").strip()
        source_kind = str(guidance_material.get("kind") or "").strip()
        source_label = str(guidance_material.get("label") or "").strip()
        source_url = str(guidance_material.get("url") or "").strip()
        source_date = str(guidance_material.get("date") or "").strip()
        if source_role:
            guidance["source_role"] = source_role
        if source_kind:
            guidance["source_kind"] = source_kind
        if source_label:
            guidance["source_label"] = source_label
            refreshed_payload["guidance_source_label"] = source_label
        if source_url:
            refreshed_payload["source_url"] = source_url
        if source_date:
            refreshed_payload["source_date"] = source_date
    commentary = _normalize_guidance_commentary(guidance.get("commentary"))
    if commentary:
        guidance["commentary"] = commentary
    else:
        guidance.pop("commentary", None)
    refreshed_payload["guidance"] = guidance
    return _sanitize_historical_official_quarter_payload(
        company,
        refreshed_payload,
        sources=cached_material_sources,
    )


def _extract_historical_official_quarter_payload_from_cached_materials_lightweight(
    company: dict[str, Any],
    calendar_quarter: str,
    seed_entry: dict[str, Any],
    cached_material_sources: list[dict[str, Any]],
    *,
    require_guidance: bool,
) -> Optional[dict[str, Any]]:
    if not cached_material_sources:
        return None
    source_materials = hydrate_source_materials(
        str(company["id"]),
        calendar_quarter,
        cached_material_sources,
        refresh=False,
    )
    loaded_materials = _load_materials(source_materials)
    if not loaded_materials:
        return None

    revenue_bn = seed_entry.get("revenue_bn")
    latest_kpis = dict(_quarter_fallback_for_structure(seed_entry, cached_material_sources).get("latest_kpis") or {})
    current_segments = [
        dict(item)
        for item in _extract_company_segments(
            str(company["id"]),
            loaded_materials,
            revenue_bn,
            target_calendar_quarter=calendar_quarter,
        )
        if isinstance(item, dict)
    ]
    current_geographies = [
        dict(item)
        for item in _extract_company_geographies(
            str(company["id"]),
            loaded_materials,
            revenue_bn,
        )
        if isinstance(item, dict)
    ]

    guidance: dict[str, Any] = {}
    guidance_source_label = ""
    source_url = str(cached_material_sources[0].get("url") or "") if cached_material_sources else ""
    source_date = str(cached_material_sources[0].get("date") or "") if cached_material_sources else ""
    lightweight_guidance, guidance_material = _extract_generic_guidance_from_materials(loaded_materials)
    if lightweight_guidance:
        guidance = _merge_guidance_payload({}, dict(lightweight_guidance))
        commentary = _normalize_guidance_commentary(guidance.get("commentary"))
        if commentary:
            guidance["commentary"] = commentary
        else:
            guidance.pop("commentary", None)
        if guidance_material is not None:
            material_role = str(guidance_material.get("role") or "").strip()
            material_kind = str(guidance_material.get("kind") or "").strip()
            material_label = str(guidance_material.get("label") or "").strip()
            material_url = str(guidance_material.get("url") or "").strip()
            material_date = str(guidance_material.get("date") or "").strip()
            if material_role:
                guidance["source_role"] = material_role
            if material_kind:
                guidance["source_kind"] = material_kind
            if material_label:
                guidance["source_label"] = material_label
                guidance_source_label = material_label
            if material_url:
                source_url = material_url
            if material_date:
                source_date = material_date

    payload = {
        "latest_kpis": latest_kpis,
        "current_segments": current_segments,
        "current_geographies": current_geographies,
        "guidance": guidance,
        "source_url": source_url,
        "source_date": source_date,
        "profit_basis": None,
        "guidance_source_label": guidance_source_label,
    }
    payload = _sanitize_historical_official_quarter_payload(
        company,
        payload,
        seed_entry=seed_entry,
        sources=cached_material_sources,
    )
    has_structures = bool(current_segments) or bool(current_geographies)
    has_usable_guidance = _historical_guidance_candidate_is_usable(payload, require_guidance=require_guidance)
    if require_guidance and not has_usable_guidance:
        return None
    if not (bool(payload.get("current_segments")) or bool(payload.get("current_geographies"))) and not has_usable_guidance:
        return None
    return payload


def _parse_historical_official_quarter_payload_from_sources(
    company: dict[str, Any],
    calendar_quarter: str,
    seed_entry: dict[str, Any],
    sources: list[dict[str, Any]],
    *,
    refresh: bool,
    require_guidance: bool,
) -> Optional[dict[str, Any]]:
    if not sources:
        return None
    source_materials = hydrate_source_materials(
        str(company["id"]),
        calendar_quarter,
        sources,
        refresh=refresh,
    )
    if require_guidance:
        embedded_sources = _guidance_related_sources_from_materials(company, source_materials)
        if embedded_sources:
            existing_urls = {str(item.get("url") or "") for item in sources}
            appended = [item for item in embedded_sources if str(item.get("url") or "") not in existing_urls]
            if appended:
                sources = list(sources) + appended
                source_materials = hydrate_source_materials(
                    str(company["id"]),
                    calendar_quarter,
                    sources,
                    refresh=refresh,
                )
    parsed = parse_official_materials(
        company,
        _quarter_fallback_for_structure(seed_entry, sources),
        source_materials,
    )
    if not parsed:
        return None
    return _historical_official_quarter_payload_from_parsed(company, seed_entry, parsed, sources)


def _historical_guidance_candidate_is_usable(payload: Optional[dict[str, Any]], *, require_guidance: bool) -> bool:
    guidance = dict((payload or {}).get("guidance") or {})
    if _guidance_revenue_signal_rank(guidance) >= 2:
        return True
    if _guidance_snapshot_has_official_context(guidance):
        return True
    return not require_guidance and bool(payload)


def _load_or_parse_historical_official_quarter_payload(
    company: dict[str, Any],
    calendar_quarter: str,
    *,
    entry: Optional[dict[str, Any]] = None,
    require_guidance: bool = False,
    prefer_lightweight_structure: bool = False,
) -> Optional[dict[str, Any]]:
    cached_parsed = _load_historical_official_quarter_cache(str(company["id"]), calendar_quarter)
    if entry is not None and cached_parsed is not None and not _historical_official_cache_is_temporally_aligned(entry, cached_parsed):
        cached_parsed = _salvage_stale_historical_cache_structures(company, entry, cached_parsed)
    if cached_parsed is not None and (not require_guidance or _guidance_revenue_signal_rank(dict(cached_parsed.get("guidance") or {})) >= 2):
        return cached_parsed

    fixture = get_quarter_fixture(str(company["id"]), calendar_quarter)
    seed_entry = _historical_quarter_seed_entry(company, calendar_quarter, fixture, entry)
    base_sources = list((fixture or {}).get("sources") or [])
    period_end = str(seed_entry.get("period_end") or "") or _estimate_period_end(calendar_quarter)
    prefer_sec_only = _history_prefers_sec_only_sources(company) and not require_guidance
    attempts = (False, True) if require_guidance else (False,)
    best_payload = cached_parsed
    best_rank = _guidance_revenue_signal_rank(dict((cached_parsed or {}).get("guidance") or {})) if cached_parsed else -1
    best_score = _guidance_signal_score(dict((cached_parsed or {}).get("guidance") or {})) if cached_parsed else -1
    seen_source_sets: set[tuple[tuple[str, str, str, str], ...]] = set()

    cached_material_sources = _discover_cached_material_sources(
        str(company["id"]),
        calendar_quarter,
        required_roles={"earnings_release", "earnings_call", "earnings_presentation", "earnings_commentary", "sec_filing"},
    )
    cached_material_signature = tuple(
        (
            str(item.get("url") or ""),
            str(item.get("kind") or ""),
            str(item.get("role") or ""),
            str(item.get("date") or ""),
        )
        for item in cached_material_sources
    )
    if cached_parsed is not None and cached_material_sources:
        refreshed_cached_payload = _refresh_historical_guidance_from_cached_materials(
            company,
            calendar_quarter,
            cached_parsed,
            list(cached_material_sources),
        )
        if refreshed_cached_payload is not None:
            refreshed_rank = _guidance_revenue_signal_rank(dict(refreshed_cached_payload.get("guidance") or {}))
            refreshed_score = _guidance_signal_score(dict(refreshed_cached_payload.get("guidance") or {}))
            if (
                best_payload is None
                or refreshed_rank > best_rank
                or (refreshed_rank == best_rank and refreshed_score > best_score)
            ):
                best_payload = refreshed_cached_payload
                best_rank = refreshed_rank
                best_score = refreshed_score
            if _historical_guidance_candidate_is_usable(refreshed_cached_payload, require_guidance=require_guidance):
                _store_historical_official_quarter_cache(str(company["id"]), calendar_quarter, refreshed_cached_payload)
                return refreshed_cached_payload
    if cached_material_signature:
        seen_source_sets.add(cached_material_signature)
        if prefer_lightweight_structure:
            lightweight_payload = _extract_historical_official_quarter_payload_from_cached_materials_lightweight(
                company,
                calendar_quarter,
                seed_entry,
                list(cached_material_sources),
                require_guidance=require_guidance,
            )
            lightweight_rank = _guidance_revenue_signal_rank(dict((lightweight_payload or {}).get("guidance") or {}))
            lightweight_score = _guidance_signal_score(dict((lightweight_payload or {}).get("guidance") or {}))
            if (
                lightweight_payload is not None
                and (
                    best_payload is None
                    or lightweight_rank > best_rank
                    or (
                        lightweight_rank == best_rank
                        and lightweight_score > best_score
                    )
                    or (
                        lightweight_rank == best_rank
                        and lightweight_score == best_score
                        and len(list(lightweight_payload.get("current_segments") or []))
                        + len(list(lightweight_payload.get("current_geographies") or []))
                        > len(list(best_payload.get("current_segments") or []))
                        + len(list(best_payload.get("current_geographies") or []))
                    )
                )
            ):
                best_payload = lightweight_payload
                best_rank = lightweight_rank
                best_score = lightweight_score
            if lightweight_payload is not None and (
                _historical_guidance_candidate_is_usable(lightweight_payload, require_guidance=require_guidance)
                or (
                    not require_guidance
                    and (
                        bool(list(lightweight_payload.get("current_segments") or []))
                        or bool(list(lightweight_payload.get("current_geographies") or []))
                    )
                )
            ):
                _store_historical_official_quarter_cache(str(company["id"]), calendar_quarter, lightweight_payload)
                return lightweight_payload
        cached_material_payload = _parse_historical_official_quarter_payload_from_sources(
            company,
            calendar_quarter,
            seed_entry,
            list(cached_material_sources),
            refresh=False,
            require_guidance=require_guidance,
        )
        cached_material_rank = _guidance_revenue_signal_rank(dict((cached_material_payload or {}).get("guidance") or {}))
        cached_material_score = _guidance_signal_score(dict((cached_material_payload or {}).get("guidance") or {}))
        if (
            cached_material_payload is not None
            and (
                best_payload is None
                or cached_material_rank > best_rank
                or (
                    cached_material_rank == best_rank
                    and cached_material_score > best_score
                )
                or (
                    cached_material_rank == best_rank
                    and cached_material_score == best_score
                    and len(list(cached_material_payload.get("current_segments") or []))
                    + len(list(cached_material_payload.get("current_geographies") or []))
                    > len(list(best_payload.get("current_segments") or []))
                    + len(list(best_payload.get("current_geographies") or []))
                )
            )
        ):
            best_payload = cached_material_payload
            best_rank = cached_material_rank
            best_score = cached_material_score
        if _historical_guidance_candidate_is_usable(cached_material_payload, require_guidance=require_guidance):
            _store_historical_official_quarter_cache(str(company["id"]), calendar_quarter, cached_material_payload)
            return cached_material_payload

    for refresh in attempts:
        sources = resolve_official_sources(
            company,
            calendar_quarter,
            period_end,
            base_sources,
            refresh=refresh,
            prefer_sec_only=prefer_sec_only,
        )
        source_signature = tuple(
            (
                str(item.get("url") or ""),
                str(item.get("kind") or ""),
                str(item.get("role") or ""),
                str(item.get("date") or ""),
            )
            for item in sources
        )
        if source_signature in seen_source_sets:
            continue
        seen_source_sets.add(source_signature)
        payload = _parse_historical_official_quarter_payload_from_sources(
            company,
            calendar_quarter,
            seed_entry,
            list(sources),
            refresh=refresh,
            require_guidance=require_guidance,
        )
        if not payload:
            continue
        payload_rank = _guidance_revenue_signal_rank(dict(payload.get("guidance") or {}))
        payload_score = _guidance_signal_score(dict(payload.get("guidance") or {}))
        if (
            best_payload is None
            or payload_rank > best_rank
            or (payload_rank == best_rank and payload_score > best_score)
            or (
                payload_rank == best_rank
                and payload_score == best_score
                and len(list(payload.get("current_segments") or [])) + len(list(payload.get("current_geographies") or []))
                > len(list(best_payload.get("current_segments") or [])) + len(list(best_payload.get("current_geographies") or []))
            )
        ):
            best_payload = payload
            best_rank = payload_rank
            best_score = payload_score
        if payload_rank >= 2 or not require_guidance:
            break

    if best_payload is not None:
        _store_historical_official_quarter_cache(str(company["id"]), calendar_quarter, best_payload)
    return best_payload


def _resolve_quarter_guidance_snapshot(
    company_id: str,
    calendar_quarter: Optional[str],
    history: list[dict[str, Any]],
    *,
    allow_expensive_parse: bool = True,
) -> dict[str, Any]:
    quarter = str(calendar_quarter or "").strip()
    if not quarter:
        return {"calendar_quarter": "", "fiscal_label": "", "guidance": {}}
    quarter_history: list[dict[str, Any]] = []
    for item in history:
        quarter_history.append(dict(item))
        item_quarter = str(item.get("calendar_quarter") or item.get("quarter_label") or "")
        if item_quarter == quarter:
            break
    fixture = get_quarter_fixture(company_id, quarter)
    history_entry = next(
        (
            item
            for item in history
            if str(item.get("calendar_quarter") or item.get("quarter_label") or "") == quarter
        ),
        None,
    )
    fiscal_label = str(
        (history_entry or {}).get("fiscal_label")
        or (fixture or {}).get("fiscal_label")
        or quarter
    )
    guidance = dict((fixture or {}).get("guidance") or {})
    if _guidance_revenue_signal_rank(guidance) >= 2:
        return {
            "calendar_quarter": quarter,
            "fiscal_label": fiscal_label,
            "guidance": guidance,
        }
    company = get_company(company_id)
    if company and company.get("official_source"):
        cached_payload = _load_historical_official_quarter_cache(str(company["id"]), quarter)
        cached_guidance = dict((cached_payload or {}).get("guidance") or {})
        if cached_payload is not None and (
            history_entry is None or _historical_official_guidance_cache_is_temporally_aligned(history_entry, cached_payload)
        ):
            if (
                _guidance_revenue_signal_rank(cached_guidance) > _guidance_revenue_signal_rank(guidance)
                or (
                    _guidance_revenue_signal_rank(cached_guidance) == _guidance_revenue_signal_rank(guidance)
                    and _guidance_signal_score(cached_guidance) > _guidance_signal_score(guidance)
                )
            ):
                guidance = cached_guidance
            if _guidance_revenue_signal_rank(guidance) >= 2 or _guidance_snapshot_has_official_context(guidance):
                commentary = _normalize_guidance_commentary(guidance.get("commentary"))
                if commentary:
                    guidance["commentary"] = commentary
                else:
                    guidance.pop("commentary", None)
                return {
                    "calendar_quarter": quarter,
                    "fiscal_label": fiscal_label,
                    "guidance": guidance,
                }
        if not allow_expensive_parse:
            commentary = _normalize_guidance_commentary(guidance.get("commentary"))
            if commentary:
                guidance["commentary"] = commentary
            else:
                guidance.pop("commentary", None)
            return {
                "calendar_quarter": quarter,
                "fiscal_label": fiscal_label,
                "guidance": guidance,
            }
        parsed = _load_or_parse_historical_official_quarter_payload(
            company,
            quarter,
            entry=history_entry,
            require_guidance=False,
        )
        parsed_guidance = dict((parsed or {}).get("guidance") or {})
        if _guidance_revenue_signal_rank(parsed_guidance) < 2 and not _guidance_snapshot_has_official_context(parsed_guidance):
            parsed = _load_or_parse_historical_official_quarter_payload(
                company,
                quarter,
                entry=history_entry,
                require_guidance=True,
            )
            parsed_guidance = dict((parsed or {}).get("guidance") or {})
        if (
            _guidance_revenue_signal_rank(parsed_guidance) > _guidance_revenue_signal_rank(guidance)
            or (
                _guidance_revenue_signal_rank(parsed_guidance) == _guidance_revenue_signal_rank(guidance)
                and _guidance_signal_score(parsed_guidance) > _guidance_signal_score(guidance)
            )
        ):
            guidance = parsed_guidance
    commentary = _normalize_guidance_commentary(guidance.get("commentary"))
    if commentary:
        guidance["commentary"] = commentary
    else:
        guidance.pop("commentary", None)
    return {
        "calendar_quarter": quarter,
        "fiscal_label": fiscal_label,
        "guidance": guidance,
    }


def _same_metric_run_length(history: list[dict[str, Any]], key: str) -> int:
    longest = 1
    current = 1
    previous: Optional[float] = None
    for entry in history:
        value = entry.get(key)
        if value is None:
            current = 1
            previous = None
            continue
        current_value = round(float(value), 6)
        if previous is not None and current_value == previous:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
        previous = current_value
    return longest


def _history_needs_official_metric_enrichment(
    company: dict[str, Any],
    history: list[dict[str, Any]],
) -> bool:
    if not company.get("official_source"):
        return False
    structured_entries = [entry for entry in history if str(entry.get("source_type") or "") == "structured_financial_series"]
    if not structured_entries:
        return False
    missing_revenue = sum(1 for entry in structured_entries if entry.get("revenue_bn") is None)
    missing_profit = sum(1 for entry in structured_entries if entry.get("net_income_bn") is None)
    missing_margin = sum(1 for entry in structured_entries if entry.get("gross_margin_pct") is None)
    missing_equity = sum(1 for entry in structured_entries if entry.get("equity_bn") is None)
    missing_roe = sum(1 for entry in structured_entries if entry.get("roe_pct") is None)
    repeated_revenue_run = _same_metric_run_length(structured_entries, "revenue_bn")
    repeated_profit_run = _same_metric_run_length(structured_entries, "net_income_bn")
    return (
        missing_revenue > 0
        or missing_profit > 0
        or missing_margin >= max(4, len(structured_entries) // 2)
        or missing_equity > 0
        or missing_roe > 0
        or repeated_revenue_run >= 3
        or repeated_profit_run >= 3
    )


def _entry_needs_official_metric_enrichment(entry: dict[str, Any]) -> bool:
    return (
        entry.get("revenue_bn") is None
        or entry.get("net_income_bn") is None
        or entry.get("gross_margin_pct") is None
        or entry.get("revenue_yoy_pct") is None
        or entry.get("net_income_yoy_pct") is None
        or entry.get("equity_bn") is None
        or entry.get("roe_pct") is None
    )


def _entry_needs_official_core_metric_enrichment(entry: dict[str, Any]) -> bool:
    return (
        entry.get("revenue_bn") is None
        or entry.get("net_income_bn") is None
        or entry.get("gross_margin_pct") is None
        or entry.get("revenue_yoy_pct") is None
        or entry.get("net_income_yoy_pct") is None
    )


def _historical_metric_candidate(
    existing_value: Optional[float],
    parsed_value: Optional[float],
    *,
    min_ratio: float = 0.45,
    max_ratio: float = 2.25,
) -> Optional[float]:
    if parsed_value is None:
        return existing_value
    if existing_value in (None, 0):
        return float(parsed_value)
    ratio = float(parsed_value) / float(existing_value)
    if min_ratio <= ratio <= max_ratio:
        return float(parsed_value)
    return existing_value


def _historical_revenue_candidate(
    existing_revenue_bn: Optional[float],
    parsed_revenue_bn: Optional[float],
    *,
    reference_profit_bn: Optional[float],
) -> Optional[float]:
    candidate = _historical_metric_candidate(existing_revenue_bn, parsed_revenue_bn)
    if candidate is None or existing_revenue_bn in (None, 0) or reference_profit_bn in (None, 0):
        return candidate
    candidate_margin = abs(float(reference_profit_bn)) / float(candidate)
    existing_margin = abs(float(reference_profit_bn)) / float(existing_revenue_bn)
    if (
        candidate != existing_revenue_bn
        and candidate_margin >= 0.75
        and existing_margin <= 0.65
        and float(existing_revenue_bn) >= float(candidate) * 1.35
    ):
        return float(existing_revenue_bn)
    return candidate


def _segment_snapshot_quality(
    company: dict[str, Any],
    segments: list[dict[str, Any]],
    revenue_bn: Optional[float],
    reference_profile: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    normalized = _normalize_historical_segments(company, segments, revenue_bn)
    if not normalized or _segments_are_geography_like(company, list(normalized)):
        return {
            "segments": [],
            "count": 0,
            "coverage_ratio": 0.0,
            "reasonable": False,
            "complete": False,
        }
    total_value = _segments_total_value(normalized)
    coverage_ratio = total_value / float(revenue_bn) if revenue_bn not in (None, 0) else 0.0
    reasonable = _quarterly_structure_looks_reasonable(normalized, revenue_bn)
    complete = reasonable and _is_complete_segment_snapshot(
        company,
        {
            "segments": normalized,
            "revenue_bn": revenue_bn,
        },
        reference_profile,
    )
    return {
        "segments": normalized,
        "count": len(normalized),
        "coverage_ratio": coverage_ratio,
        "reasonable": reasonable,
        "complete": complete,
    }


def _should_replace_history_segments(
    existing_quality: dict[str, Any],
    candidate_quality: dict[str, Any],
    *,
    allow_partial: bool,
) -> bool:
    if not candidate_quality.get("segments"):
        return False
    if not existing_quality.get("segments"):
        return True
    if existing_quality.get("complete") and not candidate_quality.get("complete"):
        return False
    if candidate_quality.get("complete") and not existing_quality.get("complete"):
        return True
    if candidate_quality.get("complete") and existing_quality.get("complete"):
        return bool(
            int(candidate_quality.get("count") or 0) >= int(existing_quality.get("count") or 0)
            and float(candidate_quality.get("coverage_ratio") or 0.0)
            >= float(existing_quality.get("coverage_ratio") or 0.0) - 0.03
        )
    if not allow_partial:
        return False
    return bool(
        int(candidate_quality.get("count") or 0) > int(existing_quality.get("count") or 0)
        or float(candidate_quality.get("coverage_ratio") or 0.0)
        > float(existing_quality.get("coverage_ratio") or 0.0) + 0.08
        or not existing_quality.get("reasonable")
    )


def _enrich_history_with_official_structures(
    company: dict[str, Any],
    history: list[dict[str, Any]],
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> list[dict[str, Any]]:
    needs_metric_enrichment = _history_needs_official_metric_enrichment(company, history)
    needs_structure_enrichment = any(
        not entry.get("segments") or not entry.get("geographies")
        for entry in history
    )
    if not company.get("official_source") or (not needs_metric_enrichment and not needs_structure_enrichment):
        return history
    enriched = [dict(entry) for entry in history]
    target_indexes = [
        index
        for index, entry in enumerate(enriched)
        if (
            not entry.get("segments")
            or not entry.get("geographies")
            or (needs_metric_enrichment and _entry_needs_official_core_metric_enrichment(entry))
        )
    ]
    total_entries = max(len(target_indexes), 1)
    if not target_indexes:
        return enriched
    reference_profile = _historical_segment_reference_profile(company, enriched)

    def _enrich_single(index: int) -> tuple[int, dict[str, Any]]:
        entry = dict(enriched[index])
        period = str(entry["quarter_label"])
        prefer_lightweight_structure = not _entry_needs_official_core_metric_enrichment(entry)
        cached_parsed = _load_or_parse_historical_official_quarter_payload(
            company,
            period,
            entry=entry,
            prefer_lightweight_structure=prefer_lightweight_structure,
        )
        if cached_parsed is None:
            return (index, entry)
        latest_kpis = dict(cached_parsed.get("latest_kpis") or {})
        stale_structure_only = bool(cached_parsed.get("structure_only_from_stale_cache"))
        profit_basis = str(cached_parsed.get("profit_basis") or "")
        if profit_basis == "adjusted_special_items" and latest_kpis.get("net_income_bn") is not None:
            net_income_bn = latest_kpis.get("net_income_bn")
        else:
            net_income_bn = _historical_metric_candidate(entry.get("net_income_bn"), latest_kpis.get("net_income_bn"), min_ratio=0.25, max_ratio=2.8)
        revenue_bn = _historical_revenue_candidate(
            entry.get("revenue_bn"),
            latest_kpis.get("revenue_bn"),
            reference_profit_bn=net_income_bn if net_income_bn is not None else entry.get("net_income_bn"),
        )
        gross_margin_pct = latest_kpis.get("gaap_gross_margin_pct")
        revenue_yoy_pct = latest_kpis.get("revenue_yoy_pct")
        net_income_yoy_pct = latest_kpis.get("net_income_yoy_pct")
        ending_equity_bn = _normalize_equity_bn_value(
            latest_kpis.get("ending_equity_bn"),
            reference_equity_bn=entry.get("equity_bn"),
            revenue_bn=revenue_bn if revenue_bn is not None else entry.get("revenue_bn"),
        )
        if ending_equity_bn is not None and latest_kpis.get("ending_equity_bn") != ending_equity_bn:
            latest_kpis["ending_equity_bn"] = ending_equity_bn
            cached_parsed["latest_kpis"] = dict(latest_kpis)
            _store_historical_official_quarter_cache(str(company["id"]), period, cached_parsed)
        if revenue_bn is not None:
            entry["revenue_bn"] = float(revenue_bn)
        if net_income_bn is not None:
            entry["net_income_bn"] = float(net_income_bn)
        if ending_equity_bn is not None and float(ending_equity_bn) > 0:
            entry["equity_bn"] = float(ending_equity_bn)
        if gross_margin_pct is not None and -10 <= float(gross_margin_pct) <= 95:
            entry["gross_margin_pct"] = float(gross_margin_pct)
        if revenue_yoy_pct is not None and abs(float(revenue_yoy_pct)) <= 500:
            entry["revenue_yoy_pct"] = float(revenue_yoy_pct)
        if net_income_yoy_pct is not None and abs(float(net_income_yoy_pct)) <= 800:
            entry["net_income_yoy_pct"] = float(net_income_yoy_pct)
        if entry.get("revenue_bn") not in (None, 0) and entry.get("net_income_bn") is not None:
            entry["net_margin_pct"] = _safe_ratio(float(entry["net_income_bn"]), float(entry["revenue_bn"]))
        trusted_revenue = entry.get("revenue_bn") or latest_kpis.get("revenue_bn")
        existing_segment_quality = _segment_snapshot_quality(
            company,
            list(entry.get("segments") or []),
            trusted_revenue,
            reference_profile,
        )
        parsed_segment_quality = _segment_snapshot_quality(
            company,
            list(cached_parsed.get("current_segments") or []),
            trusted_revenue,
            reference_profile,
        )
        parsed_segments = _normalize_historical_segments(
            company,
            list(cached_parsed.get("current_segments") or []),
            trusted_revenue,
        )
        parsed_geographies = _normalize_historical_geographies(
            list(cached_parsed.get("current_geographies") or []),
            trusted_revenue,
        )
        valid_parsed_segments = bool(parsed_segment_quality.get("complete")) and _should_replace_history_segments(
            existing_segment_quality,
            parsed_segment_quality,
            allow_partial=False,
        )
        partial_parsed_segments = (
            bool(parsed_segments)
            and bool(parsed_segment_quality.get("reasonable"))
            and len(parsed_segments) >= 2
            and _should_replace_history_segments(
                existing_segment_quality,
                parsed_segment_quality,
                allow_partial=True,
            )
        )
        if valid_parsed_segments:
            entry["segments"] = parsed_segments
            entry["structure_basis"] = "segment"
            entry["segments_partial"] = False
        elif partial_parsed_segments:
            # Preserve partially parsed segment snapshots when they are numerically
            # reasonable. Later harmonization can still decide whether to use them
            # for 12-quarter transition analysis, but we should not discard useful
            # quarter-level details up front.
            entry["segments"] = parsed_segments
            entry["structure_basis"] = "segment_partial"
            entry["segments_partial"] = True
        elif parsed_geographies and not existing_segment_quality.get("complete"):
            entry["structure_basis"] = "geography"
            entry["segments"] = []
            entry["segments_partial"] = False
        if parsed_geographies:
            entry["geographies"] = parsed_geographies
        if (
            not stale_structure_only
            and (
                latest_kpis.get("revenue_bn") is not None
                or latest_kpis.get("net_income_bn") is not None
                or latest_kpis.get("ending_equity_bn") is not None
                or parsed_segments
                or parsed_geographies
            )
        ):
            entry["source_type"] = "official_release"
        source_url = str(cached_parsed.get("source_url") or "")
        source_date = str(cached_parsed.get("source_date") or "")
        if source_url:
            entry["source_url"] = source_url
        if _is_iso_date_token(source_date):
            entry["release_date"] = source_date
        return (index, entry)

    max_workers = min(6, len(target_indexes))
    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_enrich_single, index): index for index in target_indexes}
        for future in as_completed(future_map):
            index, entry = future.result()
            enriched[index] = entry
            completed += 1
            if progress_callback is not None:
                progress_callback(
                    completed / total_entries,
                    f"正在补齐 {entry['quarter_label']} 的官方结构与关键指标...",
                )
    if progress_callback is not None:
        progress_callback(1.0, "历史官方结构补齐完成。")
    return enriched


def resolve_structure_dimension(company_id: str, historical_bundle: list[dict[str, Any]]) -> str:
    company = get_company(company_id)
    if historical_bundle and all(entry.get("segments") and not _segments_are_geography_like(company, list(entry.get("segments") or [])) for entry in historical_bundle):
        bases = {str(entry.get("structure_basis") or "segment") for entry in historical_bundle}
        if bases == {"geography"}:
            return "geography"
        return "segment"
    if historical_bundle and all(entry.get("geographies") for entry in historical_bundle):
        return "geography"
    return "management"


def _format_delta(start: Optional[float], end: Optional[float]) -> str:
    if start is None or end is None:
        return "-"
    return format_pct(end - start, signed=True)


def generate_historical_insights(
    historical_cube: list[dict[str, Any]],
    latest_bundle: dict[str, Any],
    money_symbol: str,
    profit_margin_label: str = "净利率",
) -> list[dict[str, str]]:
    first = historical_cube[0]
    latest = historical_cube[-1]
    midpoint = historical_cube[-5] if len(historical_cube) >= 5 else latest
    guidance = latest_bundle["guidance"]
    cumulative_growth_pct = None
    if first.get("revenue_bn") not in (None, 0) and latest.get("revenue_bn") is not None:
        cumulative_growth_pct = (float(latest["revenue_bn"]) / float(first["revenue_bn"]) - 1) * 100
    first_to_latest_summary = (
        f"总收入从 {format_money_bn(first.get('revenue_bn'), money_symbol)} 抬升到 "
        f"{format_money_bn(latest.get('revenue_bn'), money_symbol)}，累计变化 "
        f"{format_pct(cumulative_growth_pct, signed=True)}。"
        if cumulative_growth_pct is not None
        else "当前 12 季窗口内缺少首尾完整收入口径，因此本页保留已披露季度的趋势与结构说明，不伪造累计增幅。"
    )
    midpoint_summary = (
        f"与 4 季前相比，单季收入由 {format_money_bn(midpoint.get('revenue_bn'), money_symbol)} "
        f"增至 {format_money_bn(latest.get('revenue_bn'), money_symbol)}。"
        if midpoint.get("revenue_bn") is not None and latest.get("revenue_bn") is not None
        else "部分历史季度收入序列暂缺，最近四季比较改由已接入的 TTM 与利润质量指标辅助阅读。"
    )
    insights = [
        {
            "title": "12 季总量抬升",
            "body": first_to_latest_summary,
            "evidence": f"最新季度收入同比 {format_pct(latest.get('revenue_yoy_pct'))}。",
        },
        {
            "title": "近四季成长节奏",
            "body": (
                f"最近四季 TTM 收入为 {format_money_bn(latest.get('ttm_revenue_bn'), money_symbol)}，"
                f"TTM 增速 {format_pct(latest.get('ttm_revenue_growth_pct'))}。"
            ),
            "evidence": midpoint_summary,
        },
        {
            "title": "利润质量变化",
            "body": (
                f"毛利率 12 季变化 {_format_delta(first.get('gross_margin_pct'), latest.get('gross_margin_pct'))}，"
                f"{profit_margin_label}变化 {_format_delta(first.get('net_margin_pct'), latest.get('net_margin_pct'))}。"
            ),
            "evidence": (
                f"当前毛利率 {format_pct(latest.get('gross_margin_pct'))}，"
                f"当前{profit_margin_label} {format_pct(latest.get('net_margin_pct'))}。"
            ),
        },
    ]
    if latest.get("segments") and first.get("segments"):
        first_top = max(first["segments"], key=lambda item: float(item["share_pct"]))
        latest_top = max(latest["segments"], key=lambda item: float(item["share_pct"]))
        insights.append(
            {
                "title": "结构集中度变化",
                "body": (
                    f"头部分部由 {first_top['name']} {format_pct(float(first_top['share_pct']))} "
                    f"变化到 {latest_top['name']} {format_pct(float(latest_top['share_pct']))}。"
                ),
                "evidence": "结构迁移页展示了头部分部权重的连续变化。",
            }
        )
    elif latest_bundle["structure_dimension"] == "geography":
        insights.append(
            {
                "title": "结构页按地区维度展示",
                "body": "由于历史业务分部披露不连续，结构迁移与增量贡献页已自动切换到地区收入结构。",
                "evidence": "报告已在结构页与圆环图中明确标注当前采用地区口径。",
            }
        )
    elif latest_bundle["structure_dimension"] != "segment":
        insights.append(
            {
                "title": "结构页按管理层口径降级",
                "body": "由于缺少连续 12 季业务分部披露，结构迁移与增量贡献页改看总量成长与结构边界说明。",
                "evidence": "报告已在结构页和附录页明确标注当前披露限制。",
            }
        )
    if latest.get("roe_pct") is not None or first.get("roe_pct") is not None:
        insights.append(
            {
                "title": "资本效率与兑现",
                "body": (
                    f"ROE 从 {format_pct(first.get('roe_pct'))} 变化到 {format_pct(latest.get('roe_pct'))}，"
                    "说明利润兑现与资本效率在同步变化。"
                ),
                "evidence": f"最新季度 ROE {format_pct(latest.get('roe_pct'))}。",
            }
        )
    else:
        insights.append(
            {
                "title": "利润兑现以利润率为主线",
                "body": "当前样本未稳定接入 ROE，因此盈利质量页主要用毛利率、净利率与收入同比判断经营杠杆。",
                "evidence": "这属于结构化季度财务序列的已知覆盖边界。",
            }
        )
    if guidance.get("mode") == "official":
        insights.append(
            {
                "title": "指引延续性",
                "body": _excerpt_text(str(guidance.get("commentary") or ""), 180),
                "evidence": f"下一季收入指引 {format_money_bn(guidance.get('revenue_bn'), money_symbol)}。",
            }
        )
    elif guidance.get("mode") == "official_context":
        insights.append(
            {
                "title": "管理层展望口径",
                "body": _excerpt_text(str(guidance.get("commentary") or ""), 180),
                "evidence": "公司未给出数值 top-line 指引，因此本页以官方表述叠加经营基线阅读。",
            }
        )
    else:
        insights.append(
            {
                "title": "经营基线对照",
                "body": _excerpt_text(str(guidance.get("commentary") or ""), 180),
                "evidence": f"基线收入 {format_money_bn(guidance.get('revenue_bn'), money_symbol)}。",
            }
        )
    return insights[:6]


def _keyword_topics(text: str, *, note: str = "来自手动上传 transcript 的关键词聚类。") -> list[dict[str, Any]]:
    lowered = text.lower()
    topic_map = {
        "AI 需求": ["ai", "inference", "training", "accelerator"],
        "指引与订单": ["guidance", "outlook", "order", "demand"],
        "供给与交付": ["supply", "yield", "ship", "production"],
        "利润率": ["margin", "gross margin", "ebitda"],
        "软件与平台": ["software", "vmware", "platform"],
    }
    topics = []
    for label, keywords in topic_map.items():
        score = sum(lowered.count(keyword) for keyword in keywords) * 12
        if score > 0:
            topics.append({"label": label, "score": min(100, score), "note": note})
    return sorted(topics, key=lambda item: item["score"], reverse=True)[:4]


_TOPIC_NOTE_PLACEHOLDER_MARKERS = (
    "关键词聚类",
    "来自自动抓取的官方电话会材料",
    "来自官方补充材料",
    "来自手动上传 transcript",
)


def _topic_label_key(label: Any) -> str:
    normalized = re.sub(r"[\W_]+", "", str(label or "").casefold())
    return normalized.strip()


def _topic_note_is_placeholder(note: Any) -> bool:
    cleaned = re.sub(r"\s+", " ", str(note or "")).strip()
    if not cleaned:
        return True
    if any(marker in cleaned for marker in _TOPIC_NOTE_PLACEHOLDER_MARKERS):
        return True
    if "..." in cleaned or "…" in cleaned:
        return True
    if re.search(r"(?:同比|环比)\s*[-–—]", cleaned):
        return True
    if cleaned.endswith(("...", "…", "-", "—", "–", ":", "：", ",", "，")):
        return True
    return not _text_looks_human_readable(cleaned, min_readable_terms=3)


def _placeholder_topic_count(topics: list[dict[str, Any]]) -> int:
    return sum(
        1
        for item in list(topics or [])
        if isinstance(item, dict) and _topic_note_is_placeholder(item.get("note"))
    )


def _payload_contains_placeholder_topic_cards(payload: dict[str, Any]) -> bool:
    qna_topics = [dict(item) for item in list(payload.get("qna_themes") or []) if isinstance(item, dict)]
    management_themes = [dict(item) for item in list(payload.get("management_themes") or []) if isinstance(item, dict)]
    return _placeholder_topic_count(qna_topics) > 0 or _placeholder_topic_count(management_themes) > 0


def _stabilize_topic_cards(
    topics: list[dict[str, Any]],
    fallback_topics: list[dict[str, Any]],
    *,
    minimum: int = 4,
) -> list[dict[str, Any]]:
    primary_items = [dict(item) for item in list(topics or []) if isinstance(item, dict)]
    fallback_items = [dict(item) for item in list(fallback_topics or []) if isinstance(item, dict)]
    rich_by_label: dict[str, dict[str, Any]] = {}
    rich_fallbacks: list[dict[str, Any]] = []
    for item in fallback_items:
        label = str(item.get("label") or "").strip()
        note = str(item.get("note") or "").strip()
        if not label or _topic_note_is_placeholder(note):
            continue
        key = _topic_label_key(label)
        if key and key not in rich_by_label:
            rich_by_label[key] = dict(item)
        rich_fallbacks.append(dict(item))

    target_size = max(minimum, sum(1 for item in primary_items if str(item.get("label") or "").strip()))
    stabilized: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    for item in primary_items:
        label = str(item.get("label") or "").strip()
        if not label or label in seen_labels:
            continue
        candidate = dict(item)
        note = str(candidate.get("note") or "").strip()
        if _topic_note_is_placeholder(note):
            replacement = rich_by_label.get(_topic_label_key(label))
            if replacement is None:
                continue
            candidate["note"] = replacement.get("note")
            if candidate.get("score") is None and replacement.get("score") is not None:
                candidate["score"] = replacement.get("score")
        if _topic_note_is_placeholder(candidate.get("note")):
            continue
        stabilized.append(candidate)
        seen_labels.add(label)

    for item in rich_fallbacks:
        if len(stabilized) >= target_size:
            break
        label = str(item.get("label") or "").strip()
        if not label or label in seen_labels:
            continue
        stabilized.append(dict(item))
        seen_labels.add(label)
    return stabilized[:target_size]


def _ensure_minimum_qna_topics(
    qna_topics: list[dict[str, Any]],
    management_themes: list[dict[str, Any]],
    risks: list[dict[str, Any]],
    catalysts: list[dict[str, Any]],
    *,
    minimum: int = 3,
) -> list[dict[str, Any]]:
    enriched = [dict(item) for item in list(qna_topics or []) if isinstance(item, dict)]
    seen = {str(item.get("label") or "").strip() for item in enriched if str(item.get("label") or "").strip()}
    for pool, prefix in (
        (management_themes, "延伸关注"),
        (risks, "风险追问"),
        (catalysts, "催化验证"),
    ):
        for item in list(pool or []):
            if len(enriched) >= minimum:
                break
            label = str(item.get("label") or "").strip()
            note = str(item.get("note") or "").strip()
            if not label or not note:
                continue
            candidate_label = f"{prefix}：{label}"
            if candidate_label in seen:
                continue
            enriched.append(
                {
                    "label": candidate_label,
                    "score": float(item.get("score") or 70),
                    "note": note,
                }
            )
            seen.add(candidate_label)
        if len(enriched) >= minimum:
            break
    return enriched


def _transcript_summary(upload_id: Optional[str]) -> Optional[dict[str, Any]]:
    if not upload_id:
        return None
    upload = get_upload(upload_id)
    if upload is None:
        return None
    text = upload["extracted_text"].strip()
    if not _text_looks_human_readable(text, min_readable_terms=6):
        return None
    paragraphs = [chunk.strip() for chunk in text.splitlines() if chunk.strip()]
    highlights: list[str] = []
    for paragraph in paragraphs:
        if _append_unique_highlight(highlights, paragraph, limit=3):
            break
    if not highlights:
        highlights = _transcript_highlights(text)
    topics = _keyword_topics(text)
    return {
        "upload_id": upload_id,
        "source_type": "manual_transcript",
        "filename": upload["filename"],
        "highlights": highlights[:3],
        "topics": topics,
    }


def _transcript_chunks(text: str) -> list[str]:
    normalized = str(text or "").replace("\r", "\n")
    chunks = [
        re.sub(r"\s+", " ", chunk).strip()
        for chunk in re.split(r"\n\s*\n+", normalized)
        if re.sub(r"\s+", " ", chunk).strip()
    ]
    if len(chunks) >= 3:
        return chunks
    lines = [re.sub(r"\s+", " ", line).strip() for line in normalized.splitlines() if re.sub(r"\s+", " ", line).strip()]
    merged: list[str] = []
    bucket: list[str] = []
    for line in lines:
        bucket.append(line)
        if len(" ".join(bucket)) >= 220:
            merged.append(" ".join(bucket))
            bucket = []
    if bucket:
        merged.append(" ".join(bucket))
    return merged


def _highlight_candidate_is_readable(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) < 24:
        return False
    if not _text_looks_human_readable(normalized, min_readable_terms=3):
        return False
    lowered = normalized.lower()
    url_matches = re.findall(r"https?://\S+", normalized)
    taxonomy_tokens = re.findall(r"\b[a-z0-9._-]+:[A-Za-z0-9._-]+\b", normalized, flags=re.IGNORECASE)
    date_tokens = re.findall(r"\b20\d{2}-\d{2}-\d{2}\b", normalized)
    slash_hash_tokens = [
        token
        for token in normalized.split()
        if token.startswith(("http://", "https://")) or token.count("/") >= 2 or "#" in token
    ]
    code_like_tokens = re.findall(r"\b[A-Z0-9._-]{6,}\b", normalized)
    readable_terms = re.findall(r"[A-Za-z\u4e00-\u9fff]{2,}", normalized)
    if len(url_matches) >= 2:
        return False
    if "fasb.org/us-gaap" in lowered or "ifrs-full" in lowered:
        return False
    if len(taxonomy_tokens) >= 2:
        return False
    if len(date_tokens) >= 3 and len(readable_terms) < 10:
        return False
    if len(slash_hash_tokens) >= 3 and len(slash_hash_tokens) >= max(2, len(normalized.split()) // 4):
        return False
    if len(code_like_tokens) >= 5 and len(readable_terms) < 8:
        return False
    return True


def _normalize_highlight_candidate(text: str, *, max_length: int = 220) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized or not _highlight_candidate_is_readable(normalized):
        return ""
    return _excerpt_text(normalized, max_length)


def _append_unique_highlight(highlights: list[str], candidate: str, *, limit: int = 3) -> bool:
    normalized = _normalize_highlight_candidate(candidate)
    if not normalized or normalized in highlights:
        return len(highlights) >= limit
    highlights.append(normalized)
    return len(highlights) >= limit


def _looks_like_transcript_text(text: str, label: str = "") -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    lowered = normalized.lower()
    label_lower = str(label or "").lower()
    if not _text_looks_human_readable(normalized, min_readable_terms=6):
        return False
    registration_tokens = (
        "already registered",
        "log in now",
        "complete this form to enter the webcast",
        "enter the webcast",
        "register now",
        "system test",
        "not registered",
        "email:",
    )
    transcript_tokens = (
        "question-and-answer",
        "question and answer",
        "prepared remarks",
        "conference call",
        "earnings call",
        "operator",
        "analyst",
    )
    signal_count = sum(1 for token in transcript_tokens if token in lowered)
    speaker_count = len(
        re.findall(
            r"\b(?:Operator|Analyst|Question(?:er)?|CEO|CFO|COO|President|Chief Executive Officer|Chief Financial Officer)\b",
            normalized,
            flags=re.IGNORECASE,
        )
    )
    if any(token in label_lower for token in ("transcript", "prepared remarks")):
        return signal_count >= 1 or speaker_count >= 2
    registration_signal_count = sum(1 for token in registration_tokens if token in lowered)
    if registration_signal_count >= 2 and speaker_count < 4:
        return False
    return signal_count >= 2 or speaker_count >= 4


def _transcript_highlights(text: str) -> list[str]:
    chunks = _transcript_chunks(text)
    if not chunks:
        return []
    skip_tokens = (
        "forward-looking statements",
        "reconciliation of gaap",
        "all lines have been placed on mute",
        "i would now like to turn",
        "thank you. good afternoon",
        "this call will be recorded",
        "good afternoon and thank you for joining us",
        "thanks for joining us today",
        "welcome to the earnings conference call",
        "on the call with me are",
    )
    preferred: list[str] = []
    fallback: list[str] = []
    business_tokens = (
        "revenue",
        "growth",
        "demand",
        "margin",
        "orders",
        "guidance",
        "ai",
        "cloud",
        "advertising",
        "deliver",
        "production",
        "video",
        "instagram",
        "capacity",
    )
    for chunk in chunks:
        lowered = chunk.lower()
        if any(token in lowered for token in skip_tokens):
            continue
        intro_count = sum(token in lowered for token in ("good afternoon", "thank you for joining us", "welcome to"))
        title_count = sum(
            token in lowered
            for token in (
                "chief executive officer",
                "chief financial officer",
                "chief accounting officer",
                "president and ceo",
            )
        )
        if intro_count >= 1 and title_count >= 2:
            continue
        if len(chunk) < 80:
            continue
        if any(token in lowered for token in business_tokens):
            preferred.append(chunk)
        else:
            fallback.append(chunk)
    selected = preferred[:3] if preferred else fallback[:3]
    return [_excerpt_text(item, 190) for item in selected[:3]]


def _automatic_transcript_summary(source_materials: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    candidates: list[tuple[int, int, dict[str, Any], str]] = []
    for item in source_materials:
        if item.get("status") not in {"fetched", "cached"}:
            continue
        label = str(item.get("label") or "")
        label_lower = label.lower()
        role = str(item.get("role") or "")
        kind = str(item.get("kind") or "")
        call_like_material = item.get("kind") == "call_summary" or item.get("role") == "earnings_call"
        if not call_like_material and any(
            token in label_lower
            for token in ("transcript", "prepared remarks", "conference call", "earnings call", "webcast replay", "call replay", "script")
        ):
            call_like_material = True
        if not call_like_material and role in {"earnings_commentary", "earnings_presentation"} and kind in {"presentation", "sec_filing"}:
            call_like_material = True
        if not call_like_material:
            continue
        text_path = str(item.get("text_path") or "").strip()
        if not text_path or not Path(text_path).exists():
            continue
        text = Path(text_path).read_text(encoding="utf-8", errors="ignore").strip()
        if len(text) < 400:
            continue
        if not _text_looks_human_readable(text, min_readable_terms=6):
            continue
        if not _looks_like_transcript_text(text, label):
            continue
        label_score = 3 if "transcript" in label.lower() else 2 if "prepared remarks" in label.lower() else 1
        candidates.append((label_score, len(text), item, text))
    if not candidates:
        return None
    _label_score, _text_len, best_item, best_text = max(candidates, key=lambda row: (row[0], row[1]))
    highlights = _transcript_highlights(best_text)
    topics = _keyword_topics(best_text, note="来自自动抓取的官方电话会材料关键词聚类。")
    if not highlights and not topics:
        return None
    return {
        "source_type": "official_call_material",
        "filename": str(best_item.get("label") or "Official earnings call"),
        "highlights": highlights[:3],
        "topics": topics,
    }


def _official_material_proxy_summary(
    fixture: dict[str, Any],
    source_materials: list[dict[str, Any]],
    qna_topics: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    extracted = [
        item
        for item in list(source_materials or [])
        if item.get("status") in {"fetched", "cached"} and int(item.get("text_length") or 0) > 0
    ]
    if not extracted:
        return None
    def _proxy_material_score(item: dict[str, Any]) -> tuple[int, int]:
        kind = str(item.get("kind") or "")
        role = str(item.get("role") or "")
        label = str(item.get("label") or "").lower()
        title = str(item.get("title") or "").lower()
        combined = f"{label} {title}"
        text_length = int(item.get("text_length") or 0)
        score = 0
        if role == "earnings_call" or kind == "call_summary":
            score += 7
        if any(token in combined for token in ("prepared remarks", "transcript", "conference call", "earnings call", "commentary", "supplement")):
            score += 5
        if any(token in combined for token in ("99.2", "ex-99.2", "exhibit992", "supplement", "supplemental information")):
            score += 6
        if kind == "presentation" and role == "earnings_commentary":
            score += 4
        if kind in {"official_release", "presentation", "sec_filing"}:
            score += 2
        if any(
            token in combined
            for token in ("registration", "log in", "already registered", "complete this form to enter the webcast", "not registered")
        ):
            score -= 12
        if text_length >= 5000:
            score += 4
        elif text_length < 1200:
            score -= 4
        return (score, text_length)

    preferred_material = max(extracted, key=_proxy_material_score)
    preferred_text = ""
    preferred_text_path = str(preferred_material.get("text_path") or "").strip()
    if preferred_text_path and Path(preferred_text_path).exists():
        preferred_text = Path(preferred_text_path).read_text(encoding="utf-8", errors="ignore").strip()

    highlights: list[str] = []
    if preferred_text:
        if _text_looks_human_readable(preferred_text, min_readable_terms=6) and _looks_like_transcript_text(preferred_text, str(preferred_material.get("label") or "")):
            for item in _transcript_highlights(preferred_text):
                if _append_unique_highlight(highlights, item, limit=2):
                    break
        if len(highlights) < 2 and _text_looks_human_readable(preferred_text, min_readable_terms=4):
            for chunk in _transcript_chunks(preferred_text):
                lowered_chunk = chunk.lower()
                if len(chunk) < 90:
                    continue
                if any(token in lowered_chunk for token in ("revenue", "margin", "guidance", "demand", "traffic", "comparable", "cash flow", "operating income")):
                    if _append_unique_highlight(highlights, chunk, limit=2):
                        break
                if len(highlights) >= 2:
                    break
    for quote in list(fixture.get("call_quote_cards") or []):
        text = re.sub(r"\s+", " ", str(quote.get("quote") or "")).strip()
        if _append_unique_highlight(highlights, text, limit=2):
            break
    for topic in list(qna_topics or []):
        note = re.sub(r"\s+", " ", str(topic.get("note") or "")).strip()
        if _append_unique_highlight(highlights, note, limit=3):
            break
    if not highlights:
        for card in list(fixture.get("evidence_cards") or []):
            note = re.sub(r"\s+", " ", str(card.get("detail") or card.get("note") or "")).strip()
            if _append_unique_highlight(highlights, note, limit=3):
                break
    if not highlights:
        material_label = str(preferred_material.get("label") or "官方材料").strip()
        for fallback in (
            f"这季没有完整电话会实录，所以这页改从 {material_label} 里抓管理层最在意的经营线索。",
            "先盯收入方向、利润率和管理层反复强调的业务主线，不要被杂音带跑。",
        ):
            if _append_unique_highlight(highlights, fallback, limit=2):
                break

    topics: list[dict[str, Any]] = []
    if preferred_text:
        for item in _keyword_topics(preferred_text, note="来自官方补充材料的关键词聚类。"):
            label = str(item.get("label") or "").strip()
            if not label:
                continue
            topics.append(
                {
                    "label": label,
                    "score": float(item.get("score") or 72),
                    "note": str(item.get("note") or "来自官方补充材料的关键词聚类。").strip(),
                }
            )
            if len(topics) >= 2:
                break
    seen_labels: set[str] = set()
    seen_labels.update(str(item.get("label") or "").strip() for item in topics if str(item.get("label") or "").strip())
    for item in list(qna_topics or []) + list(fixture.get("management_themes") or []):
        label = str(item.get("label") or "").strip()
        note = str(item.get("note") or "").strip()
        if not label or label in seen_labels:
            continue
        topics.append(
            {
                "label": label,
                "score": float(item.get("score") or 68),
                "note": note or "基于官方 release / deck / filing 构建的电话会代理主题。",
            }
        )
        seen_labels.add(label)
        if len(topics) >= 4:
            break
    if not highlights and not topics:
        return None
    return {
        "source_type": "official_material_proxy",
        "filename": str(preferred_material.get("label") or "Official earnings materials"),
        "highlights": [_writer_polish_generated_text(item) for item in highlights[:3]],
        "topics": topics[:4],
    }


def _material_scores(
    fixture: dict[str, Any],
    structure_dimension: str,
    transcript_summary: Optional[dict[str, Any]],
    source_materials: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    materials = fixture["materials"]
    source_materials = source_materials or []
    extracted = [item for item in source_materials if item.get("status") in {"fetched", "cached"} and item.get("text_length", 0) > 0]
    has_release = any(item.get("kind") == "official_release" and item.get("status") in {"fetched", "cached"} for item in source_materials)
    has_presentation = any(item.get("kind") == "presentation" and item.get("status") in {"fetched", "cached"} for item in source_materials)
    crawl_score = 26
    if extracted:
        crawl_score = 100 if len(extracted) == len(source_materials) else 74
    transcript_source_type = str((transcript_summary or {}).get("source_type") or "")
    if transcript_source_type in {"manual_transcript", "official_call_material"}:
        call_material_score = 100
    elif transcript_source_type == "official_material_proxy":
        call_material_score = 74
    else:
        call_material_score = 42 if "call_summary" in materials else 18
    return [
        {"label": "官方财报新闻稿", "score": 100 if has_release or "earnings_release" in materials else 32},
        {"label": "演示材料", "score": 92 if has_presentation or "presentation" in materials else 36},
        {"label": "电话会信息", "score": call_material_score},
        {"label": "原文抓取与解析", "score": crawl_score if source_materials else 18},
    ]


def _merge_sources_with_materials(
    sources: list[dict[str, Any]],
    source_materials: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    material_lookup = {
        (str(item.get("url") or ""), str(item.get("label") or "")): item
        for item in source_materials
    }
    merged: list[dict[str, Any]] = []
    for source in sources:
        material = material_lookup.get((str(source.get("url") or ""), str(source.get("label") or "")))
        merged.append({**source, "material": material})
    return merged


def _parse_calendar_quarter_token(value: Optional[str]) -> Optional[tuple[int, int]]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(value or "").strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _shift_calendar_quarter(value: Optional[str], delta: int) -> Optional[str]:
    parsed = _parse_calendar_quarter_token(value)
    if parsed is None:
        return None
    year, quarter = parsed
    ordinal = year * 4 + (quarter - 1) + int(delta)
    if ordinal < 0:
        return None
    shifted_year, shifted_index = divmod(ordinal, 4)
    return f"{shifted_year}Q{shifted_index + 1}"


def _source_material_is_ready(source: Optional[dict[str, Any]]) -> bool:
    material = (source or {}).get("material") or {}
    return str(material.get("status") or "") in {"fetched", "cached"}


def _pick_best_source(
    sources: list[dict[str, Any]],
    *,
    preferred_kinds: tuple[str, ...] = (),
    preferred_roles: tuple[str, ...] = (),
) -> Optional[dict[str, Any]]:
    if not sources:
        return None

    def score(source: dict[str, Any]) -> tuple[int, int, int, int, str]:
        kind = str(source.get("kind") or "")
        role = str(source.get("role") or "")
        label = str(source.get("label") or "").strip().lower()
        kind_rank = len(preferred_kinds) - preferred_kinds.index(kind) if kind in preferred_kinds else 0
        role_rank = len(preferred_roles) - preferred_roles.index(role) if role in preferred_roles else 0
        ready_rank = 2 if _source_material_is_ready(source) else 1 if source.get("url") else 0
        readable_label_rank = 0 if any(token in label for token in (".htm", ".html", ".txt", ".pdf", "ex99", "8-k")) else 1
        date_rank = 0
        parsed_date = _parse_iso_date(str(source.get("date") or ""))
        if parsed_date is not None:
            date_rank = parsed_date.toordinal()
        return kind_rank, role_rank, ready_rank, readable_label_rank, f"{date_rank:08d}"

    return max(sources, key=score, default=None)


def _format_source_anchor(source: Optional[dict[str, Any]], fallback: str = "结构化季度财务序列") -> str:
    if not source:
        return fallback
    label = str(source.get("label") or "").strip()
    material = source.get("material") or {}
    material_title = str(material.get("title") or "").strip()
    generic_titles = {"document", "source", "material", "filing"}
    if (
        material_title
        and material_title.casefold() not in generic_titles
        and (not label or ".htm" in label.lower() or len(material_title) + 12 < len(label))
    ):
        label = material_title
    if not label:
        label = fallback
    date_text = str(source.get("date") or "").strip()
    return f"{label} | {date_text}" if date_text else label


def _guidance_mode_label(mode: str) -> str:
    return {
        "official": "官方数值指引",
        "official_context": "官方展望语境",
        "proxy": "经营基线对照",
    }.get(str(mode or "proxy"), "经营口径")


def _guidance_display_mode(guidance: dict[str, Any]) -> str:
    mode = str(guidance.get("mode") or "proxy")
    if guidance.get("revenue_derived_from_baseline"):
        return "proxy"
    return mode


def _guidance_snapshot_title(period_label: str, mode: str, *, current: bool) -> str:
    label = period_label or ("本次" if current else "上一季")
    normalized_mode = str(mode or "proxy")
    if normalized_mode == "official":
        return f"{label} 给出的本季口径" if not current else "本次新的下一阶段口径"
    if normalized_mode == "official_context":
        return f"{label} 给出的本季官方展望" if not current else "本次新的下一阶段官方展望"
    return f"{label} 的可比经营基线" if not current else "本次新的下一阶段经营基线"


def _guidance_snapshot_anchor(previous_label: str, current_label: str, previous_mode: str, current_mode: str) -> str:
    mode_set = {str(previous_mode or "proxy"), str(current_mode or "proxy")}
    if mode_set == {"official"}:
        return f"对照 {previous_label} 与 {current_label} 两期管理层口径"
    if "official_context" in mode_set or "official" in mode_set:
        return f"对照 {previous_label} 与 {current_label} 两期官方展望语境及经营基线"
    return f"对照 {previous_label} 与 {current_label} 两期经营基线"


def _guidance_display_value(
    guidance: dict[str, Any],
    money_symbol: str,
) -> str:
    mode = str(guidance.get("mode") or "proxy")
    commentary = _clean_summary_fragment(guidance.get("commentary"))
    revenue_low = guidance.get("revenue_low_bn")
    revenue_high = guidance.get("revenue_high_bn")
    revenue_bn = guidance.get("revenue_bn")
    if revenue_low is not None and revenue_high is not None:
        return f"{format_money_bn(revenue_low, money_symbol)}-{format_money_bn(revenue_high, money_symbol)}"
    if revenue_bn is not None:
        return format_money_bn(revenue_bn, money_symbol)
    gross_margin = guidance.get("gaap_gross_margin_pct")
    if gross_margin is not None:
        return format_pct(gross_margin)
    adjusted_margin = guidance.get("adjusted_ebitda_margin_pct")
    if adjusted_margin is not None:
        return format_pct(adjusted_margin)
    if mode == "official_context":
        return "官方展望" if commentary else "未披露"
    if mode == "proxy":
        return "未披露"
    return "经营口径"


def _guidance_display_note(guidance: dict[str, Any], fallback_note: str) -> str:
    revenue_low = guidance.get("revenue_low_bn")
    revenue_high = guidance.get("revenue_high_bn")
    revenue_bn = guidance.get("revenue_bn")
    commentary = _clean_summary_fragment(guidance.get("commentary"))
    revenue_derived_from_baseline = bool(guidance.get("revenue_derived_from_baseline"))
    mode = _guidance_display_mode(guidance)
    if revenue_derived_from_baseline and revenue_bn is not None:
        return "经营基线对照（非官方数值指引）"
    if revenue_low is not None and revenue_high is not None:
        return _guidance_mode_label(mode or "official")
    if revenue_bn is not None:
        return _guidance_mode_label(mode or "official")
    if commentary:
        return _excerpt_text(commentary, 54)
    return fallback_note


def _layered_takeaway_anchor(
    title: str,
    *,
    merged_sources: list[dict[str, Any]],
    guidance: dict[str, Any],
) -> tuple[str, str]:
    if title == "发生了什么":
        source = _pick_best_source(
            merged_sources,
            preferred_kinds=("official_release", "sec_filing", "presentation", "structured_financials"),
            preferred_roles=("earnings_release",),
        )
        return _format_source_anchor(source), "收入、净利润与同比/环比"
    if title == "为什么重要":
        source = _pick_best_source(
            merged_sources,
            preferred_kinds=("sec_filing", "presentation", "official_release", "structured_financials"),
            preferred_roles=("earnings_release",),
        )
        return _format_source_anchor(source), "利润率、头部结构占比与近四季中枢"
    if guidance.get("mode") == "official":
        source = _pick_best_source(
            merged_sources,
            preferred_kinds=("official_release", "sec_filing", "presentation"),
            preferred_roles=("earnings_call", "earnings_release"),
        )
        return _format_source_anchor(source), "下一阶段收入口径与管理层语气"
    if guidance.get("mode") == "official_context":
        source = _pick_best_source(
            merged_sources,
            preferred_kinds=("official_release", "presentation", "sec_filing"),
            preferred_roles=("earnings_call", "earnings_release"),
        )
        return _format_source_anchor(source), "官方展望语境与经营基线"
    source = _pick_best_source(
        merged_sources,
        preferred_kinds=("structured_financials", "official_release", "sec_filing"),
    )
    return _format_source_anchor(source), "经营基线、收入中枢与利润率方向"


def _build_guidance_change_panel(
    company_id: str,
    calendar_quarter: str,
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
    *,
    allow_expensive_parse: bool = True,
) -> dict[str, Any]:
    guidance = dict(fixture.get("guidance") or {})
    latest_kpis = dict(fixture.get("latest_kpis") or {})
    current_mode = str(guidance.get("mode") or "proxy")
    current_revenue = None if guidance.get("revenue_derived_from_baseline") else guidance.get("revenue_bn")
    current_low = guidance.get("revenue_low_bn")
    current_high = guidance.get("revenue_high_bn")
    current_margin = guidance.get("gaap_gross_margin_pct") or guidance.get("adjusted_ebitda_margin_pct")
    current_actual = _value_or_history(latest_kpis.get("revenue_bn"), history[-1].get("revenue_bn") if history else None)
    previous_actual = history[-2].get("revenue_bn") if len(history) > 1 else None
    previous_quarter = _shift_calendar_quarter(calendar_quarter, -1)
    previous_snapshot = (
        _resolve_quarter_guidance_snapshot(
            company_id,
            previous_quarter,
            history,
            allow_expensive_parse=allow_expensive_parse,
        )
        if previous_quarter
        else None
    )
    bullets: list[str] = []
    source_anchor = ""

    if previous_snapshot is not None:
        previous_guidance = dict(previous_snapshot.get("guidance") or {})
        previous_mode = str(previous_guidance.get("mode") or "proxy")
        previous_revenue = previous_guidance.get("revenue_bn")
        previous_low = previous_guidance.get("revenue_low_bn")
        previous_high = previous_guidance.get("revenue_high_bn")
        previous_margin = previous_guidance.get("gaap_gross_margin_pct") or previous_guidance.get("adjusted_ebitda_margin_pct")
        previous_label = str(previous_snapshot.get("fiscal_label") or previous_quarter)
        current_label = str(fixture.get("fiscal_label") or calendar_quarter)
        source_anchor = _guidance_snapshot_anchor(previous_label, current_label, previous_mode, current_mode)

        if previous_revenue is not None and current_actual not in (None, 0):
            realized_delta = (float(current_actual) / float(previous_revenue) - 1) * 100
            if previous_mode == "official":
                bullets.append(
                    f"{previous_label} 时管理层给出的本季收入口径约 {format_money_bn(previous_revenue, money_symbol)}，"
                    f"本季实际为 {format_money_bn(current_actual, money_symbol)}，兑现 {format_pct(realized_delta, signed=True)}。"
                )
            elif previous_mode == "official_context":
                bullets.append(
                    f"{previous_label} 的官方展望语境对应本季经营中枢约 {format_money_bn(previous_revenue, money_symbol)}，"
                    f"本季实际为 {format_money_bn(current_actual, money_symbol)}，相对中枢变化 {format_pct(realized_delta, signed=True)}。"
                )
            else:
                bullets.append(
                    f"{previous_label} 的可比经营基线约 {format_money_bn(previous_revenue, money_symbol)}，"
                    f"本季实际为 {format_money_bn(current_actual, money_symbol)}，相对基线变化 {format_pct(realized_delta, signed=True)}。"
                )
        elif previous_low is not None and previous_high is not None and current_actual is not None:
            bullets.append(
                f"{previous_label} 给出的本季收入区间为 {format_money_bn(previous_low, money_symbol)} 到 "
                f"{format_money_bn(previous_high, money_symbol)}，当前已可直接对照本季实际是否落在区间内。"
            )

        if current_revenue is not None and previous_revenue is not None:
            delta = (float(current_revenue) / float(previous_revenue) - 1) * 100
            if current_mode == "official" and previous_mode == "official":
                bullets.append(
                    f"本次下一阶段收入口径 {format_money_bn(current_revenue, money_symbol)}，相对上一季管理层给出的下一阶段口径 "
                    f"{format_money_bn(previous_revenue, money_symbol)} 变化 {format_pct(delta, signed=True)}。"
                )
            else:
                current_label_text = "下一阶段经营中枢" if current_mode == "official_context" else "下一阶段经营基线" if current_mode == "proxy" else "下一阶段收入口径"
                previous_label_text = "上一季经营中枢" if previous_mode == "official_context" else "上一季经营基线" if previous_mode == "proxy" else "上一季收入口径"
                bullets.append(
                    f"本次{current_label_text}约 {format_money_bn(current_revenue, money_symbol)}，相对{previous_label_text} "
                    f"{format_money_bn(previous_revenue, money_symbol)} 变化 {format_pct(delta, signed=True)}。"
                )
        elif current_low is not None and current_high is not None and previous_low is not None and previous_high is not None:
            current_mid = (float(current_low) + float(current_high)) / 2
            previous_mid = (float(previous_low) + float(previous_high)) / 2
            delta = (current_mid / previous_mid - 1) * 100 if previous_mid else 0.0
            bullets.append(
                f"本次收入指引区间中枢约 {format_money_bn(current_mid, money_symbol)}，上一季对应口径中枢约 "
                f"{format_money_bn(previous_mid, money_symbol)}，顺序变化 {format_pct(delta, signed=True)}。"
            )

        if current_mode != previous_mode:
            bullets.append(
                f"披露口径已从{_guidance_mode_label(previous_mode)}切换为{_guidance_mode_label(current_mode)}，"
                "说明管理层在这一季愿意给出的前瞻信息密度发生了变化。"
            )
        elif current_margin is not None and previous_margin is not None:
            margin_delta = float(current_margin) - float(previous_margin)
            bullets.append(
                f"利润率口径从 {format_pct(previous_margin)} 变化到 {format_pct(current_margin)}，"
                f"边际变化 {format_pct(margin_delta, signed=True)}。"
            )

    if not bullets:
        source_anchor = "当前以历史序列与本季官方材料做方向性对照"
        if current_revenue is not None and previous_actual not in (None, 0):
            sequential_delta = (float(current_revenue) / float(previous_actual) - 1) * 100
            bullets.append(
                f"上一自然季度实际收入为 {format_money_bn(previous_actual, money_symbol)}，当前下一阶段口径约 "
                f"{format_money_bn(current_revenue, money_symbol)}，顺序变化 {format_pct(sequential_delta, signed=True)}。"
            )
        bullets.append(
            "上一季官方指引缓存尚未就绪时，本页先对照上一季实际与当前管理层口径，判断方向是上修、持平还是收缩。"
        )
        if current_mode != "official":
            bullets.append(
                f"当前口径仍属于{_guidance_mode_label(current_mode)}，因此更适合看方向与语气，而不是把它当成精确市场一致预期。"
            )

    return {
        "title": "相对上一季口径变化",
        "bullets": bullets[:3],
        "source_anchor": source_anchor,
    }


def _signal_bucket(label: str, note: str) -> str:
    combined = f"{label} {note}".lower()
    if any(token in combined for token in ("利润", "毛利", "净利", "margin", "盈利", "费用", "杠杆")):
        return "margin"
    if any(token in combined for token in ("结构", "披露", "集中", "mix", "地区", "分部")):
        return "structure"
    return "growth"


def _build_signal_scenarios(
    items: list[dict[str, Any]],
    *,
    direction: str,
    latest: dict[str, Any],
    history: list[dict[str, Any]],
    guidance: dict[str, Any],
    source_anchor: str,
    money_symbol: str,
) -> list[dict[str, str]]:
    baseline = _build_generic_guidance(history)
    revenue_reference = guidance.get("revenue_bn") or baseline.get("revenue_bn") or latest.get("revenue_bn")
    margin_reference = (
        guidance.get("gaap_gross_margin_pct")
        or guidance.get("adjusted_ebitda_margin_pct")
        or baseline.get("gaap_gross_margin_pct")
        or latest.get("gross_margin_pct")
        or latest.get("net_margin_pct")
    )
    scenarios: list[dict[str, str]] = []
    for item in list(items or [])[:2]:
        label = str(item.get("label") or "").strip()
        note = re.sub(r"\s+", " ", str(item.get("note") or "")).strip()
        bucket = _signal_bucket(label, note)
        if direction == "risk":
            if bucket == "margin":
                trigger = f"若利润率重新掉到 {format_pct(margin_reference)} 下方，或费用率继续抬升。"
                verify = "重点看毛利率、经营利润率、费用率与现金兑现是否同步转弱。"
            elif bucket == "structure":
                trigger = "若头部业务或地区继续过度集中，而第二曲线没有接上。"
                verify = "重点看头部结构占比、次主线增速与管理层是否补充结构解释。"
            else:
                trigger = f"若下一阶段收入落到 {format_money_bn(revenue_reference, money_symbol)} 下方，或同比继续放缓。"
                verify = "重点看收入同比/环比、头部业务增速与需求相关原文表述。"
            impact = note or "若该风险兑现，收入中枢和利润预期都可能被下修。"
        else:
            if bucket == "margin":
                trigger = f"若利润率能够站稳在 {format_pct(margin_reference)} 附近并继续改善。"
                verify = "重点看毛利率、经营利润率、现金流与费用纪律是否同向改善。"
            elif bucket == "structure":
                trigger = "若头部业务继续增长，同时第二增长曲线开始接力。"
                verify = "重点看分部/地区结构迁移、增量贡献与管理层结构口径。"
            else:
                trigger = f"若下一阶段收入高于 {format_money_bn(revenue_reference, money_symbol)}，且主线业务继续提速。"
                verify = "重点看收入增速、头部业务体量与 TTM 趋势是否同步上修。"
            impact = note or "若该催化剂兑现，收入与利润中枢都有继续上修空间。"
        scenarios.append(
            {
                "label": label or ("主要风险" if direction == "risk" else "主要催化剂"),
                "trigger": trigger,
                "impact": impact,
                "verify": verify,
                "source_anchor": source_anchor,
            }
        )
    return scenarios


def _source_material_warnings(source_materials: list[dict[str, Any]]) -> list[str]:
    if not source_materials:
        return []
    extracted = [item for item in source_materials if item.get("status") in {"fetched", "cached"} and item.get("text_length", 0) > 0]
    fetched = [item for item in source_materials if item.get("status") == "fetched"]
    cached = [item for item in source_materials if item.get("status") == "cached"]
    failed = [item for item in source_materials if item.get("status") == "error"]
    disabled = [item for item in source_materials if item.get("status") == "disabled"]
    covered_role_keys = {
        (str(item.get("role") or ""), str(item.get("kind") or ""))
        for item in extracted
    }
    uncovered_failed = [
        item
        for item in failed
        if (str(item.get("role") or ""), str(item.get("kind") or "")) not in covered_role_keys
    ]
    warnings: list[str] = []
    if extracted:
        if fetched and cached:
            warnings.append(f"本次新抓取 {len(fetched)} 份、复用缓存 {len(cached)} 份官方材料，并已完成统一文本提取。")
        elif fetched:
            warnings.append(f"本次已自动抓取 {len(fetched)} 份官方材料并完成文本提取，可直接生成深度版报告。")
        else:
            warnings.append(f"本次已复用 {len(cached)} 份官方材料缓存，可继续沿缓存原文做自动化解析。")
    if uncovered_failed:
        warnings.append(f"{len(uncovered_failed)} 份关键源材料本次暂未抓取成功，报告仍以现有结构化口径与已接入材料生成。")
    elif failed and not extracted:
        warnings.append(f"{len(failed)} 份源材料本次暂未抓取成功，报告仍以现有结构化口径与已接入材料生成。")
    if disabled and not extracted:
        warnings.append("当前环境关闭了官方源材料自动抓取，仅使用内置口径与已有缓存生成报告。")
    return warnings


def _material_tokens_from_sources(
    fixture: dict[str, Any],
    source_materials: list[dict[str, Any]],
) -> list[str]:
    tokens = {
        str(item).strip()
        for item in list(fixture.get("materials") or [])
        if str(item).strip()
    }
    for item in source_materials:
        if item.get("status") not in {"fetched", "cached"}:
            continue
        has_text = int(item.get("text_length") or 0) > 0 or bool(str(item.get("text_path") or "").strip())
        if not has_text:
            continue
        for token in (item.get("kind"), item.get("role")):
            normalized = str(token or "").strip()
            if normalized:
                tokens.add(normalized)
    return sorted(tokens)


def _quote_cards_are_synthesized(cards: list[dict[str, Any]]) -> bool:
    if not cards:
        return False
    synthesized_speakers = {"Management context", "Guidance context"}
    return all(str(item.get("speaker") or "").strip() in synthesized_speakers for item in cards)


def _narrative_entry(status: str) -> dict[str, Any]:
    mapping = {
        "manual_transcript": {
            "label": "手动 transcript 原文",
            "detail": "问答主题直接来自手动上传的电话会 transcript。",
            "is_inferred": False,
        },
        "official_call_material": {
            "label": "官方电话会材料",
            "detail": "主题优先依据官方 transcript / call summary / commentary 提炼，不是静态模板。",
            "is_inferred": False,
        },
        "official_material_proxy": {
            "label": "官方材料代理摘要",
            "detail": "未拿到完整 transcript 时，系统会基于官方 release / deck / filing 生成电话会式摘要与问答主题。",
            "is_inferred": True,
        },
        "official_material_inferred": {
            "label": "官方财报材料推断",
            "detail": "当前未拿到完整问答时，系统根据官方 release / deck / filing 动态归纳。",
            "is_inferred": True,
        },
        "structured_fallback": {
            "label": "结构化财务序列 fallback",
            "detail": "当前缺少可解析官方原文材料，相关主题仍带有统一研究 fallback 性质。",
            "is_inferred": True,
        },
        "official_quote_excerpt": {
            "label": "官方原文引述",
            "detail": "锚点卡片直接取自官方材料中的管理层表述。",
            "is_inferred": False,
        },
        "synthesized_quote": {
            "label": "主题转写锚点",
            "detail": "缺少可直接引用原话时，系统会把主题或指引转写成阅读锚点。",
            "is_inferred": True,
        },
        "no_quote_excerpt": {
            "label": "暂无原话引述",
            "detail": "当前未抓到可直接引用的管理层原话，页面回退到证据卡片。",
            "is_inferred": True,
        },
    }
    return {"status": status, **mapping[status]}


def _build_narrative_provenance(
    fixture: dict[str, Any],
    source_materials: list[dict[str, Any]],
    transcript_summary: Optional[dict[str, Any]],
    qna_topics: list[dict[str, Any]],
) -> dict[str, Any]:
    del qna_topics
    materials = set(_material_tokens_from_sources(fixture, source_materials))
    extracted = [
        item
        for item in source_materials
        if item.get("status") in {"fetched", "cached"}
        and (int(item.get("text_length") or 0) > 0 or bool(str(item.get("text_path") or "").strip()))
    ]
    transcript_source_type = str((transcript_summary or {}).get("source_type") or "")
    has_call_material = transcript_source_type in {"official_call_material", "official_material_proxy"}
    has_official_material = bool(
        any(
            item.get("kind") in {"official_release", "presentation", "sec_filing"}
            for item in extracted
        )
        or any(token in materials for token in {"official_release", "presentation", "sec_filing", "earnings_release", "earnings_presentation", "earnings_commentary"})
    )
    quote_cards = list(fixture.get("call_quote_cards") or [])
    synthesized_quotes = _quote_cards_are_synthesized(quote_cards)

    return _writer_build_narrative_provenance(
        transcript_source_type=transcript_source_type,
        has_official_material=has_official_material,
        has_quote_cards=bool(quote_cards),
        synthesized_quotes=synthesized_quotes,
    )


def _reconcile_coverage_warnings(
    warnings: list[str],
    *,
    fixture: dict[str, Any],
    source_materials: list[dict[str, Any]],
    structure_dimension: str,
    institutional_views: list[dict[str, Any]],
) -> list[str]:
    del fixture
    extracted_release = any(
        item.get("kind") == "official_release" and item.get("status") in {"fetched", "cached"}
        for item in source_materials
    )
    if extracted_release:
        warnings = [
            item
            for item in warnings
            if "未内置该季度官方财报新闻稿摘要" not in item
        ]
    if structure_dimension == "segment":
        warnings = [
            item
            for item in warnings
            if "缺少连续 12 季分部披露" not in item
            and "结构迁移与增量贡献页将自动降级" not in item
            and "历史分部连续性不足" not in item
            and "由于缺少连续 12 季分部披露" not in item
        ]
    if institutional_views:
        warnings = [item for item in warnings if "未稳定抓到可追溯的机构观点条目" not in item]
    if structure_dimension == "geography":
        warnings = [
            item
            for item in warnings
            if "缺少连续 12 季分部披露" not in item
            and "结构迁移与增量贡献页将自动降级" not in item
            and "历史分部连续性不足" not in item
            and "由于缺少连续 12 季分部披露" not in item
        ]
        warnings.append("历史业务分部披露不足时，系统已自动切换为地区结构的 12 季迁移与增量分析。")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in warnings:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _has_merge_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _merge_fixture_payload(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if not _has_merge_value(value):
            continue
        if key == "coverage_notes" and isinstance(value, list):
            existing = list(merged.get("coverage_notes") or [])
            combined = []
            seen: set[str] = set()
            for note in list(value) + existing:
                normalized = str(note).strip()
                if not normalized or normalized in seen:
                    continue
                combined.append(normalized)
                seen.add(normalized)
            merged[key] = combined
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged[key])
            if key == "guidance":
                override_mode = str(value.get("mode") or nested.get("mode") or "")
                if override_mode in {"official", "official_context"}:
                    for inherited_key in (
                        "revenue_bn",
                        "revenue_low_bn",
                        "revenue_high_bn",
                        "gaap_gross_margin_pct",
                        "adjusted_ebitda_margin_pct",
                        "comparison_revenue_bn",
                        "comparison_margin_pct",
                        "current_label",
                        "comparison_label",
                        "current_margin_label",
                        "comparison_margin_label",
                        "revenue_derived_from_baseline",
                    ):
                        if inherited_key not in value:
                            nested.pop(inherited_key, None)
            for nested_key, nested_value in value.items():
                if _has_merge_value(nested_value):
                    nested[nested_key] = nested_value
            merged[key] = nested
            continue
        merged[key] = value
    return merged


def _refresh_latest_history_entry(
    company: dict[str, Any],
    history: list[dict[str, Any]],
    fixture: dict[str, Any],
) -> list[dict[str, Any]]:
    refreshed = [dict(entry) for entry in history]
    latest_index = len(refreshed) - 1
    latest = dict(refreshed[latest_index])
    latest_kpis = fixture.get("latest_kpis", {})

    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    if revenue_bn is not None:
        latest["revenue_bn"] = float(revenue_bn)
    profit_bn = _historical_profit_series_value(company, latest_kpis)
    if profit_bn is not None:
        latest["net_income_bn"] = float(profit_bn)
    gross_margin_pct = _metric_or_fallback(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct"))
    if gross_margin_pct is not None:
        latest["gross_margin_pct"] = float(gross_margin_pct)
    revenue_yoy_pct = _value_or_history(latest_kpis.get("revenue_yoy_pct"), latest.get("revenue_yoy_pct"))
    if revenue_yoy_pct is not None:
        latest["revenue_yoy_pct"] = float(revenue_yoy_pct)
    if latest_kpis.get("net_income_yoy_pct") is not None:
        latest["net_income_yoy_pct"] = float(latest_kpis["net_income_yoy_pct"])
    normalized_latest_equity_bn = _normalize_equity_bn_value(
        latest_kpis.get("ending_equity_bn"),
        reference_equity_bn=latest.get("equity_bn"),
        revenue_bn=latest.get("revenue_bn"),
    )
    if normalized_latest_equity_bn is not None:
        latest["equity_bn"] = normalized_latest_equity_bn

    if latest.get("revenue_bn") is not None and latest.get("net_income_bn") is not None:
        latest["net_margin_pct"] = _safe_ratio(float(latest["net_income_bn"]), float(latest["revenue_bn"]))

    current_segments = fixture.get("current_segments") or []
    if current_segments and latest.get("revenue_bn") not in (None, 0):
        total_revenue = float(latest["revenue_bn"])
        latest["segments"] = [
            {
                "name": item["name"],
                "value_bn": float(item["value_bn"]),
                "share_pct": float(item["value_bn"]) / total_revenue * 100,
                "yoy_pct": item.get("yoy_pct"),
            }
            for item in current_segments
        ]
        latest["structure_basis"] = "segment"
    current_geographies = fixture.get("current_geographies") or []
    if current_geographies:
        latest["geographies"] = [
            {
                "name": item["name"],
                "value_bn": float(item["value_bn"]),
                "share_pct": float(item.get("share_pct") or 0.0) or (
                    float(item["value_bn"]) / float(latest["revenue_bn"]) * 100 if latest.get("revenue_bn") not in (None, 0) else 0.0
                ),
                "yoy_pct": item.get("yoy_pct"),
                "scope": item.get("scope"),
            }
            for item in current_geographies
            if item.get("value_bn") is not None
        ]
        if not latest.get("segments"):
            latest["structure_basis"] = "geography"

    previous = refreshed[latest_index - 1] if latest_index > 0 else None
    year_ago = refreshed[latest_index - 4] if latest_index > 3 else None
    if previous and latest.get("revenue_bn") and previous.get("revenue_bn"):
        latest["revenue_qoq_pct"] = (float(latest["revenue_bn"]) / float(previous["revenue_bn"]) - 1) * 100
    else:
        latest["revenue_qoq_pct"] = None
    if previous and latest.get("net_income_bn") is not None and previous.get("net_income_bn") not in (None, 0):
        latest["net_income_qoq_pct"] = (float(latest["net_income_bn"]) / float(previous["net_income_bn"]) - 1) * 100
    else:
        latest["net_income_qoq_pct"] = None
    if year_ago and latest.get("revenue_bn") not in (None, 0) and year_ago.get("revenue_bn") not in (None, 0):
        latest["revenue_yoy_pct"] = (float(latest["revenue_bn"]) / float(year_ago["revenue_bn"]) - 1) * 100
    else:
        latest["revenue_yoy_pct"] = None
    if year_ago and latest.get("net_income_bn") is not None and year_ago.get("net_income_bn") not in (None, 0):
        latest["net_income_yoy_pct"] = (float(latest["net_income_bn"]) / float(year_ago["net_income_bn"]) - 1) * 100
    else:
        latest["net_income_yoy_pct"] = None

    refreshed[latest_index] = latest
    latest["ttm_revenue_bn"] = _ttm_sum(refreshed, "revenue_bn", latest_index)
    previous_ttm = _ttm_sum(refreshed, "revenue_bn", latest_index - 4) if latest_index >= 7 else None
    if latest["ttm_revenue_bn"] and previous_ttm:
        latest["ttm_revenue_growth_pct"] = (float(latest["ttm_revenue_bn"]) / float(previous_ttm) - 1) * 100
    else:
        latest["ttm_revenue_growth_pct"] = None
    recomputed_roe_pct = _ttm_roe_pct(refreshed, latest_index)
    if recomputed_roe_pct is not None:
        latest["roe_pct"] = recomputed_roe_pct
    return refreshed


def _fallback_to_history_if_outlier(
    parsed_value: Optional[float],
    history_value: Optional[float],
    *,
    min_ratio: float,
    max_ratio: float,
    min_abs_delta: float,
) -> Optional[float]:
    if parsed_value is None or history_value in (None, 0):
        return parsed_value
    parsed = float(parsed_value)
    baseline = float(history_value)
    ratio = parsed / baseline
    if ratio < min_ratio or ratio > max_ratio:
        if abs(parsed - baseline) >= min_abs_delta:
            return history_value
    return parsed_value


def _segments_total_value(items: list[dict[str, Any]]) -> float:
    return sum(float(item.get("value_bn") or 0.0) for item in items)


def _quarterly_structure_looks_reasonable(items: list[dict[str, Any]], revenue_bn: Optional[float]) -> bool:
    if not items or revenue_bn in (None, 0):
        return False
    total = _segments_total_value(items)
    if total <= 0:
        return False
    ratio = total / float(revenue_bn)
    return 0.45 <= ratio <= 1.45


def _sanitize_fixture_payload(
    company: dict[str, Any],
    fixture: dict[str, Any],
    latest_history: dict[str, Any],
) -> dict[str, Any]:
    sanitized = dict(fixture)
    latest_kpis = dict(sanitized.get("latest_kpis") or {})
    history_revenue = latest_history.get("revenue_bn")
    history_gross_margin = latest_history.get("gross_margin_pct")
    current_segments = list(sanitized.get("current_segments") or [])
    income_statement = dict(sanitized.get("income_statement") or {})
    statement_total = _statement_structure_total(income_statement)

    latest_revenue = latest_kpis.get("revenue_bn")
    segment_total = round(
        sum(float(item.get("value_bn") or 0.0) for item in current_segments if item.get("value_bn") is not None),
        3,
    )
    if latest_revenue not in (None, 0) and segment_total > 0 and float(latest_revenue) < segment_total * 0.25:
        scaled_revenue = round(float(latest_revenue) * 1000.0, 3)
        if segment_total * 0.7 <= scaled_revenue <= segment_total * 1.3:
            latest_kpis["revenue_bn"] = segment_total
        elif history_revenue not in (None, 0) and segment_total * 0.7 <= float(history_revenue) <= segment_total * 1.3:
            latest_kpis["revenue_bn"] = round(float(history_revenue), 3)
    latest_revenue = latest_kpis.get("revenue_bn")
    reference_revenue = history_revenue
    if reference_revenue in (None, 0):
        reference_revenue = statement_total
    if reference_revenue in (None, 0) and segment_total > 0:
        reference_revenue = segment_total
    latest_revenue = _fallback_to_history_if_outlier(
        latest_revenue,
        reference_revenue,
        min_ratio=0.25,
        max_ratio=4.0,
        min_abs_delta=1.0,
    )
    if latest_revenue is not None:
        latest_kpis["revenue_bn"] = round(float(latest_revenue), 3)

    for key in ("gaap_gross_margin_pct", "non_gaap_gross_margin_pct"):
        value = latest_kpis.get(key)
        if value is None:
            continue
        if not (-10 <= float(value) <= 95):
            latest_kpis[key] = history_gross_margin
    for key in ("gaap_eps", "non_gaap_eps"):
        value = latest_kpis.get(key)
        if value is None:
            continue
        if abs(float(value)) > 100:
            latest_kpis[key] = None

    trusted_revenue = latest_kpis.get("revenue_bn")
    trusted_revenue = _fallback_to_history_if_outlier(
        trusted_revenue,
        reference_revenue,
        min_ratio=0.25,
        max_ratio=4.0,
        min_abs_delta=1.0,
    )
    if trusted_revenue is None:
        trusted_revenue = _value_or_history(latest_kpis.get("revenue_bn"), history_revenue)

    calendar_quarter = str(
        sanitized.get("calendar_quarter")
        or latest_history.get("calendar_quarter")
        or latest_history.get("quarter_label")
        or ""
    )
    if calendar_quarter:
        companyfacts_supplement = get_companyfacts_quarter_supplement(str(company.get("id") or ""), calendar_quarter)
        for key in (
            "operating_cash_flow_bn",
            "capital_expenditures_bn",
            "free_cash_flow_bn",
            "share_repurchases_bn",
            "dividends_bn",
            "capital_return_bn",
        ):
            if companyfacts_supplement.get(key) is not None:
                latest_kpis[key] = companyfacts_supplement[key]

    operating_cash_flow_bn = latest_kpis.get("operating_cash_flow_bn")
    free_cash_flow_bn = latest_kpis.get("free_cash_flow_bn")
    capital_expenditures_bn = latest_kpis.get("capital_expenditures_bn")
    if any(
        _cash_flow_value_looks_implausible(value, trusted_revenue)
        for value in (operating_cash_flow_bn, free_cash_flow_bn, capital_expenditures_bn)
    ):
        latest_kpis.pop("operating_cash_flow_bn", None)
        latest_kpis.pop("free_cash_flow_bn", None)
        latest_kpis.pop("capital_expenditures_bn", None)
    else:
        if free_cash_flow_bn is None and operating_cash_flow_bn is not None and capital_expenditures_bn is not None:
            latest_kpis["free_cash_flow_bn"] = round(
                max(float(operating_cash_flow_bn) - abs(float(capital_expenditures_bn)), 0.0),
                3,
            )
        if capital_expenditures_bn is None and operating_cash_flow_bn is not None and free_cash_flow_bn is not None:
            gap = float(operating_cash_flow_bn) - float(free_cash_flow_bn)
            if gap >= 0:
                latest_kpis["capital_expenditures_bn"] = round(gap, 3)

    if current_segments and (
        not _quarterly_structure_looks_reasonable(current_segments, trusted_revenue)
        or _segments_look_incomplete_for_company(company, current_segments)
        or _segments_are_geography_like(company, current_segments)
    ):
        sanitized["current_segments"] = []
    current_geographies = list(sanitized.get("current_geographies") or [])
    if current_geographies:
        sanitized["current_geographies"] = _rescale_display_structure_items(current_geographies, trusted_revenue)
        current_geographies = list(sanitized.get("current_geographies") or [])
    if current_geographies:
        non_annual = [item for item in current_geographies if str(item.get("scope") or "") not in {"annual_filing", "regional_segment"}]
        if non_annual and not _quarterly_structure_looks_reasonable(non_annual, trusted_revenue):
            sanitized["current_geographies"] = [item for item in current_geographies if item not in non_annual]

    guidance = dict(sanitized.get("guidance") or {})
    commentary = _normalize_guidance_commentary(guidance.get("commentary"))
    if commentary:
        guidance["commentary"] = commentary
    else:
        guidance.pop("commentary", None)
    management_themes = [dict(item) for item in list(sanitized.get("management_themes") or []) if isinstance(item, dict)]
    qna_themes = [dict(item) for item in list(sanitized.get("qna_themes") or []) if isinstance(item, dict)]
    risks = [dict(item) for item in list(sanitized.get("risks") or []) if isinstance(item, dict)]
    catalysts = [dict(item) for item in list(sanitized.get("catalysts") or []) if isinstance(item, dict)]
    sanitized["management_themes"] = _stabilize_topic_cards(management_themes, qna_themes + risks + catalysts, minimum=4)
    sanitized["qna_themes"] = _stabilize_topic_cards(qna_themes, management_themes + risks + catalysts, minimum=4)
    sanitized["guidance"] = guidance
    sanitized["latest_kpis"] = latest_kpis
    return sanitized


def _financial_scale_mismatch(
    candidate_value: Optional[float],
    baseline_value: Optional[float],
    *,
    min_ratio: float = 0.25,
    max_ratio: float = 4.0,
    min_abs_delta: float = 1.0,
) -> bool:
    if candidate_value in (None, 0) or baseline_value in (None, 0):
        return False
    candidate = float(candidate_value)
    baseline = float(baseline_value)
    if abs(candidate - baseline) < min_abs_delta:
        return False
    ratio = candidate / baseline if baseline else 0.0
    return ratio < min_ratio or ratio > max_ratio


def _cash_flow_value_looks_implausible(
    candidate_value: Optional[float],
    revenue_bn: Optional[float],
    *,
    max_ratio: float = 4.0,
    min_abs_delta: float = 5.0,
) -> bool:
    if candidate_value in (None, 0) or revenue_bn in (None, 0):
        return False
    candidate = abs(float(candidate_value))
    revenue = float(revenue_bn)
    if candidate <= revenue * max_ratio:
        return False
    return abs(candidate - revenue) >= min_abs_delta


def _statement_structure_total(statement: dict[str, Any]) -> Optional[float]:
    groups = _statement_items(list(statement.get("business_groups") or statement.get("sources") or []))
    if not groups:
        return None
    total = _segments_total_value(groups)
    return total if total > 0 else None


def _growth_metric_mismatch(
    actual_value: Optional[float],
    expected_value: Optional[float],
    *,
    tolerance: float = 0.75,
) -> bool:
    if actual_value is None or expected_value is None:
        return False
    return abs(float(actual_value) - float(expected_value)) > tolerance


def _refresh_fixture_growth_metrics_from_history(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    if not history or not _history_has_contiguous_quarters(history):
        return fixture
    refreshed_fixture = dict(fixture)
    latest_kpis = dict(refreshed_fixture.get("latest_kpis") or {})
    recompute_base = [dict(entry) for entry in history]
    latest = dict(recompute_base[-1])

    latest_revenue = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    if latest_revenue is not None:
        latest["revenue_bn"] = float(latest_revenue)
    latest_profit = _value_or_history(_historical_profit_series_value(company, latest_kpis), latest.get("net_income_bn"))
    if latest_profit is not None:
        latest["net_income_bn"] = float(latest_profit)
    recompute_base[-1] = latest
    expected_latest = _recompute_history_derivatives(recompute_base)[-1]

    for key in ("revenue_qoq_pct", "revenue_yoy_pct", "net_income_qoq_pct", "net_income_yoy_pct"):
        value = expected_latest.get(key)
        if value is not None:
            latest_kpis[key] = float(value)
    refreshed_fixture["latest_kpis"] = latest_kpis
    return refreshed_fixture


def _guidance_matches_single_diversified_segment(
    guidance_revenue_bn: Optional[float],
    segments: list[dict[str, Any]],
    current_revenue_bn: Optional[float],
) -> bool:
    if guidance_revenue_bn in (None, 0) or current_revenue_bn in (None, 0):
        return False
    normalized_segments = _statement_items(list(segments or []))
    if len(normalized_segments) < 2:
        return False
    segment_total = _segments_total_value(normalized_segments)
    if segment_total <= 0:
        return False
    max_share = max(float(item.get("value_bn") or 0.0) for item in normalized_segments) / segment_total
    if max_share > 0.75:
        return False
    guidance_revenue = float(guidance_revenue_bn)
    current_revenue = float(current_revenue_bn)
    if guidance_revenue >= current_revenue * 0.78:
        return False
    for item in normalized_segments:
        segment_value = float(item.get("value_bn") or 0.0)
        if segment_value <= 0:
            continue
        if abs(guidance_revenue - segment_value) <= max(1.0, segment_value * 0.15):
            return True
    return False


def _guidance_payload_looks_implausible(
    payload: dict[str, Any],
    company: dict[str, Any],
    history: list[dict[str, Any]],
) -> bool:
    guidance = dict(payload.get("guidance") or {})
    if str(guidance.get("mode") or "").casefold() != "official":
        return False
    if guidance.get("revenue_derived_from_baseline"):
        return False
    guidance_revenue = guidance.get("revenue_bn")
    if guidance_revenue in (None, 0):
        return False
    current_segments = [dict(item) for item in list(payload.get("current_segments") or []) if isinstance(item, dict)]
    if not current_segments and history:
        current_segments = [dict(item) for item in list(history[-1].get("segments") or []) if isinstance(item, dict)]
    current_revenue = dict(payload.get("latest_kpis") or {}).get("revenue_bn")
    if current_revenue in (None, 0) and history:
        current_revenue = history[-1].get("revenue_bn")
    return _guidance_matches_single_diversified_segment(guidance_revenue, current_segments, current_revenue)


def _refresh_guidance_from_cached_materials_if_needed(
    payload: dict[str, Any],
    company: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    if not history:
        return normalized
    guidance = _resolve_guidance_payload(dict(normalized.get("guidance") or {}), history)
    normalized["guidance"] = guidance
    if not _guidance_payload_looks_implausible(normalized, company, history):
        return normalized

    calendar_quarter = str(normalized.get("calendar_quarter") or history[-1].get("calendar_quarter") or "")
    if not calendar_quarter:
        return normalized
    cached_material_sources = _discover_cached_material_sources(
        str(company["id"]),
        calendar_quarter,
        required_roles={"earnings_release", "earnings_call", "earnings_presentation", "earnings_commentary", "sec_filing"},
    )
    if not cached_material_sources:
        return normalized
    refreshed_payload = _refresh_historical_guidance_from_cached_materials(
        company,
        calendar_quarter,
        {"guidance": guidance},
        list(cached_material_sources),
    )
    refreshed_guidance = _resolve_guidance_payload(dict((refreshed_payload or {}).get("guidance") or {}), history)
    if not refreshed_guidance:
        return normalized

    candidate_payload = dict(normalized)
    candidate_payload["guidance"] = refreshed_guidance
    if _guidance_payload_looks_implausible(candidate_payload, company, history):
        return normalized

    latest = dict(history[-1])
    latest_kpis = dict(normalized.get("latest_kpis") or {})
    brand = dict(company.get("brand") or {})
    mode = str(refreshed_guidance.get("mode") or "proxy")
    if mode == "official":
        guidance_chart_title = "当前业绩与下一季指引"
    elif mode == "official_context":
        guidance_chart_title = "当前季度与管理层展望"
    else:
        guidance_chart_title = "当前业绩与后续经营参照"
    visuals = dict(normalized.get("visuals") or {})
    visuals["guidance"] = render_guidance_svg(
        latest_kpis.get("revenue_bn"),
        refreshed_guidance.get("revenue_bn"),
        _metric_or_fallback(latest_kpis.get("gaap_gross_margin_pct"), latest.get("net_margin_pct")) or 0.0,
        refreshed_guidance.get(
            "gaap_gross_margin_pct",
            refreshed_guidance.get(
                "adjusted_ebitda_margin_pct",
                _metric_or_fallback(latest_kpis.get("gaap_gross_margin_pct"), latest.get("net_margin_pct")) or 0.0,
            ),
        ),
        str(brand.get("primary") or "#0F172A"),
        str(brand.get("accent") or "#16A34A"),
        money_symbol=money_symbol,
        current_label=refreshed_guidance.get("current_label", "本季收入"),
        comparison_label=refreshed_guidance.get("comparison_label", "下一季收入指引"),
        current_margin_label=refreshed_guidance.get("current_margin_label", "本季毛利率"),
        comparison_margin_label=refreshed_guidance.get("comparison_margin_label", "下一季毛利率指引"),
        chart_title=guidance_chart_title,
    )
    normalized["guidance"] = refreshed_guidance
    normalized["visuals"] = visuals
    normalized["guidance_panel"] = _build_guidance_panel(normalized, latest, money_symbol)
    return normalized


def _refresh_topic_cards_from_cached_materials_if_needed(
    payload: dict[str, Any],
    company: dict[str, Any],
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized = dict(payload or {})
    if not history or not _payload_contains_placeholder_topic_cards(normalized):
        return normalized

    calendar_quarter = str(normalized.get("calendar_quarter") or history[-1].get("calendar_quarter") or "")
    if not calendar_quarter:
        return normalized

    refreshed = _load_or_parse_historical_official_quarter_payload(
        company,
        calendar_quarter,
        entry=dict(history[-1]),
        prefer_lightweight_structure=True,
    )
    if not refreshed:
        return normalized

    for key in ("management_themes", "qna_themes", "risks", "catalysts"):
        existing_items = [dict(item) for item in list(normalized.get(key) or []) if isinstance(item, dict)]
        refreshed_items = [dict(item) for item in list(refreshed.get(key) or []) if isinstance(item, dict)]
        if not refreshed_items or _placeholder_topic_count(refreshed_items) >= len(refreshed_items):
            continue
        if not existing_items or _placeholder_topic_count(existing_items) > 0:
            normalized[key] = refreshed_items
    return normalized


def _report_payload_has_financial_inconsistency(payload: dict[str, Any]) -> bool:
    normalized = dict(payload or {})
    latest_kpis = dict(normalized.get("latest_kpis") or {})
    income_statement = dict(normalized.get("income_statement") or {})
    history = [dict(item) for item in list(normalized.get("historical_cube") or []) if isinstance(item, dict)]
    latest_history = dict(history[-1]) if history else {}
    current_segments = [dict(item) for item in list(normalized.get("current_segments") or []) if isinstance(item, dict)]

    latest_revenue = latest_kpis.get("revenue_bn")
    statement_revenue = income_statement.get("revenue_bn")
    history_revenue = latest_history.get("revenue_bn")
    segment_total = _segments_total_value(current_segments) if current_segments else None
    statement_total = _statement_structure_total(income_statement)

    for trusted_value in (history_revenue, segment_total, statement_total):
        if trusted_value in (None, 0):
            continue
        if _financial_scale_mismatch(latest_revenue, trusted_value):
            return True
        if _financial_scale_mismatch(statement_revenue, trusted_value):
            return True

    if current_segments and history_revenue not in (None, 0) and not _quarterly_structure_looks_reasonable(current_segments, history_revenue):
        return True
    if _history_has_contiguous_quarters(history):
        expected_latest = _recompute_history_derivatives(history)[-1]
        for metric_key in ("revenue_qoq_pct", "revenue_yoy_pct", "net_income_qoq_pct", "net_income_yoy_pct"):
            if _growth_metric_mismatch(latest_kpis.get(metric_key), expected_latest.get(metric_key)):
                return True
            if _growth_metric_mismatch(latest_history.get(metric_key), expected_latest.get(metric_key)):
                return True
    return False


def _normalize_cached_income_statement_overrides(
    company: dict[str, Any],
    statement: dict[str, Any],
    revenue_bn: Optional[float],
) -> dict[str, Any]:
    normalized = dict(statement or {})
    for key in (
        "revenue_bn",
        "gross_profit_bn",
        "gross_margin_pct",
        "cost_of_revenue_bn",
        "operating_profit_bn",
        "operating_margin_pct",
        "operating_expenses_bn",
        "net_profit_bn",
        "net_margin_pct",
        "sources",
        "official_sources",
        "business_groups",
    ):
        normalized.pop(key, None)

    for key in ("sources", "business_groups"):
        items = _statement_items(list(statement.get(key) or []))
        if not items:
            continue
        if not _quarterly_structure_looks_reasonable(items, revenue_bn):
            continue
        if key == "business_groups" and _segments_are_geography_like(company, items):
            continue
        normalized[key] = items

    opex_breakdown: list[dict[str, Any]] = []
    for item in list(statement.get("opex_breakdown") or []):
        if item.get("value_bn") is None:
            continue
        normalized_item = dict(item)
        if revenue_bn not in (None, 0):
            normalized_item["pct_of_revenue"] = round(float(normalized_item.get("value_bn") or 0.0) / float(revenue_bn) * 100, 1)
        opex_breakdown.append(normalized_item)
    if opex_breakdown:
        normalized["opex_breakdown"] = opex_breakdown
    return normalized


def _repair_report_payload_financial_sections(
    payload: dict[str, Any],
    company: dict[str, Any],
    money_symbol: str,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    history = [dict(item) for item in list(normalized.get("historical_cube") or []) if isinstance(item, dict)]
    if not history or not _report_payload_has_financial_inconsistency(normalized):
        return normalized

    fixture = dict(normalized)
    fixture.setdefault("calendar_quarter", str(normalized.get("calendar_quarter") or ""))
    fixture.setdefault("fiscal_label", str(normalized.get("fiscal_label") or ""))
    fixture.setdefault("period_end", str(normalized.get("period_end") or ""))
    if not fixture.get("coverage_months"):
        fixture["coverage_months"] = _coverage_months_for_period(
            str(fixture.get("calendar_quarter") or ""),
            str(fixture.get("period_end") or ""),
        )
    latest_history = dict(history[-1])
    fixture = _sanitize_fixture_payload(company, fixture, latest_history)
    history = _refresh_latest_history_entry(company, history, fixture)
    history = _recompute_history_derivatives(history)
    fixture = _refresh_fixture_growth_metrics_from_history(company, fixture, history)
    fixture = _rehydrate_current_structures_from_history(company, fixture, history[-1])
    history = _refresh_latest_history_entry(company, history, fixture)
    history = _recompute_history_derivatives(history)
    fixture = _refresh_fixture_growth_metrics_from_history(company, fixture, history)
    latest_history = dict(history[-1])

    trusted_revenue = _value_or_history(dict(fixture.get("latest_kpis") or {}).get("revenue_bn"), latest_history.get("revenue_bn"))
    fixture["income_statement"] = _normalize_cached_income_statement_overrides(
        company,
        dict(fixture.get("income_statement") or {}),
        trusted_revenue,
    )
    takeaways = list(fixture.get("takeaways") or normalized.get("takeaways") or [])
    if takeaways:
        fixture["takeaways"] = takeaways

    structure_dimension = str(normalized.get("structure_dimension_used") or resolve_structure_dimension(str(company.get("id") or ""), history))
    merged_sources = [dict(item) for item in list(normalized.get("sources") or []) if isinstance(item, dict)]
    management_themes = _stabilize_topic_cards(
        [dict(item) for item in list(normalized.get("management_themes") or []) if isinstance(item, dict)],
        [dict(item) for item in list(normalized.get("qna_themes") or []) if isinstance(item, dict)]
        + [dict(item) for item in list(normalized.get("risks") or []) if isinstance(item, dict)]
        + [dict(item) for item in list(normalized.get("catalysts") or []) if isinstance(item, dict)],
        minimum=4,
    )
    qna_topics = _stabilize_topic_cards(
        [dict(item) for item in list(normalized.get("qna_themes") or []) if isinstance(item, dict)],
        management_themes
        + [dict(item) for item in list(normalized.get("risks") or []) if isinstance(item, dict)]
        + [dict(item) for item in list(normalized.get("catalysts") or []) if isinstance(item, dict)],
        minimum=4,
    )
    fixture["management_themes"] = management_themes
    normalized["management_themes"] = management_themes
    normalized["qna_themes"] = qna_topics
    transcript_summary = normalized.get("transcript_summary")
    brand = dict(company.get("brand") or {})

    metric_rows = _build_metric_rows(company, history, money_symbol)
    income_statement = _build_income_statement_snapshot(company, fixture, history)
    fixture["guidance"] = _resolve_guidance_payload(dict(fixture.get("guidance") or {}), history)
    guidance = dict(fixture.get("guidance") or {})
    guidance_mode = str(guidance.get("mode") or "proxy")
    if guidance_mode == "official":
        guidance_chart_title = "当前业绩与下一季指引"
    elif guidance_mode == "official_context":
        guidance_chart_title = "当前季度与管理层展望"
    else:
        guidance_chart_title = "当前业绩与后续经营参照"
    guidance_note = str(
        normalized.get("guidance_note")
        or dict(normalized.get("section_meta") or {}).get("guidance", {}).get("note")
        or ""
    )
    section_meta = _build_dynamic_section_meta(
        company,
        fixture,
        history,
        structure_dimension=structure_dimension,
        money_symbol=money_symbol,
        institutional_views=[dict(item) for item in list(normalized.get("institutional_views") or []) if isinstance(item, dict)],
        transcript_summary=transcript_summary if isinstance(transcript_summary, dict) else None,
        qna_topics=qna_topics,
        merged_sources=merged_sources,
        guidance_note=guidance_note,
    )

    visuals = dict(normalized.get("visuals") or {})
    visuals["current_quarter"] = render_current_quarter_svg(
        metric_rows,
        str(brand.get("primary") or "#0F172A"),
        str(brand.get("secondary") or "#94A3B8"),
    )
    visuals["guidance"] = render_guidance_svg(
        fixture["latest_kpis"].get("revenue_bn"),
        guidance.get("revenue_bn"),
        _metric_or_fallback(fixture["latest_kpis"].get("gaap_gross_margin_pct"), latest_history.get("net_margin_pct")) or 0.0,
        guidance.get(
            "gaap_gross_margin_pct",
            guidance.get(
                "adjusted_ebitda_margin_pct",
                _metric_or_fallback(fixture["latest_kpis"].get("gaap_gross_margin_pct"), latest_history.get("net_margin_pct")) or 0.0,
            ),
        ),
        str(brand.get("primary") or "#0F172A"),
        str(brand.get("accent") or "#16A34A"),
        money_symbol=money_symbol,
        current_label=guidance.get("current_label", "本季收入"),
        comparison_label=guidance.get("comparison_label", "下一季收入指引"),
        current_margin_label=guidance.get("current_margin_label", "本季毛利率"),
        comparison_margin_label=guidance.get("comparison_margin_label", "下一季毛利率指引"),
        chart_title=guidance_chart_title,
    )
    visuals["segment_mix"] = render_segment_mix_svg(
        list(fixture.get("current_segments") or []),
        list(fixture.get("current_geographies") or []),
        dict(brand.get("segment_colors") or {}),
        str(brand.get("primary") or "#0F172A"),
        money_symbol=money_symbol,
    )
    visuals["income_statement"] = render_income_statement_svg(
        income_statement,
        dict(brand.get("segment_colors") or {}),
        str(brand.get("primary") or "#0F172A"),
        money_symbol=money_symbol,
    )
    visuals["statement_translation"] = render_statement_translation_svg(
        income_statement,
        str(brand.get("primary") or "#0F172A"),
        str(brand.get("accent") or "#16A34A"),
        money_symbol=money_symbol,
    )

    repaired = dict(normalized)
    repaired["latest_kpis"] = dict(fixture.get("latest_kpis") or {})
    repaired["guidance"] = guidance
    repaired["current_segments"] = list(fixture.get("current_segments") or [])
    repaired["current_geographies"] = list(fixture.get("current_geographies") or [])
    repaired["historical_cube"] = history
    repaired["headline"] = _compose_summary_headline(
        company,
        str(normalized.get("fiscal_label") or fixture.get("fiscal_label") or ""),
        dict(fixture.get("latest_kpis") or {}),
        latest_history,
        fixture,
    )
    if takeaways:
        repaired["takeaways"] = _normalize_takeaways(takeaways, dict(fixture.get("latest_kpis") or {}), latest_history, money_symbol)
        repaired["layered_takeaways"] = _build_layered_takeaways(company, fixture, history, money_symbol, merged_sources)
    repaired["income_statement"] = income_statement
    repaired["scoreboard"] = _build_scoreboard(company, fixture, history, structure_dimension, money_symbol)
    repaired["current_detail_cards"] = _build_current_detail_cards(company, fixture, history, structure_dimension, money_symbol)
    repaired["cash_panel"] = _build_cash_quality_panel(company, fixture, latest_history, money_symbol)
    repaired["guidance_panel"] = _build_guidance_panel(fixture, latest_history, money_symbol)
    repaired["historical_summary_cards"] = _build_history_summary_cards(history, money_symbol)
    repaired["mix_page_title"] = _mix_page_title(
        list(fixture.get("current_segments") or []),
        list(fixture.get("current_geographies") or []),
    )
    repaired["comparison"] = {
        "latest": latest_history,
        "previous": history[-2] if len(history) > 1 else latest_history,
        "year_ago": history[-5] if len(history) > 4 else latest_history,
    }
    repaired["section_meta"] = section_meta
    repaired["visuals"] = visuals
    return repaired


def _quarterize_fixture_geographies(fixture: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(fixture)
    geographies = list(normalized.get("current_geographies") or [])
    revenue_bn = (dict(normalized.get("latest_kpis") or {})).get("revenue_bn")
    if not geographies:
        return normalized
    mapped_geographies, mapped_from_official = _prepare_quarterly_geographies(geographies, revenue_bn)
    if not mapped_geographies:
        return normalized
    normalized["current_geographies"] = mapped_geographies
    coverage_notes = list(normalized.get("coverage_notes") or [])
    if mapped_from_official and not any("季度化映射" in str(note) for note in coverage_notes):
        coverage_notes.append("地区结构已按官方地理披露占比完成季度化映射，确保与当季收入口径一致。")
    normalized["coverage_notes"] = coverage_notes
    return normalized


def _promote_geographies_as_segments(
    company: dict[str, Any],
    fixture: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(fixture)
    segments = list(normalized.get("current_segments") or [])
    geographies = list(normalized.get("current_geographies") or [])
    if segments or len(geographies) < 2:
        return normalized
    if len(list(company.get("segment_order") or [])) >= 2:
        return normalized
    blocked_scopes = {"annual_filing", "quarterly_mapped_from_official_geography", "regional_segment"}
    if any(str(item.get("scope") or "").casefold() in blocked_scopes for item in geographies):
        return normalized
    normalized["current_segments"] = [
        {
            "name": str(item.get("name") or "Region"),
            "value_bn": float(item.get("value_bn") or 0.0),
            "yoy_pct": item.get("yoy_pct"),
            "scope": "geo_proxy",
        }
        for item in geographies
        if float(item.get("value_bn") or 0.0) > 0
    ]
    return normalized


def _promote_history_geographies_as_segments(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    promoted: list[dict[str, Any]] = []
    for entry in history:
        row = dict(entry)
        segments = list(row.get("segments") or [])
        geographies = _normalize_historical_geographies(list(row.get("geographies") or []), row.get("revenue_bn"))
        blocked_scopes = {"annual_filing", "quarterly_mapped_from_official_geography", "regional_segment"}
        if not segments and len(geographies) >= 2 and not any(
            str(item.get("scope") or "").casefold() in blocked_scopes for item in geographies
        ):
            row["segments"] = [
                {
                    "name": str(item.get("name") or "Region"),
                    "value_bn": float(item.get("value_bn") or 0.0),
                    "share_pct": float(item.get("share_pct") or 0.0),
                    "yoy_pct": item.get("yoy_pct"),
                    "scope": "geo_proxy",
                }
                for item in geographies
                if float(item.get("value_bn") or 0.0) > 0
            ]
            row["segments_inferred"] = True
        promoted.append(row)
    return promoted


def _rehydrate_current_structures_from_history(
    company: dict[str, Any],
    fixture: dict[str, Any],
    latest_history: dict[str, Any],
) -> dict[str, Any]:
    hydrated = dict(fixture)
    if (
        (
            not hydrated.get("current_segments")
            or _segments_look_incomplete_for_company(company, list(hydrated.get("current_segments") or []))
            or _segments_are_geography_like(company, list(hydrated.get("current_segments") or []))
        )
        and latest_history.get("segments")
        and str(latest_history.get("structure_basis") or "segment") == "segment"
        and not _segments_are_geography_like(company, list(latest_history.get("segments") or []))
    ):
        hydrated["current_segments"] = _normalize_segment_items(
            company,
            [
                {
                    "name": item.get("name"),
                    "value_bn": item.get("value_bn"),
                    "yoy_pct": item.get("yoy_pct"),
                    "share_pct": item.get("share_pct"),
                }
                for item in list(latest_history.get("segments") or [])
            ],
        )
    current_geographies = list(hydrated.get("current_geographies") or [])
    if (
        not current_geographies
        and latest_history.get("geographies")
    ):
        hydrated["current_geographies"] = [
            {
                "name": item.get("name"),
                "value_bn": item.get("value_bn"),
                "yoy_pct": item.get("yoy_pct"),
                "scope": item.get("scope"),
            }
                for item in list(latest_history.get("geographies") or [])
            ]
        hydrated["current_geographies"] = _rescale_display_structure_items(
            list(hydrated.get("current_geographies") or []),
            latest_history.get("revenue_bn"),
        )
    elif latest_history.get("geographies"):
        latest_geo_names = {
            str(item.get("name") or "")
            for item in list(latest_history.get("geographies") or [])
            if str(item.get("name") or "")
        }
        current_geo_names = {
            str(item.get("name") or "")
            for item in current_geographies
            if str(item.get("name") or "")
        }
        if len(latest_geo_names) > len(current_geo_names) or (latest_geo_names and current_geo_names and latest_geo_names != current_geo_names):
            hydrated["current_geographies"] = _rescale_display_structure_items(
                [
                    {
                        "name": item.get("name"),
                        "value_bn": item.get("value_bn"),
                        "yoy_pct": item.get("yoy_pct"),
                        "scope": item.get("scope"),
                    }
                    for item in list(latest_history.get("geographies") or [])
                ],
                latest_history.get("revenue_bn"),
            )
    return hydrated


def _build_metric_rows(company: dict[str, Any], history: list[dict[str, Any]], money_symbol: str) -> list[dict[str, Any]]:
    latest = history[-1]
    previous = history[-2] if len(history) > 1 else latest
    year_ago = history[-5] if len(history) > 4 else latest
    profit_label = company.get("historical_profit_label", "净利润")
    rows = [
        {
            "label": "收入",
            "current": latest["revenue_bn"],
            "previous": previous["revenue_bn"],
            "year_ago": year_ago["revenue_bn"],
            "delta_yoy": latest.get("revenue_yoy_pct") or 0,
            "labels": {
                "current": format_money_bn(latest["revenue_bn"], money_symbol),
                "previous": format_money_bn(previous["revenue_bn"], money_symbol),
                "year_ago": format_money_bn(year_ago["revenue_bn"], money_symbol),
            },
        },
        {
            "label": profit_label,
            "current": latest["net_income_bn"],
            "previous": previous["net_income_bn"],
            "year_ago": year_ago["net_income_bn"],
            "delta_yoy": latest.get("net_income_yoy_pct") or 0,
            "labels": {
                "current": format_money_bn(latest["net_income_bn"], money_symbol),
                "previous": format_money_bn(previous["net_income_bn"], money_symbol),
                "year_ago": format_money_bn(year_ago["net_income_bn"], money_symbol),
            },
        },
    ]
    quality_key = "gross_margin_pct"
    quality_label = "毛利率"
    if latest.get("gross_margin_pct") is None and previous.get("gross_margin_pct") is None and year_ago.get("gross_margin_pct") is None:
        quality_key = "net_margin_pct"
        quality_label = "净利率"
    rows.append(
        {
            "label": quality_label,
            "current": latest.get(quality_key),
            "previous": previous.get(quality_key),
            "year_ago": year_ago.get(quality_key),
            "delta_yoy": (latest.get(quality_key) or 0) - (year_ago.get(quality_key) or 0),
            "labels": {
                "current": format_pct(latest.get(quality_key)),
                "previous": format_pct(previous.get(quality_key)),
                "year_ago": format_pct(year_ago.get(quality_key)),
            },
        }
    )
    return rows


def _history_average(history: list[dict[str, Any]], key: str, window: int = 4) -> Optional[float]:
    return _mean([entry.get(key) for entry in history[-window:]])


def _build_generic_guidance(history: list[dict[str, Any]]) -> dict[str, Any]:
    latest = history[-1]
    baseline_revenue = _history_average(history, "revenue_bn", 4) or latest.get("revenue_bn")
    use_gross_margin = latest.get("gross_margin_pct") is not None or _history_average(history, "gross_margin_pct", 4) is not None
    margin_key = "gross_margin_pct" if use_gross_margin else "net_margin_pct"
    margin_label = "毛利率" if use_gross_margin else "净利率"
    latest_margin = latest.get(margin_key) or 0.0
    baseline_margin = _history_average(history, margin_key, 4) or latest_margin
    return {
        "mode": "proxy",
        "revenue_bn": baseline_revenue,
        "comparison_revenue_bn": latest.get("revenue_bn"),
        "gaap_gross_margin_pct": baseline_margin,
        "comparison_margin_pct": latest_margin,
        "current_label": "本季收入",
        "comparison_label": "近四季季度均值",
        "current_margin_label": f"本季{margin_label}",
        "comparison_margin_label": f"近四季平均{margin_label}",
        "commentary": "当前尚未接入官方下一季指引，因此本页使用最近四季均值作为经营基线，用来判断本季收入与利润率是否高于中枢。",
    }


def _generic_focus_themes(company: dict[str, Any], history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest = history[-1]
    yoy = latest.get("revenue_yoy_pct") or 0
    qoq = latest.get("revenue_qoq_pct") or 0
    ttm = latest.get("ttm_revenue_growth_pct") or 0
    gross_margin = latest.get("gross_margin_pct") or 0
    net_margin = latest.get("net_margin_pct") or 0
    return [
        {
            "label": "收入动能与需求持续性",
            "score": _clamp_score(62 + max(yoy, 0) * 0.45 + max(qoq, 0) * 0.35),
            "note": f"最新季度收入同比 {format_pct(latest.get('revenue_yoy_pct'), signed=True)}，环比 {format_pct(latest.get('revenue_qoq_pct'), signed=True)}。",
        },
        {
            "label": "利润率与经营杠杆",
            "score": _clamp_score(50 + gross_margin * 0.5 + max(net_margin, 0) * 0.25),
            "note": f"当前毛利率 {format_pct(latest.get('gross_margin_pct'))}，净利率 {format_pct(latest.get('net_margin_pct'))}。",
        },
        {
            "label": "TTM 成长延续性",
            "score": _clamp_score(48 + max(ttm, 0) * 0.6),
            "note": f"近四季收入合计增速 {format_pct(latest.get('ttm_revenue_growth_pct'), signed=True)}。",
        },
        {
            "label": "研究主线与结构披露",
            "score": 58,
            "note": f"{company['description']} 当前默认以结构化季度财务序列组织研究框架。",
        },
    ]


def _generic_qna_themes(company: dict[str, Any], history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest = history[-1]
    ttm = latest.get("ttm_revenue_growth_pct") or 0
    margin_change = 0.0
    if len(history) > 4:
        margin_change = (latest.get("gross_margin_pct") or 0) - (history[-5].get("gross_margin_pct") or 0)
    return [
        {
            "label": "增长是否可持续",
            "score": _clamp_score(58 + max(ttm, 0) * 0.45),
            "note": "研究重点会放在本季高于还是低于最近四季经营中枢，以及下个阶段是否还能延续。",
        },
        {
            "label": "利润率拐点与兑现",
            "score": _clamp_score(50 + abs(margin_change) * 2.2),
            "note": f"12 季毛利率变化 {_format_delta(history[0].get('gross_margin_pct'), latest.get('gross_margin_pct'))}。",
        },
        {
            "label": "结构升级与第二曲线",
            "score": 54,
            "note": "当前缺少连续分部数据时，会优先结合管理层长期口径与利润质量一起判断结构变化。",
        },
        {
            "label": "经营基线与估值锚",
            "score": 52,
            "note": f"{company['english_name']} 当前页以趋势对照代替正式指引，不把基线图误写成官方展望。",
        },
    ]


def _generic_risks(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest = history[-1]
    yoy = latest.get("revenue_yoy_pct") or 0
    qoq = latest.get("revenue_qoq_pct") or 0
    margin_change = 0.0
    if len(history) > 4:
        margin_change = (latest.get("gross_margin_pct") or 0) - (history[-5].get("gross_margin_pct") or 0)
    return [
        {
            "label": "收入增速放缓",
            "score": _clamp_score(56 + max(0, 18 - yoy) * 1.2 + max(0, -qoq) * 1.5),
            "note": "若同比或环比继续走弱，市场会更快质疑需求持续性和盈利预测。",
        },
        {
            "label": "利润率回落",
            "score": _clamp_score(48 + max(0, -margin_change) * 2.5),
            "note": "毛利率或净利率回落通常意味着经营杠杆开始减弱，叙事质量会同步下降。",
        },
        {
            "label": "披露粒度有限",
            "score": 62,
            "note": "当前未接入完整电话会与连续分部历史，结构判断需要配合覆盖说明一起阅读。",
        },
    ]


def _generic_catalysts(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest = history[-1]
    yoy = latest.get("revenue_yoy_pct") or 0
    qoq = latest.get("revenue_qoq_pct") or 0
    ttm = latest.get("ttm_revenue_growth_pct") or 0
    margin_change = 0.0
    if len(history) > 4:
        margin_change = (latest.get("gross_margin_pct") or 0) - (history[-5].get("gross_margin_pct") or 0)
    return [
        {
            "label": "收入动能延续",
            "score": _clamp_score(54 + max(yoy, 0) * 0.6 + max(qoq, 0) * 0.45),
            "note": "若本季高于经营基线且下季仍能维持，报告中的趋势判断有继续上修空间。",
        },
        {
            "label": "利润率继续改善",
            "score": _clamp_score(48 + max(margin_change, 0) * 2.6),
            "note": "利润率改善会直接强化经营杠杆和现金创造的想象空间。",
        },
        {
            "label": "TTM 周期上修",
            "score": _clamp_score(50 + max(ttm, 0) * 0.55),
            "note": "TTM 增速上修意味着单季改善不只是短期噪音，而是更完整的成长节奏延续。",
        },
    ]


def _generic_evidence_cards(history: list[dict[str, Any]], money_symbol: str) -> list[dict[str, Any]]:
    latest = history[-1]
    prior = history[-5] if len(history) > 4 else history[0]
    quality_text = (
        f"毛利率 {format_pct(latest.get('gross_margin_pct'))}，净利率 {format_pct(latest.get('net_margin_pct'))}，"
        f"相较 4 季前变化 {_format_delta(prior.get('gross_margin_pct'), latest.get('gross_margin_pct'))}。"
        if latest.get("gross_margin_pct") is not None
        else f"净利率 {format_pct(latest.get('net_margin_pct'))}，当前样本未接入毛利率口径，因此利润质量改由净利率和净利润兑现共同观察。"
    )
    return [
        {
            "title": "结构化收入序列",
            "text": (
                f"最新季度收入 {format_money_bn(latest.get('revenue_bn'), money_symbol)}，同比 "
                f"{format_pct(latest.get('revenue_yoy_pct'), signed=True)}，已高于或低于近四季中枢。"
            ),
            "source_label": "Structured quarterly financial series",
        },
        {
            "title": "盈利质量锚点",
            "text": quality_text,
            "source_label": "Structured quarterly financial series",
        },
        {
            "title": "覆盖限制说明",
            "text": "当前公司默认走通用深度模板，尚未接入该季度官方电话会全文与连续季度分部细拆，因此结构页会按披露情况自动降级。",
            "source_label": "System coverage note",
        },
    ]


def _generic_sources(company: dict[str, Any], calendar_quarter: str, period_end: str) -> list[dict[str, Any]]:
    manual_sources = (
        dict(company.get("manual_sources_by_quarter") or {}).get(str(calendar_quarter))
        if isinstance(company.get("manual_sources_by_quarter"), dict)
        else None
    )
    if isinstance(manual_sources, list) and manual_sources:
        sources = [dict(item) for item in manual_sources if isinstance(item, dict)]
        if not any(str(item.get("kind") or "") == "structured_financials" for item in sources):
            sources.append(
                {
                    "label": f"{company['english_name']} quarterly financials",
                    "url": f"https://stockanalysis.com/stocks/{company['slug']}/financials/?p=quarterly",
                    "kind": "structured_financials",
                    "date": period_end,
                }
            )
        return sources
    structured_url = f"https://stockanalysis.com/stocks/{company['slug']}/financials/?p=quarterly"
    sources = [
        {
            "label": f"{company['english_name']} quarterly financials",
            "url": structured_url,
            "kind": "structured_financials",
            "date": period_end,
        }
    ]
    if company.get("ir_url"):
        sources.insert(
            0,
            {
                "label": f"{company['english_name']} investor relations",
                "url": company["ir_url"],
                "kind": "investor_relations",
                "date": period_end,
            },
        )
    return sources


def _generic_takeaways(
    company: dict[str, Any],
    history: list[dict[str, Any]],
    guidance: dict[str, Any],
    money_symbol: str,
) -> list[str]:
    latest = history[-1]
    profitability_line = (
        f"净利润 {format_money_bn(latest.get('net_income_bn'), money_symbol)}，净利率 "
        f"{format_pct(latest.get('net_margin_pct'))}，毛利率 "
        f"{format_pct(latest.get('gross_margin_pct'))}。"
        if latest.get("gross_margin_pct") is not None
        else f"净利润 {format_money_bn(latest.get('net_income_bn'), money_symbol)}，净利率 {format_pct(latest.get('net_margin_pct'))}，利润质量以净利率与净利润兑现为主线观察。"
    )
    takeaways = [
        (
            f"收入 {format_money_bn(latest.get('revenue_bn'), money_symbol)}，同比 "
            f"{format_pct(latest.get('revenue_yoy_pct'), signed=True)}，环比 "
            f"{format_pct(latest.get('revenue_qoq_pct'), signed=True)}。"
        ),
        profitability_line,
        (
            f"近四季收入合计 {format_money_bn(latest.get('ttm_revenue_bn'), money_symbol)}，"
            f"TTM 增速 {format_pct(latest.get('ttm_revenue_growth_pct'), signed=True)}。"
        ),
        "当前尚未接入官方下一季指引与完整 transcript，因此指引页使用经营基线对照，电话会页使用研究关注主题。"
        if guidance["mode"] == "proxy"
        else guidance["commentary"],
    ]
    if company.get("currency_code") != "USD":
        takeaways.append(f"金额按 {company['currency_code']} 报告币种展示，未做汇率换算。")
    return takeaways[:4]


def _build_generic_fixture(
    company: dict[str, Any],
    calendar_quarter: str,
    history: list[dict[str, Any]],
    series: dict[str, Any],
) -> dict[str, Any]:
    latest = history[-1]
    period_end = _series_period_end(series, calendar_quarter) or _estimate_period_end(calendar_quarter)
    coverage_months = _coverage_months_for_period(calendar_quarter, period_end)
    guidance = _build_generic_guidance(history)
    latest_kpis = {
        "revenue_bn": latest.get("revenue_bn"),
        "revenue_yoy_pct": latest.get("revenue_yoy_pct"),
        "revenue_qoq_pct": latest.get("revenue_qoq_pct"),
        "net_income_yoy_pct": latest.get("net_income_yoy_pct"),
        "gaap_gross_margin_pct": _metric_or_fallback(latest.get("gross_margin_pct"), latest.get("net_margin_pct")),
        "non_gaap_gross_margin_pct": _metric_or_fallback(latest.get("gross_margin_pct"), latest.get("net_margin_pct")),
        "operating_income_bn": None,
        "net_income_bn": latest.get("net_income_bn"),
        "operating_cash_flow_bn": None,
        "free_cash_flow_bn": None,
        "gaap_eps": None,
        "non_gaap_eps": None,
    }
    headline = (
        f"{company['english_name']} 最新自然季度收入 {format_money_bn(latest.get('revenue_bn'), company['money_symbol'])}，"
        f"同比 {format_pct(latest.get('revenue_yoy_pct'), signed=True)}，"
        f"当前报告基于结构化季度财务序列生成深度版研究摘要。"
    )
    coverage_notes = [
        "这份报告会先看当季官方材料；未披露字段再回到历史财务序列补齐。",
        "电话会原文暂不完整时，这一页先展示研究问题与证据卡片，不硬做逐段摘录。",
        "由于缺少连续 12 季分部披露，结构迁移与增量贡献页改看总量成长、盈利质量与结构边界说明。",
    ]
    if company.get("currency_code") != "USD":
        coverage_notes.append(f"金额按 {company['currency_code']} 报告币种展示，未做外汇折算。")
    return {
        "fiscal_label": _resolved_fiscal_label(company, calendar_quarter, period_end),
        "release_date": "未内置官方发布日期",
        "period_end": period_end,
        "coverage_months": coverage_months,
        "materials": ["structured_financial_series"],
        "headline": headline,
        "takeaways": _generic_takeaways(company, history, guidance, company["money_symbol"]),
        "latest_kpis": latest_kpis,
        "guidance": guidance,
        "current_segments": [],
        "current_geographies": [],
        "management_themes": _generic_focus_themes(company, history),
        "qna_themes": _generic_qna_themes(company, history),
        "risks": _generic_risks(history),
        "catalysts": _generic_catalysts(history),
        "evidence_cards": _generic_evidence_cards(history, company["money_symbol"]),
        "coverage_notes": coverage_notes,
        "sources": _generic_sources(company, calendar_quarter, period_end),
    }


def _build_current_detail_cards(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    structure_dimension: str,
    money_symbol: str,
) -> list[dict[str, str]]:
    latest = history[-1]
    current_segments = fixture.get("current_segments") or []
    current_geographies = fixture.get("current_geographies") or []
    segment_copy = _segment_copy_profile(current_segments)
    regional_segment_duplicate_mode = (
        bool(current_segments)
        and bool(current_geographies)
        and _items_match_scope(current_geographies, "regional_segment")
        and {
            (
                str(item.get("name") or "").casefold(),
                round(float(item.get("value_bn") or 0.0), 3),
            )
            for item in current_segments
        }
        == {
            (
                str(item.get("name") or "").casefold(),
                round(float(item.get("value_bn") or 0.0), 3),
            )
            for item in current_geographies
        }
    )
    total = latest.get("revenue_bn") or 1
    cards: list[dict[str, str]] = []
    if current_segments:
        ranked_segments = sorted(current_segments, key=lambda item: float(item["value_bn"]), reverse=True)
        top_segment = ranked_segments[0]
        cards.append(
            {
                "title": f"{segment_copy['top_label']} | {top_segment['name']}",
                "value": format_money_bn(float(top_segment["value_bn"]), money_symbol),
                "note": (
                    f"占当季收入 {format_pct(float(top_segment['value_bn']) / float(total) * 100)}"
                    + (
                        f" | 同比 {format_pct(top_segment.get('yoy_pct'), signed=True)}"
                        if top_segment.get("yoy_pct") is not None
                        else ""
                    )
                ),
            }
        )
        fastest_candidates = [item for item in ranked_segments if item.get("yoy_pct") is not None and item["name"] != top_segment["name"]]
        if not fastest_candidates and len(ranked_segments) > 1:
            fastest_candidates = [ranked_segments[1]]
        fastest_segment = max(
            fastest_candidates,
            key=lambda item: float(item.get("yoy_pct") or 0.0),
            default=None,
        )
        if fastest_segment:
            cards.append(
                {
                    "title": f"{segment_copy['fast_label']} | {fastest_segment['name']}",
                    "value": format_pct(fastest_segment.get("yoy_pct"), signed=True),
                    "note": f"当季收入 {format_money_bn(float(fastest_segment['value_bn']), money_symbol)}",
                }
            )
    if current_geographies and not regional_segment_duplicate_mode:
        top_geo = max(current_geographies, key=lambda item: float(item.get("value_bn") or 0.0))
        annual_geo_mode = bool(current_geographies) and all(str(item.get("scope") or "") == "annual_filing" for item in current_geographies)
        regional_segment_mode = bool(current_geographies) and all(str(item.get("scope") or "") == "regional_segment" for item in current_geographies)
        cards.append(
            {
                "title": f"头部地区 | {top_geo['name']}",
                "value": format_money_bn(float(top_geo["value_bn"]), money_symbol),
                "note": (
                    (
                        f"占当季收入 {format_pct(float(top_geo['value_bn']) / float(total) * 100)}"
                        if not annual_geo_mode and not regional_segment_mode
                        else "区域经营分部口径"
                        if regional_segment_mode
                        else "最新年报地理口径"
                    )
                    + (f" | 同比 {format_pct(top_geo.get('yoy_pct'), signed=True)}" if top_geo.get("yoy_pct") is not None else "")
                ),
            }
        )
    if current_segments and len(cards) < 3:
        ranked_segments = sorted(current_segments, key=lambda item: float(item["value_bn"]), reverse=True)
        for segment in ranked_segments:
            title = f"{segment_copy['focus_label']} | {segment['name']}"
            if any(card["title"] == title or segment["name"] in card["title"] for card in cards):
                continue
            cards.append(
                {
                    "title": title,
                    "value": format_money_bn(float(segment["value_bn"]), money_symbol),
                    "note": (
                        f"占当季收入 {format_pct(float(segment['value_bn']) / float(total) * 100)}"
                        + (
                            f" | 同比 {format_pct(segment.get('yoy_pct'), signed=True)}"
                            if segment.get("yoy_pct") is not None
                            else ""
                        )
                    ),
                }
            )
            if len(cards) >= 3:
                break
    if cards:
        return cards[:3]
    return [
        {
            "title": "收入同比",
            "value": format_pct(latest.get("revenue_yoy_pct"), signed=True),
            "note": f"最新自然季度 {latest['quarter_label']}",
        },
        {
            "title": "TTM 增速",
            "value": format_pct(latest.get("ttm_revenue_growth_pct"), signed=True),
            "note": "近四季收入合计维度",
        },
        {
            "title": "净利率",
            "value": format_pct(latest.get("net_margin_pct")),
            "note": "利润兑现效率",
        },
    ]


def _build_cash_quality_panel(
    company: dict[str, Any],
    fixture: dict[str, Any],
    latest: dict[str, Any],
    money_symbol: str,
) -> dict[str, str]:
    latest_kpis = fixture["latest_kpis"]
    operating_cash_flow = latest_kpis.get("operating_cash_flow_bn")
    free_cash_flow = latest_kpis.get("free_cash_flow_bn")
    if operating_cash_flow is not None and free_cash_flow is not None:
        return {
            "title": "现金与盈利",
            "body": (
                f"本季经营现金流 {format_money_bn(operating_cash_flow, money_symbol)}，"
                f"自由现金流 {format_money_bn(free_cash_flow, money_symbol)}，"
                "反映利润兑现并未停留在会计口径。"
            ),
        }
    if operating_cash_flow is not None and latest_kpis.get("net_income_bn") is not None:
        return {
            "title": "现金与盈利",
            "body": (
                f"本季经营现金流 {format_money_bn(operating_cash_flow, money_symbol)}，"
                f"净利润 {format_money_bn(latest_kpis.get('net_income_bn'), money_symbol)}，"
                "说明利润兑现与现金创造仍在同向支撑。"
            ),
        }
    profit_label = company.get("historical_profit_label", "净利润")
    profit_margin_label = company.get("historical_profit_margin_label", "净利率")
    summary_profit_bn = latest_kpis.get("net_income_bn")
    summary_profit_margin = _safe_ratio(summary_profit_bn, latest.get("revenue_bn")) if summary_profit_bn is not None else latest.get("net_margin_pct")
    return {
        "title": "利润兑现",
        "body": (
            f"当前结构化季度序列尚未接入经营现金流与自由现金流，因此本页改看{profit_label} "
            f"{format_money_bn(summary_profit_bn if summary_profit_bn is not None else latest.get('net_income_bn'), money_symbol)}、{profit_margin_label} "
            f"{format_pct(summary_profit_margin)}"
            + (
                f" 与毛利率 {format_pct(latest.get('gross_margin_pct'))}。"
                if latest.get("gross_margin_pct") is not None
                else f"，并以{profit_margin_label}作为当前利润质量的主观察指标。"
            )
        ),
    }


def _build_guidance_panel(
    fixture: dict[str, Any],
    latest: dict[str, Any],
    money_symbol: str,
) -> dict[str, Any]:
    guidance = fixture["guidance"]
    mode = str(guidance.get("mode") or "proxy")
    if mode == "official":
        title = "业绩指引要点"
    elif mode == "official_context":
        title = "官方展望要点"
    else:
        title = "经营基线要点"

    bullets: list[str] = []
    revenue_low = guidance.get("revenue_low_bn")
    revenue_high = guidance.get("revenue_high_bn")
    guidance_revenue = guidance.get("revenue_bn")
    if revenue_low is not None and revenue_high is not None:
        if guidance_revenue is not None:
            bullets.append(
                f"{guidance.get('comparison_label') or '下一季收入指引'}约为 {format_money_bn(guidance_revenue, money_symbol)}。"
            )
        bullets.append(
            f"{guidance.get('comparison_label') or '下一季收入指引'}区间为 "
            f"{format_money_bn(revenue_low, money_symbol)} 到 {format_money_bn(revenue_high, money_symbol)}。"
        )
    elif guidance_revenue is not None:
        label = str(guidance.get("comparison_label") or ("下一季收入指引" if mode == "official" else "经营基线收入"))
        bullets.append(f"{label}约为 {format_money_bn(guidance_revenue, money_symbol)}。")

    guidance_margin = guidance.get("gaap_gross_margin_pct")
    if guidance_margin is not None:
        bullets.append(
            f"{guidance.get('comparison_margin_label') or '下一季利润率口径'}约为 {format_pct(guidance_margin)}。"
        )
    elif guidance.get("adjusted_ebitda_margin_pct") is not None:
        bullets.append(f"调整后 EBITDA 利润率口径约为 {format_pct(guidance.get('adjusted_ebitda_margin_pct'))}。")

    current_revenue = fixture["latest_kpis"].get("revenue_bn") or latest.get("revenue_bn")
    if current_revenue is not None and guidance_revenue is not None and current_revenue not in (None, 0):
        delta = (float(guidance_revenue) / float(current_revenue) - 1) * 100
        bullets.append(f"与本季收入相比，下一阶段收入参考变化 {format_pct(delta, signed=True)}。")

    commentary = _normalize_guidance_commentary(guidance.get("commentary"), limit=108)
    if commentary:
        bullets.append(commentary)

    return {
        "title": title,
        "bullets": bullets[:4],
    }


def _build_call_panel(
    fixture: dict[str, Any],
    transcript_summary: Optional[dict[str, Any]],
    qna_topics: list[dict[str, Any]],
    narrative_provenance: dict[str, Any],
) -> dict[str, Any]:
    del fixture
    return _writer_build_call_panel(transcript_summary, qna_topics, narrative_provenance)


def _build_history_summary_cards(history: list[dict[str, Any]], money_symbol: str) -> list[dict[str, str]]:
    latest = history[-1]
    start = history[0]
    return [
        {
            "title": "起点收入",
            "value": format_money_bn(start.get("revenue_bn"), money_symbol),
            "note": start["quarter_label"],
        },
        {
            "title": "最新收入",
            "value": format_money_bn(latest.get("revenue_bn"), money_symbol),
            "note": latest["quarter_label"],
        },
        {
            "title": "最新 TTM 增速",
            "value": format_pct(latest.get("ttm_revenue_growth_pct")),
            "note": "近四季收入合计维度",
        },
    ]


def _midpoint_bn(value: Optional[float], low: Optional[float], high: Optional[float]) -> Optional[float]:
    if value is not None:
        return float(value)
    if low is not None and high is not None:
        return (float(low) + float(high)) / 2
    return None


def _support_text_pool(report_data: dict[str, Any]) -> list[str]:
    fragments: list[str] = []
    guidance = dict(report_data.get("guidance") or {})
    coverage_lines = list(report_data.get("coverage_notes") or report_data.get("coverage_warnings") or [])
    for value in [
        guidance.get("commentary"),
        *(item.get("text") for item in list(report_data.get("evidence_cards") or []) if isinstance(item, dict)),
        *(item.get("analysis") for item in list(report_data.get("call_quote_cards") or []) if isinstance(item, dict)),
        *(item.get("note") for item in list(report_data.get("management_themes") or []) if isinstance(item, dict)),
        *(item.get("note") for item in list(report_data.get("qna_themes") or []) if isinstance(item, dict)),
        *(item.get("note") for item in list(report_data.get("risks") or []) if isinstance(item, dict)),
        *(item.get("note") for item in list(report_data.get("catalysts") or []) if isinstance(item, dict)),
        *coverage_lines,
    ]:
        cleaned = _clean_summary_fragment(value)
        if cleaned:
            fragments.append(cleaned)
    return fragments


def _extract_support_signals(
    report_data: dict[str, Any],
    topics: list[tuple[str, tuple[str, ...]]],
    *,
    limit: int = 4,
) -> list[dict[str, str]]:
    signals: list[dict[str, str]] = []
    seen_labels: set[str] = set()
    seen_notes: set[str] = set()
    fragments = _support_text_pool(report_data)
    for title, keywords in topics:
        for fragment in fragments:
            lowered = fragment.casefold()
            if not any(keyword in lowered for keyword in keywords):
                continue
            note = _excerpt_text(fragment, 120)
            normalized = re.sub(r"\s+", "", note.casefold())
            if title in seen_labels or normalized in seen_notes:
                continue
            signals.append({"title": title, "note": note})
            seen_labels.add(title)
            seen_notes.add(normalized)
            break
        if len(signals) >= limit:
            return signals
    return signals


def _capital_metric(label: str, value: Optional[str]) -> dict[str, str]:
    rendered = str(value or "-").strip() or "-"
    return {"label": label, "value": rendered}


def _capital_signal(
    *,
    kind: str,
    title: str,
    status: str,
    summary: str,
    note: str,
    metrics: list[dict[str, str]],
    evidence: Optional[str] = None,
) -> dict[str, Any]:
    supporting = " | ".join(
        f"{item['label']} {item['value']}"
        for item in metrics[:2]
        if str(item.get("value") or "-").strip() not in {"", "-"}
    )
    return {
        "kind": kind,
        "title": title,
        "status": status,
        "summary": summary,
        "note": note,
        "metrics": metrics,
        "evidence": evidence,
        "visual": {
            "headline": summary,
            "supporting": supporting,
        },
    }


def _build_capital_allocation_snapshot(
    company: dict[str, Any],
    report_data: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> dict[str, Any]:
    latest = history[-1] if history else {}
    latest_kpis = dict(report_data.get("latest_kpis") or {})
    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    net_income_bn = _value_or_history(latest_kpis.get("net_income_bn"), latest.get("net_income_bn"))
    operating_cash_flow_bn = latest_kpis.get("operating_cash_flow_bn")
    free_cash_flow_bn = latest_kpis.get("free_cash_flow_bn")
    capital_expenditures_bn = latest_kpis.get("capital_expenditures_bn")
    capital_return_bn = latest_kpis.get("capital_return_bn")
    share_repurchases_bn = latest_kpis.get("share_repurchases_bn")
    dividends_bn = latest_kpis.get("dividends_bn")

    implied_capex_bn = capital_expenditures_bn
    if implied_capex_bn is None and operating_cash_flow_bn is not None and free_cash_flow_bn is not None:
        implied_capex_bn = max(float(operating_cash_flow_bn) - float(free_cash_flow_bn), 0.0)
    if capital_return_bn is None and share_repurchases_bn is not None and dividends_bn is not None:
        capital_return_bn = round(float(share_repurchases_bn) + float(dividends_bn), 3)

    revenue_yoy_pct = _value_or_history(latest_kpis.get("revenue_yoy_pct"), latest.get("revenue_yoy_pct"))
    profit_margin_pct = _safe_ratio(net_income_bn, revenue_bn) if net_income_bn is not None else latest.get("net_margin_pct")
    cash_conversion_pct = _safe_ratio(operating_cash_flow_bn, net_income_bn) if net_income_bn not in (None, 0) else None
    ocf_margin_pct = _safe_ratio(operating_cash_flow_bn, revenue_bn)
    fcf_margin_pct = _safe_ratio(free_cash_flow_bn, revenue_bn)
    capex_intensity_pct = _safe_ratio(implied_capex_bn, revenue_bn)
    payout_vs_fcf_pct = _safe_ratio(capital_return_bn, free_cash_flow_bn) if free_cash_flow_bn not in (None, 0) else None
    payout_vs_ocf_pct = _safe_ratio(capital_return_bn, operating_cash_flow_bn) if operating_cash_flow_bn not in (None, 0) else None

    support_fragments = {
        "cash_generation": (_extract_support_signals(report_data, [("现金创造", ("cash flow", "经营现金流", "自由现金流", "cash generated"))], limit=1) or [{}])[0].get("note"),
        "capital_intensity": (_extract_support_signals(report_data, [("资本开支", ("capex", "capital expenditures", "资本开支", "property and equipment"))], limit=1) or [{}])[0].get("note"),
        "shareholder_return": (_extract_support_signals(report_data, [("股东回报", ("shareholder", "buyback", "repurchase", "dividend", "股东回报", "回购", "分红"))], limit=1) or [{}])[0].get("note"),
    }

    cards = [
        {
            "title": "Revenue",
            "value": format_money_bn(revenue_bn, money_symbol),
            "note": f"收入同比 {format_pct(revenue_yoy_pct, signed=True)}",
        },
        {
            "title": company.get("historical_profit_label", "净利润"),
            "value": format_money_bn(net_income_bn, money_symbol),
            "note": f"利润率 {format_pct(profit_margin_pct)}",
        },
        {
            "title": "Operating cash flow",
            "value": format_money_bn(operating_cash_flow_bn, money_symbol),
            "note": f"OCF margin {format_pct(ocf_margin_pct)}" if operating_cash_flow_bn is not None and revenue_bn not in (None, 0) else "未抓到明确 OCF 口径",
        },
        {
            "title": "Free cash flow",
            "value": format_money_bn(free_cash_flow_bn, money_symbol),
            "note": f"FCF margin {format_pct(fcf_margin_pct)}" if free_cash_flow_bn is not None and revenue_bn not in (None, 0) else "未抓到明确 FCF 口径",
        },
    ]
    if implied_capex_bn is not None:
        cards.append(
            {
                "title": "Implied CapEx",
                "value": format_money_bn(implied_capex_bn, money_symbol),
                "note": "优先使用官方 CapEx；缺失时再按 OCF - FCF 反推",
            }
        )
    if cash_conversion_pct is not None:
        cards.append(
            {
                "title": "现金转化",
                "value": format_pct(cash_conversion_pct),
                "note": "经营现金流 / 净利润",
            }
        )

    cash_status = "watch"
    if cash_conversion_pct is not None:
        cash_status = "strong" if cash_conversion_pct >= 110 else "balanced" if cash_conversion_pct >= 80 else "watch"
    cash_summary = (
        f"OCF {format_money_bn(operating_cash_flow_bn, money_symbol)} / FCF {format_money_bn(free_cash_flow_bn, money_symbol)}，现金转化 {format_pct(cash_conversion_pct)}。"
        if operating_cash_flow_bn is not None and free_cash_flow_bn is not None and cash_conversion_pct is not None
        else f"OCF {format_money_bn(operating_cash_flow_bn, money_symbol)}，相当于净利润的 {format_pct(cash_conversion_pct)}。"
        if operating_cash_flow_bn is not None and cash_conversion_pct is not None
        else f"当前只拿到利润口径 {format_money_bn(net_income_bn, money_symbol)}，现金流披露仍需继续补源。"
    )
    cash_note = (
        f"经营现金流率 {format_pct(ocf_margin_pct)}，自由现金流率 {format_pct(fcf_margin_pct)}，说明本季收入扩张在现金端也有兑现。"
        if operating_cash_flow_bn is not None and free_cash_flow_bn is not None
        else f"现金创造的核心锚点是 OCF {format_money_bn(operating_cash_flow_bn, money_symbol)}，先看它是否持续高于会计利润。"
        if operating_cash_flow_bn is not None
        else "现金流披露不足时，这一页会保留缺口，而不是用后验异常规则替代真实历史数据。"
    )
    if support_fragments["cash_generation"]:
        cash_note += f" 官方表述：{support_fragments['cash_generation']}"

    capital_status = "watch"
    if capex_intensity_pct is not None:
        capital_status = "light" if capex_intensity_pct <= 6 else "balanced" if capex_intensity_pct <= 12 else "heavy"
    capital_summary = (
        f"CapEx {format_money_bn(implied_capex_bn, money_symbol)}，约占收入 {format_pct(capex_intensity_pct)}，投入后仍保留 {format_money_bn(free_cash_flow_bn, money_symbol)} FCF。"
        if implied_capex_bn is not None and free_cash_flow_bn is not None
        else f"CapEx {format_money_bn(implied_capex_bn, money_symbol)}，约占收入 {format_pct(capex_intensity_pct)}。"
        if implied_capex_bn is not None
        else "本季缺少稳定 CapEx 披露，资本强度仍需要靠后续官方材料补齐。"
    )
    capital_note = (
        f"资本开支只占 OCF 的 {format_pct(_safe_ratio(implied_capex_bn, operating_cash_flow_bn))}，说明现金流被固定资产投入吞噬的程度有限。"
        if implied_capex_bn is not None and operating_cash_flow_bn not in (None, 0)
        else "资本强度判断优先依赖官方 CapEx / PPE 口径，缺失时才回退到 OCF 与 FCF 的差额。"
    )
    if support_fragments["capital_intensity"]:
        capital_note += f" 官方表述：{support_fragments['capital_intensity']}"

    shareholder_status = "watch"
    if payout_vs_fcf_pct is not None:
        shareholder_status = "aggressive" if payout_vs_fcf_pct > 100 else "balanced" if payout_vs_fcf_pct >= 50 else "selective"
    elif payout_vs_ocf_pct is not None:
        shareholder_status = "balanced" if payout_vs_ocf_pct >= 40 else "selective"
    shareholder_summary = (
        f"回购 {format_money_bn(share_repurchases_bn, money_symbol)} + 分红 {format_money_bn(dividends_bn, money_symbol)}，本季合计回流 {format_money_bn(capital_return_bn, money_symbol)}。"
        if capital_return_bn is not None and share_repurchases_bn is not None and dividends_bn is not None
        else f"本季股东回报 {format_money_bn(capital_return_bn, money_symbol)}。"
        if capital_return_bn is not None
        else "本季没有拿到稳定的回购 / 分红口径。"
    )
    shareholder_note = (
        f"股东回流相当于自由现金流的 {format_pct(payout_vs_fcf_pct)}，公司本季更偏向把已兑现现金直接返还股东。"
        if payout_vs_fcf_pct is not None
        else f"股东回流相当于经营现金流的 {format_pct(payout_vs_ocf_pct)}，可用来判断这季更偏扩张还是回报。"
        if payout_vs_ocf_pct is not None
        else "股东回报分析需要回购、分红与净现金口径一起看，当前官方披露仍不完整。"
    )
    if support_fragments["shareholder_return"]:
        shareholder_note += f" 官方表述：{support_fragments['shareholder_return']}"

    signals = [
        _capital_signal(
            kind="cash_generation",
            title="现金流视角",
            status=cash_status,
            summary=cash_summary,
            note=cash_note,
            metrics=[
                _capital_metric("OCF", format_money_bn(operating_cash_flow_bn, money_symbol)),
                _capital_metric("FCF", format_money_bn(free_cash_flow_bn, money_symbol)),
                _capital_metric("现金转化", format_pct(cash_conversion_pct)),
            ],
            evidence=support_fragments["cash_generation"],
        ),
        _capital_signal(
            kind="capital_intensity",
            title="资本强度",
            status=capital_status,
            summary=capital_summary,
            note=capital_note,
            metrics=[
                _capital_metric("CapEx", format_money_bn(implied_capex_bn, money_symbol)),
                _capital_metric("收入占比", format_pct(capex_intensity_pct)),
                _capital_metric("FCF", format_money_bn(free_cash_flow_bn, money_symbol)),
            ],
            evidence=support_fragments["capital_intensity"],
        ),
        _capital_signal(
            kind="shareholder_return",
            title="股东回报",
            status=shareholder_status,
            summary=shareholder_summary,
            note=shareholder_note,
            metrics=[
                _capital_metric("回购", format_money_bn(share_repurchases_bn, money_symbol)),
                _capital_metric("分红", format_money_bn(dividends_bn, money_symbol)),
                _capital_metric("总回流", format_money_bn(capital_return_bn, money_symbol)),
            ],
            evidence=support_fragments["shareholder_return"],
        ),
    ]

    bullets = [cash_summary, capital_summary, shareholder_summary]
    framework = [
        "先看 OCF/FCF 是否跟净利润同向，确认利润改善有没有真正落到现金。",
        "再看 CapEx 或 PPE 投入吞掉了多少现金，区分这是扩张投入还是现金质量转弱。",
        "最后把回购、分红和净现金变化放在一起看，判断公司当前更偏扩张期还是回报期。",
    ]

    return {
        "title": "现金流与资本配置",
        "cards": cards[:6],
        "bullets": bullets[:3],
        "signals": signals[:3],
        "framework": framework,
    }


def _build_expectation_reset_snapshot(
    company_id: str,
    calendar_quarter: str,
    report_data: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
    *,
    allow_expensive_parse: bool = True,
) -> dict[str, Any]:
    guidance = dict(report_data.get("guidance") or {})
    guidance_display_mode = _guidance_display_mode(guidance)
    latest_kpis = dict(report_data.get("latest_kpis") or {})
    current_actual = _value_or_history(latest_kpis.get("revenue_bn"), history[-1].get("revenue_bn") if history else None)
    current_low = guidance.get("revenue_low_bn")
    current_high = guidance.get("revenue_high_bn")
    current_guidance = None if guidance.get("revenue_derived_from_baseline") else _midpoint_bn(guidance.get("revenue_bn"), current_low, current_high)
    current_mode = str(guidance.get("mode") or "proxy")
    current_label = str(report_data.get("fiscal_label") or calendar_quarter)
    previous_quarter = _shift_calendar_quarter(calendar_quarter, -1)
    previous_snapshot = (
        _resolve_quarter_guidance_snapshot(
            company_id,
            previous_quarter,
            history,
            allow_expensive_parse=allow_expensive_parse,
        )
        if previous_quarter
        else None
    )
    previous_label = previous_quarter
    previous_guidance = {}
    previous_display_guidance = {}
    previous_guidance_mid = None
    if previous_snapshot is not None:
        previous_label = str(previous_snapshot.get("fiscal_label") or previous_quarter)
        previous_guidance = dict(previous_snapshot.get("guidance") or {})
        previous_history = [
            dict(item)
            for item in history
            if str(item.get("calendar_quarter") or item.get("quarter_label") or "") <= str(previous_quarter or "")
        ]
        previous_display_guidance = _resolve_guidance_payload(previous_guidance, previous_history) if previous_history else dict(previous_guidance)
        previous_guidance_mid = _midpoint_bn(
            previous_display_guidance.get("revenue_bn"),
            previous_display_guidance.get("revenue_low_bn"),
            previous_display_guidance.get("revenue_high_bn"),
        )
    previous_display_mode = _guidance_display_mode(previous_display_guidance or previous_guidance)

    cards = [
        {
            "title": _guidance_snapshot_title(str(previous_label or "上一季"), previous_display_mode, current=False),
            "value": format_money_bn(previous_guidance_mid, money_symbol) if previous_guidance_mid is not None else _guidance_display_value(previous_display_guidance, money_symbol) if previous_display_guidance else _guidance_display_value(previous_guidance, money_symbol) if previous_guidance else "未披露",
            "note": _guidance_display_note(previous_display_guidance, "上一季口径未命中缓存") if previous_display_guidance else _guidance_display_note(previous_guidance, "上一季口径未命中缓存") if previous_guidance else "上一季口径未命中缓存",
        },
        {
            "title": f"{current_label} 实际收入",
            "value": format_money_bn(current_actual, money_symbol),
            "note": f"同比 {format_pct(_value_or_history(latest_kpis.get('revenue_yoy_pct'), history[-1].get('revenue_yoy_pct') if history else None), signed=True)}",
        },
        {
            "title": _guidance_snapshot_title("本次", guidance_display_mode, current=True),
            "value": _guidance_display_value(guidance, money_symbol),
            "note": _guidance_display_note(guidance, _guidance_mode_label(current_mode)),
        },
    ]

    bullets = list((report_data.get("guidance_change_panel") or {}).get("bullets") or [])
    if not bullets:
        guidance_change_panel = _build_guidance_change_panel(
            company_id,
            calendar_quarter,
            report_data,
            history,
            money_symbol,
            allow_expensive_parse=allow_expensive_parse,
        )
        bullets = list(guidance_change_panel.get("bullets") or [])
    commentary = _clean_summary_fragment(guidance.get("commentary"))
    if commentary and not any(commentary[:18] in item for item in bullets):
        bullets.append(_excerpt_text(commentary, 120))
    expectation_panel = dict(report_data.get("expectation_panel") or {})
    for item in list(expectation_panel.get("bullets") or [])[:2]:
        cleaned = _clean_summary_fragment(item)
        if cleaned and cleaned not in bullets:
            bullets.append(cleaned)

    beat_vs_prior_pct = None
    if previous_guidance_mid not in (None, 0) and current_actual is not None:
        beat_vs_prior_pct = (float(current_actual) / float(previous_guidance_mid) - 1) * 100
    reset_vs_actual_pct = None
    if current_actual not in (None, 0) and current_guidance is not None:
        reset_vs_actual_pct = (float(current_guidance) / float(current_actual) - 1) * 100

    previous_commentary = _clean_summary_fragment(previous_display_guidance.get("commentary") or previous_guidance.get("commentary"))
    current_commentary = _clean_summary_fragment(guidance.get("commentary"))
    if previous_guidance_mid is None:
        if previous_commentary:
            bullets.insert(0, f"{previous_label or '上一季'} 未给出可验证的收入数值指引；当时的前瞻重点是：{_excerpt_text(previous_commentary, 94)}。")
        elif current_guidance is not None and current_actual is not None:
            bullets.insert(0, f"上一季正式指引缓存暂缺，因此先看本季实际 {format_money_bn(current_actual, money_symbol)} 与本次新口径 {format_money_bn(current_guidance, money_symbol)} 的顺序关系。")
        else:
            bullets.insert(0, "上一季未拿到可验证的收入数值指引，这页会保留真实前瞻口径，不再用上一季实际营收冒充 guidance。")
    if current_guidance is None and current_commentary:
        bullets.append(f"本次公司也未给出下一阶段收入数值指引；当前前瞻重点是：{_excerpt_text(current_commentary, 94)}。")

    deduped_bullets: list[str] = []
    seen_bullets: set[str] = set()
    for item in bullets:
        cleaned = _clean_summary_fragment(item)
        if not cleaned:
            continue
        normalized = re.sub(r"[，。；;,.!?！？\\s]+", "", cleaned.casefold())
        if normalized in seen_bullets:
            continue
        deduped_bullets.append(cleaned if cleaned.endswith("。") else f"{cleaned}。")
        seen_bullets.add(normalized)
    method = [
        "先看上一季给出的本季口径有没有兑现，再看本次新口径相对本季实际是抬升、持平还是降温。",
        "若公司不给数值指引，这页更该看口径变化、范围宽窄和管理层语气，而不是硬做精确预测。",
        "真正值得写进判断的是“预期中枢怎么变了”，而不是单独摘一句乐观或谨慎表述。",
    ]

    return {
        "title": "预期差与指引变化",
        "previous_label": previous_label or "上一季口径",
        "current_label": current_label,
        "cards": cards,
        "previous_guidance_bn": previous_guidance_mid,
        "current_actual_bn": current_actual,
        "current_guidance_bn": current_guidance,
        "beat_vs_prior_pct": beat_vs_prior_pct,
        "reset_vs_actual_pct": reset_vs_actual_pct,
        "bullets": deduped_bullets[:4],
        "method": method,
        "source_anchor": str((report_data.get("guidance_change_panel") or {}).get("source_anchor") or ""),
    }


def _build_balance_quality_snapshot(
    company: dict[str, Any],
    report_data: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> dict[str, Any]:
    latest = history[-1] if history else {}
    latest_kpis = dict(report_data.get("latest_kpis") or {})
    current_segments = list(report_data.get("current_segments") or [])
    current_geographies = list(report_data.get("current_geographies") or [])
    top_segment = max(current_segments, key=lambda item: float(item.get("value_bn") or 0.0), default=None)
    top_geo = max(current_geographies, key=lambda item: float(item.get("value_bn") or 0.0), default=None)
    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    equity_bn = _value_or_history(latest_kpis.get("ending_equity_bn"), latest.get("equity_bn"))
    cards = [
        {
            "title": "期末权益",
            "value": format_money_bn(equity_bn, money_symbol),
            "note": "用于观察资本厚度与后续 ROE",
        },
        {
            "title": "ROE (TTM)",
            "value": format_pct(latest.get("roe_pct")),
            "note": "过去四季净利润 / 平均权益",
        },
        {
            "title": company.get("historical_profit_margin_label", "净利率"),
            "value": format_pct(latest.get("net_margin_pct")),
            "note": "收入兑现成利润的效率",
        },
        {
            "title": "TTM 收入增速",
            "value": format_pct(latest.get("ttm_revenue_growth_pct"), signed=True),
            "note": "四季滚动口径",
        },
    ]
    if top_segment and revenue_bn not in (None, 0):
        cards.append(
            {
                "title": "头部业务集中度",
                "value": format_pct(float(top_segment.get("value_bn") or 0.0) / float(revenue_bn) * 100),
                "note": str(top_segment.get("name") or "头部业务"),
            }
        )
    elif top_geo and revenue_bn not in (None, 0):
        cards.append(
            {
                "title": "头部地区集中度",
                "value": format_pct(float(top_geo.get("value_bn") or 0.0) / float(revenue_bn) * 100),
                "note": str(top_geo.get("name") or "头部地区"),
            }
        )

    signals = _extract_support_signals(
        report_data,
        [
            ("Backlog / RPO", ("backlog", "remaining performance obligations", "rpo", "订单")),
            ("库存信号", ("inventory", "库存")),
            ("现金与有价证券", ("cash and cash equivalents", "marketable securities", "现金及有价证券", "net cash")),
            ("债务或杠杆", ("debt", "borrowings", "债务", "杠杆")),
            ("资本开支负担", ("capex", "capital expenditures", "资本开支")),
        ],
        limit=4,
    )
    if not signals:
        signals = [
            {"title": "资本厚度", "note": "看权益、ROE 和净利率是否同步改善，判断这家公司是在累积经营质量还是只在释放短期利润。"},
            {"title": "结构集中度", "note": "若头部业务或地区占比持续抬升，意味着后续几个季度的波动也会更集中地由单一变量决定。"},
            {"title": "订单与库存", "note": "硬件与半导体要多看库存，云与软件要多看 backlog / RPO，这类指标比单季收入更早暴露景气变化。"},
            {"title": "资产负债表", "note": "现金、债务和资本开支一起看，才能判断当前增长是在健康扩张还是在透支未来现金流。"},
        ]

    bullets = [
        f"期末权益 {format_money_bn(equity_bn, money_symbol)}、ROE {format_pct(latest.get('roe_pct'))}，用来判断利润质量有没有沉淀到资产负债表。",
        f"{company.get('historical_profit_margin_label', '净利率')} {format_pct(latest.get('net_margin_pct'))}、TTM 收入增速 {format_pct(latest.get('ttm_revenue_growth_pct'), signed=True)}，帮助分辨当前是提质还是单纯冲规模。",
    ]
    if top_segment and revenue_bn not in (None, 0):
        bullets.append(
            f"当前头部业务 {top_segment['name']} 占收入 {format_pct(float(top_segment.get('value_bn') or 0.0) / float(revenue_bn) * 100)}，经营质量判断不能脱离集中度来看。"
        )
    framework = [
        "硬件和半导体优先看库存、订单与集中度，云和软件优先看 backlog / RPO 与现金流质量。",
        "如果收入高增但权益、ROE 和净利率没有同步改善，通常更像短周期释放而不是质量抬升。",
        "当头部业务占比持续抬升时，下一季的风险和催化剂往往会更集中地落在同一个变量上。",
    ]

    return {
        "title": "资产负债与经营质量",
        "cards": cards[:5],
        "signals": signals[:4],
        "bullets": bullets[:3],
        "framework": framework,
    }


def _complete_validation_items(
    primary_items: list[dict[str, str]],
    fallback_items: list[dict[str, Any]],
    *,
    verify_default: str,
) -> list[dict[str, str]]:
    completed = [dict(item) for item in primary_items]
    used_labels = {str(item.get("label") or "").strip() for item in completed}
    for item in fallback_items:
        label = str(item.get("label") or "").strip()
        if not label or label in used_labels:
            continue
        completed.append(
            {
                "label": label,
                "trigger": str(item.get("note") or "").strip() or "继续观察该变量是否进一步发酵。",
                "verify": verify_default,
                "source_anchor": "",
            }
        )
        used_labels.add(label)
        if len(completed) >= 3:
            break
    return completed[:3]


def _build_validation_snapshot(
    report_data: dict[str, Any],
    qna_topics: list[dict[str, Any]],
    risk_scenarios: list[dict[str, Any]],
    catalyst_scenarios: list[dict[str, Any]],
) -> dict[str, Any]:
    positive_items = [
        {
            "label": str(item.get("label") or "多头验证点"),
            "trigger": str(item.get("trigger") or ""),
            "verify": str(item.get("verify") or ""),
            "source_anchor": str(item.get("source_anchor") or ""),
        }
        for item in catalyst_scenarios[:3]
    ]
    negative_items = [
        {
            "label": str(item.get("label") or "风险验证点"),
            "trigger": str(item.get("trigger") or ""),
            "verify": str(item.get("verify") or ""),
            "source_anchor": str(item.get("source_anchor") or ""),
        }
        for item in risk_scenarios[:3]
    ]
    positive_items = _complete_validation_items(
        positive_items,
        [item for item in list(report_data.get("catalysts") or []) if isinstance(item, dict)],
        verify_default="下季度继续跟踪公司原文、业务结构和关键 KPI 是否同步强化。",
    )
    negative_items = _complete_validation_items(
        negative_items,
        [item for item in list(report_data.get("risks") or []) if isinstance(item, dict)],
        verify_default="下季度继续核对是否真正落到收入、利润率、现金流或指引端。",
    )

    bullets = [
        f"优先追问：{str(item.get('label') or '').strip()}。"
        for item in qna_topics[:3]
        if str(item.get("label") or "").strip()
    ]
    if not bullets:
        bullets = [
            "这页的目的不是重新总结，而是把下个季度真正需要继续验证的问题提前列出来。",
            "多头与风险验证点都需要落实到可追踪指标，而不是停留在模糊叙事。",
        ]
    method = [
        "优先去找最先能动到收入、利润率、现金流或指引的那个指标，不要只盯叙事热度。",
        "如果管理层原话和卖方关注点背离，下一季最值得跟踪的通常就是那个背离最大的变量。",
        "这页最适合作为下次复盘的对照清单，直接拿来勾选“兑现/未兑现/延后”。",
    ]

    return {
        "title": "下一季验证清单",
        "positive_items": positive_items[:3],
        "negative_items": negative_items[:3],
        "bullets": bullets[:3],
        "method": method,
    }


def _build_scoreboard(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    structure_dimension: str,
    money_symbol: str,
) -> list[dict[str, str]]:
    latest = fixture["latest_kpis"]
    latest_history = history[-1] if history else {}
    top_segment = max(fixture["current_segments"], key=lambda item: float(item["value_bn"])) if fixture.get("current_segments") else None
    segment_copy = _segment_copy_profile(list(fixture.get("current_segments") or []))
    if latest.get("free_cash_flow_bn") is None:
        net_income_yoy_pct = _value_or_history(latest.get("net_income_yoy_pct"), latest_history.get("net_income_yoy_pct"))
        net_margin_pct = _value_or_history(latest_history.get("net_margin_pct"), _safe_ratio(latest.get("net_income_bn"), latest.get("revenue_bn")))
        profitability_subvalue_parts: list[str] = []
        if net_income_yoy_pct is not None:
            profitability_subvalue_parts.append(f"YoY {format_pct(net_income_yoy_pct, signed=True)}")
        if net_margin_pct is not None:
            profitability_subvalue_parts.append(f"净利率 {format_pct(net_margin_pct)}")
        profitability_card = {
            "title": "净利润",
            "value": format_money_bn(latest.get("net_income_bn"), money_symbol),
            "subvalue": " · ".join(profitability_subvalue_parts) if profitability_subvalue_parts else "利润质量跟踪",
        }
    else:
        profitability_card = {
            "title": "自由现金流",
            "value": format_money_bn(latest["free_cash_flow_bn"], money_symbol),
            "subvalue": f"FCF margin {format_pct(latest['free_cash_flow_bn'] / latest['revenue_bn'] * 100)}",
        }
    structure_card = {
        "title": segment_copy["top_label"],
        "value": top_segment["name"] if top_segment else company["currency_code"],
        "subvalue": format_pct(top_segment["value_bn"] / latest["revenue_bn"] * 100) if top_segment else "金额按报告币种展示",
    }
    if top_segment and structure_dimension == "geography":
        structure_card = {
            "title": "头部地区",
            "value": top_segment["name"],
            "subvalue": format_pct(top_segment["value_bn"] / latest["revenue_bn"] * 100),
        }
    if not top_segment:
        structure_card = {
            "title": "结构模式",
            "value": "分部历史完整" if structure_dimension == "segment" else "地区结构展示" if structure_dimension == "geography" else "结构降级展示",
            "subvalue": "12 季模块自动切换",
        }
    return [
        {"title": "公司", "value": f"{company['name']} / {company['ticker']}", "subvalue": fixture["fiscal_label"]},
        {"title": "自然季度", "value": resolve_calendar_quarter_from_months(fixture["coverage_months"]), "subvalue": "按经营覆盖月份归属"},
        {"title": "收入", "value": format_money_bn(latest["revenue_bn"], money_symbol), "subvalue": f"YoY {format_pct(latest['revenue_yoy_pct'], signed=True)}"},
        profitability_card,
        {
            "title": "指引口径",
            "value": "官方指引" if fixture["guidance"]["mode"] == "official" else "官方语境" if fixture["guidance"]["mode"] == "official_context" else "经营基线",
            "subvalue": "页面文案会随口径自动切换",
        },
        structure_card,
    ]


def build_report_payload(
    company_id: str,
    calendar_quarter: str,
    history_window: int = 12,
    manual_transcript_upload_id: Optional[str] = None,
    refresh_source_materials: bool = False,
    require_full_coverage: Optional[bool] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> dict[str, Any]:
    full_coverage_mode = _require_full_coverage_mode() if require_full_coverage is None else bool(require_full_coverage)
    if full_coverage_mode and os.environ.get("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH") == "1":
        raise ValueError("Full coverage mode requires official source fetch; unset EARNINGS_DIGEST_DISABLE_SOURCE_FETCH.")
    _emit_progress(progress_callback, 0.03, "prepare", "正在校验参数并加载公司基础数据...")
    company = get_company(company_id)
    report_style = _build_report_style(company)
    historical_growth_title = f"近 {int(history_window)} 季成长总览"
    periods, series = get_company_series(company_id)
    _emit_progress(progress_callback, 0.06, "prepare", "公司基础数据已加载，正在读取历史时间轴...")
    _emit_progress(progress_callback, 0.10, "history", f"正在构建近 {int(history_window)} 季历史数据与增长结构...")
    history = build_historical_quarter_cube(company_id, calendar_quarter, history_window, periods=periods, series=series)
    fixture = get_quarter_fixture(company_id, calendar_quarter) or _build_generic_fixture(company, calendar_quarter, history, series)
    fixture = dict(fixture)
    _emit_progress(progress_callback, 0.15, "history", "历史趋势已就绪，正在准备季度上下文...")
    _emit_progress(progress_callback, 0.18, "sources", "正在解析并定位官方财报、演示稿与 SEC 源...")
    fixture["sources"] = resolve_official_sources(
        company,
        calendar_quarter,
        fixture.get("period_end") or _estimate_period_end(calendar_quarter),
        list(fixture.get("sources") or []),
        refresh=refresh_source_materials,
        progress_callback=_stage_progress_callback(
            progress_callback,
            stage="sources",
            start=0.18,
            end=0.31,
        ),
    )
    _emit_progress(progress_callback, 0.32, "materials", "正在抓取或读取官方原文材料缓存...")
    source_materials = hydrate_source_materials(
        company_id,
        calendar_quarter,
        list(fixture.get("sources") or []),
        refresh=refresh_source_materials,
        progress_callback=_stage_progress_callback(
            progress_callback,
            stage="materials",
            start=0.32,
            end=0.56,
        ),
    )
    fixture["materials"] = _material_tokens_from_sources(fixture, source_materials)
    _emit_progress(progress_callback, 0.57, "parse", "正在从官方原文动态解析 KPI、结构和管理层表述...")
    parsed_fixture = parse_official_materials(
        company,
        fixture,
        source_materials,
        progress_callback=_stage_progress_callback(
            progress_callback,
            stage="parse",
            start=0.57,
            end=0.77,
        ),
    )
    if _guidance_revenue_signal_rank(dict(parsed_fixture.get("guidance") or {})) < 2:
        embedded_guidance_sources = _guidance_related_sources_from_materials(company, source_materials)
        if embedded_guidance_sources:
            existing_source_urls = {str(item.get("url") or "") for item in list(fixture.get("sources") or [])}
            appended_sources = [
                item for item in embedded_guidance_sources if str(item.get("url") or "") not in existing_source_urls
            ]
            if appended_sources:
                _emit_progress(progress_callback, 0.735, "materials", "正在根据正文内嵌线索补抓电话会与展望材料...")
                enriched_sources = list(fixture.get("sources") or []) + appended_sources
                enriched_materials = hydrate_source_materials(
                    company_id,
                    calendar_quarter,
                    enriched_sources,
                    refresh=refresh_source_materials,
                    progress_callback=_stage_progress_callback(
                        progress_callback,
                        stage="materials",
                        start=0.735,
                        end=0.77,
                    ),
                )
                reparsed_fixture = parse_official_materials(
                    company,
                    fixture,
                    enriched_materials,
                )
                if _guidance_revenue_signal_rank(dict(reparsed_fixture.get("guidance") or {})) >= _guidance_revenue_signal_rank(
                    dict(parsed_fixture.get("guidance") or {})
                ):
                    parsed_fixture = reparsed_fixture
                fixture["sources"] = enriched_sources
                source_materials = enriched_materials
    _emit_progress(progress_callback, 0.79, "normalize", "正在合并官方解析结果并校验异常值...")
    fixture = _merge_fixture_payload(fixture, parsed_fixture)
    fixture["current_segments"] = _normalize_segment_items(company, list(fixture.get("current_segments") or []))
    fixture = _sanitize_fixture_payload(company, fixture, history[-1])
    fixture = _quarterize_fixture_geographies(fixture)
    fixture = _promote_geographies_as_segments(company, fixture)
    history = _refresh_latest_history_entry(company, history, fixture)
    _emit_progress(progress_callback, 0.81, "history", f"正在用官方结构补齐近 {int(history_window)} 季口径...")
    history = _enrich_history_with_official_structures(
        company,
        history,
        progress_callback=_stage_progress_callback(
            progress_callback,
            stage="history",
            start=0.81,
            end=0.91,
        ),
    )
    history = _recompute_history_derivatives(history)
    history = _sanitize_history_quality_metrics(history)
    history = _backfill_historical_core_metrics(history)
    history = _promote_history_geographies_as_segments(history)
    history = _harmonize_historical_structures(company, history)
    history = _backfill_historical_segment_history(company, history)
    history = _backfill_historical_geography_history(history)
    history = _promote_history_geographies_as_segments(history)
    history = _harmonize_historical_structures(company, history)
    _emit_progress(progress_callback, 0.915, "history", "正在统一历史业务结构并补足缺口...")
    fixture = _rehydrate_current_structures_from_history(company, fixture, history[-1])
    fixture["current_segments"] = _normalize_segment_items(company, list(fixture.get("current_segments") or []))
    fixture = _sanitize_fixture_payload(company, fixture, history[-1])
    fixture = _quarterize_fixture_geographies(fixture)
    fixture = _promote_geographies_as_segments(company, fixture)
    fixture = _quarterize_fixture_geographies(fixture)
    fixture = _refresh_fixture_growth_metrics_from_history(company, fixture, history)
    fixture["guidance"] = _resolve_guidance_payload(fixture["guidance"], history)
    fixture["current_geographies"] = list(fixture.get("current_geographies") or [])
    fixture["headline"] = _compose_summary_headline(
        company,
        str(fixture.get("fiscal_label") or calendar_quarter),
        fixture.get("latest_kpis", {}),
        history[-1],
        fixture,
    )
    natural_quarter = resolve_calendar_quarter_from_months(fixture["coverage_months"])
    if natural_quarter != calendar_quarter:
        raise ValueError(f"Quarter mapping mismatch: expected {natural_quarter}, got {calendar_quarter}")

    structure_dimension = resolve_structure_dimension(company_id, history)
    transcript_summary = _transcript_summary(manual_transcript_upload_id) or _automatic_transcript_summary(source_materials)
    qna_topics = transcript_summary["topics"] if transcript_summary and transcript_summary["topics"] else fixture["qna_themes"]
    qna_topics = _stabilize_topic_cards(
        qna_topics,
        list(fixture.get("qna_themes") or [])
        + list(fixture.get("management_themes") or [])
        + list(fixture.get("risks") or [])
        + list(fixture.get("catalysts") or []),
        minimum=4,
    )
    qna_topics = _ensure_minimum_qna_topics(
        qna_topics,
        list(fixture.get("management_themes") or []),
        list(fixture.get("risks") or []),
        list(fixture.get("catalysts") or []),
    )
    if transcript_summary is None:
        transcript_summary = _official_material_proxy_summary(fixture, source_materials, qna_topics)
    narrative_provenance = _build_narrative_provenance(fixture, source_materials, transcript_summary, qna_topics)
    merged_sources = _merge_sources_with_materials(list(fixture.get("sources") or []), source_materials)
    coverage_warnings = _source_material_warnings(source_materials) + list(fixture["coverage_notes"])
    if structure_dimension != "segment":
        if full_coverage_mode:
            coverage_warnings.append(
                "历史结构页当前以地区视角呈现，结构迁移分析仍保持连续时序口径。"
                if structure_dimension == "geography"
                else "历史结构页当前以总量经营视角呈现，结构迁移分析仍保持连续时序口径。"
            )
        else:
            coverage_warnings.append(
                "历史分部连续性不足，结构迁移与增量贡献页已自动切换到地区结构或降级版式。"
                if structure_dimension == "geography"
                else "历史分部连续性不足，结构迁移与增量贡献页已使用降级版式。"
            )
    if full_coverage_mode:
        legacy_downgrade_markers = (
            "降级版式",
            "自动降级",
            "结构迁移页将使用降级版式",
            "结构迁移与增量贡献页采用总量成长与结构限制说明",
            "结构限制说明",
        )
        coverage_warnings = [
            item
            for item in coverage_warnings
            if not any(marker in str(item) for marker in legacy_downgrade_markers)
        ]
    if transcript_summary and transcript_summary.get("source_type") == "manual_transcript":
        coverage_warnings.insert(0, f"已应用手动上传 transcript：{transcript_summary['filename']}")
    elif transcript_summary and transcript_summary.get("source_type") == "official_call_material":
        coverage_warnings.insert(0, f"已自动提取官方电话会材料：{transcript_summary['filename']}")
    elif transcript_summary and transcript_summary.get("source_type") == "official_material_proxy":
        coverage_warnings.insert(0, f"已基于官方财报材料构建电话会代理摘要：{transcript_summary['filename']}")
    if narrative_provenance["qna"]["status"] == "official_call_material":
        coverage_warnings.insert(0, "当前问答主题优先依据官方电话会材料整理，并非静态模板。")
    elif narrative_provenance["qna"]["status"] == "official_material_proxy":
        coverage_warnings.insert(0, "电话会页重点来自财报稿、演示材料和 filing 的交叉整理。")
    elif narrative_provenance["qna"]["status"] == "official_material_inferred":
        coverage_warnings.insert(0, "这页的问题清单主要根据财报稿和 filing 整理。")
    elif narrative_provenance["qna"]["status"] == "structured_fallback":
        coverage_warnings.insert(0, "原始材料还不够完整，这页只保留最核心的问题。")
    _emit_progress(progress_callback, 0.92, "views", "正在整理机构观点与研究辅助视角...")
    institutional_views = get_institutional_views(
        company,
        calendar_quarter,
        fixture.get("release_date"),
        refresh=refresh_source_materials,
        progress_callback=_stage_progress_callback(
            progress_callback,
            stage="views",
            start=0.92,
            end=0.965,
        ),
    )
    if institutional_views:
        coverage_warnings.append("机构观点页来自公开媒体转述的头部 sell-side / 投行观点，仅作辅助参考，不替代官方披露。")
    else:
        coverage_warnings.append("当前未稳定抓到可追溯的机构观点条目，机构视角页会自动保留方法说明并避免伪造内容。")
    currency_code = str(company.get("currency_code") or "USD")
    if currency_code != "USD":
        coverage_warnings.insert(0, f"金额按 {currency_code} 报告币种展示，未做外汇折算。")
    coverage_warnings = _reconcile_coverage_warnings(
        coverage_warnings,
        fixture=fixture,
        source_materials=source_materials,
        structure_dimension=structure_dimension,
        institutional_views=institutional_views,
    )
    coverage_warnings = _writer_humanize_support_lines(coverage_warnings)

    brand = company["brand"]
    money_symbol = company.get("money_symbol", "$")
    metric_rows = _build_metric_rows(company, history, money_symbol)
    current_detail_cards = _build_current_detail_cards(company, fixture, history, structure_dimension, money_symbol)
    cash_panel = _build_cash_quality_panel(company, fixture, history[-1], money_symbol)
    guidance_panel = _build_guidance_panel(fixture, history[-1], money_symbol)
    guidance_change_panel = _build_guidance_change_panel(
        company_id,
        calendar_quarter,
        fixture,
        history,
        money_symbol,
        allow_expensive_parse=False,
    )
    expectation_panel = _build_expectation_panel(fixture, history, money_symbol)
    call_panel = _build_call_panel(fixture, transcript_summary, qna_topics, narrative_provenance)
    history_summary_cards = _build_history_summary_cards(history, money_symbol)
    takeaways = _normalize_takeaways(fixture["takeaways"], fixture["latest_kpis"], history[-1], money_symbol)
    layered_takeaways = _build_layered_takeaways(company, fixture, history, money_symbol, merged_sources)
    institutional_digest = _build_institutional_digest(institutional_views)
    income_statement = _build_income_statement_snapshot(company, fixture, history)
    capital_allocation = _build_capital_allocation_snapshot(company, fixture, history, money_symbol)
    expectation_reset_input = dict(fixture)
    expectation_reset_input["guidance_change_panel"] = guidance_change_panel
    expectation_reset_input["expectation_panel"] = expectation_panel
    expectation_reset = _build_expectation_reset_snapshot(
        company_id,
        calendar_quarter,
        expectation_reset_input,
        history,
        money_symbol,
        allow_expensive_parse=False,
    )
    balance_quality = _build_balance_quality_snapshot(company, fixture, history, money_symbol)
    mix_page_title = _mix_page_title(
        list(fixture.get("current_segments") or []),
        list(fixture.get("current_geographies") or []),
    )
    risk_source = _pick_best_source(
        merged_sources,
        preferred_kinds=("official_release", "presentation", "sec_filing", "structured_financials"),
        preferred_roles=("earnings_call", "earnings_release"),
    )
    catalyst_source = _pick_best_source(
        merged_sources,
        preferred_kinds=("official_release", "presentation", "sec_filing", "structured_financials"),
        preferred_roles=("earnings_call", "earnings_release"),
    )
    risk_scenarios = _build_signal_scenarios(
        list(fixture.get("risks") or []),
        direction="risk",
        latest=history[-1],
        history=history,
        guidance=fixture["guidance"],
        source_anchor=_format_source_anchor(risk_source, narrative_provenance["risks"]["label"]),
        money_symbol=money_symbol,
    )
    catalyst_scenarios = _build_signal_scenarios(
        list(fixture.get("catalysts") or []),
        direction="catalyst",
        latest=history[-1],
        history=history,
        guidance=fixture["guidance"],
        source_anchor=_format_source_anchor(catalyst_source, narrative_provenance["catalysts"]["label"]),
        money_symbol=money_symbol,
    )
    validation_checklist = _build_validation_snapshot(fixture, qna_topics, risk_scenarios, catalyst_scenarios)
    uses_official_context = _guidance_uses_official_context(fixture["guidance"])
    if fixture["guidance"]["mode"] == "official":
        guidance_title = "业绩指引页 · 下一季怎么看"
        guidance_note = "把这季已经兑现的结果和管理层正式给出的下一季口径放在一起看。"
        guidance_chart_title = "当前业绩与下一季指引"
    elif fixture["guidance"]["mode"] == "official_context":
        guidance_title = "业绩指引页 · 管理层怎么描绘下一步"
        guidance_note = "公司没有给出明确数字时，这一页保留管理层原本的展望语气，再拿近几季常态做参照。"
        guidance_chart_title = "当前季度与管理层展望"
    else:
        guidance_title = "业绩指引页 · 后续经营参照"
        guidance_note = "还没有明确指引时，这一页先拿近几季常态做参照，看看管理层后面的经营方向有没有偏离。"
        guidance_chart_title = "当前业绩与后续经营参照"
    guidance_note = _writer_polish_generated_text(f"{guidance_note} 重点看管理层口径和本季兑现是不是站在同一边。")
    section_meta = _build_dynamic_section_meta(
        company,
        fixture,
        history,
        structure_dimension=structure_dimension,
        money_symbol=money_symbol,
        institutional_views=institutional_views,
        transcript_summary=transcript_summary,
        qna_topics=qna_topics,
        merged_sources=merged_sources,
        guidance_note=guidance_note,
    )
    _emit_progress(progress_callback, 0.97, "visuals", "正在排版图表、结论卡片与 PDF 预览内容...")
    visuals = {
        "company_brand": render_company_wordmark_svg(str(company["id"]), str(company["english_name"]), brand["primary"]),
        "current_quarter": render_current_quarter_svg(metric_rows, brand["primary"], brand["secondary"]),
        "guidance": render_guidance_svg(
            fixture["latest_kpis"]["revenue_bn"],
            fixture["guidance"]["revenue_bn"],
            _metric_or_fallback(fixture["latest_kpis"]["gaap_gross_margin_pct"], history[-1].get("net_margin_pct")) or 0.0,
            fixture["guidance"].get(
                "gaap_gross_margin_pct",
                fixture["guidance"].get(
                    "adjusted_ebitda_margin_pct",
                    _metric_or_fallback(fixture["latest_kpis"]["gaap_gross_margin_pct"], history[-1].get("net_margin_pct")) or 0.0,
                ),
            ),
            brand["primary"],
            brand["accent"],
            money_symbol=money_symbol,
            current_label=fixture["guidance"].get("current_label", "本季收入"),
            comparison_label=fixture["guidance"].get("comparison_label", "下一季收入指引"),
            current_margin_label=fixture["guidance"].get("current_margin_label", "本季毛利率"),
            comparison_margin_label=fixture["guidance"].get("comparison_margin_label", "下一季毛利率指引"),
            chart_title=guidance_chart_title,
        ),
        "segment_mix": render_segment_mix_svg(
            fixture["current_segments"],
            fixture["current_geographies"],
            brand["segment_colors"],
            brand["primary"],
            money_symbol=money_symbol,
        ),
        "income_statement": render_income_statement_svg(income_statement, brand["segment_colors"], brand["primary"], money_symbol=money_symbol),
        "statement_translation": render_statement_translation_svg(
            income_statement,
            brand["primary"],
            brand["accent"],
            money_symbol=money_symbol,
        ),
        "management_qna": render_dual_ranked_svg(
            "管理层重点" if uses_official_context else "研究关注重点",
            fixture["management_themes"],
            brand["primary"],
            narrative_provenance["qna_chart_title"] if uses_official_context else "市场下一步会追问什么",
            qna_topics,
            brand["secondary"],
            left_subtitle=narrative_provenance["management_chart_subtitle"] if uses_official_context else "结合当季披露和历史表现整理",
            right_subtitle=narrative_provenance["qna_chart_subtitle"] if uses_official_context else "更像下一步该继续追的问题清单",
        ),
        "risks_catalysts": render_dual_ranked_svg(
            "主要风险",
            fixture["risks"],
            "#F97316",
            "主要催化剂",
            fixture["catalysts"],
            brand["primary"],
            left_subtitle="按影响程度排序",
            right_subtitle="按边际改善弹性排序",
        ),
        "growth_overview": render_growth_overview_svg(
            history,
            brand["segment_colors"],
            brand["primary"],
            money_symbol=money_symbol,
            title=historical_growth_title,
        ),
        "structure_transition": render_structure_transition_svg(
            history,
            brand["segment_colors"],
            brand["primary"],
            "连续 12 季可比的分部披露还不够完整，所以这一页改从结构线索和管理层判断去看变化。",
        ),
        "profitability": render_profitability_svg(
            history,
            brand["primary"],
            brand["secondary"],
            profit_margin_label=company.get("historical_profit_margin_label", "净利率"),
        ),
        "contribution": render_contribution_svg(
            history,
            brand["segment_colors"],
            brand["primary"],
            "连续季度分部明细还不够完整，所以这一页先回答增长是靠总量扩张，还是靠利润质量改善。",
            money_symbol=money_symbol,
        ),
        "capital_allocation": render_capital_allocation_svg(capital_allocation, brand["primary"], brand["accent"], money_symbol=money_symbol),
        "expectation_reset": render_expectation_reset_svg(expectation_reset, brand["primary"], brand["secondary"], money_symbol=money_symbol),
        "balance_quality": render_balance_quality_svg(balance_quality, brand["primary"], brand["secondary"], money_symbol=money_symbol),
        "validation_checklist": render_validation_checklist_svg(validation_checklist, "#F97316", brand["primary"]),
    }

    latest_history = history[-1]
    previous_history = history[-2] if len(history) > 1 else history[-1]
    year_ago_history = history[-5] if len(history) > 4 else history[-1]
    insights_fixture = dict(fixture)
    insights_fixture["structure_dimension"] = structure_dimension
    call_quote_cards = _prepare_call_quote_cards(list(fixture.get("call_quote_cards", [])))
    _emit_progress(progress_callback, 0.985, "visuals", "图表与卡片已完成，正在收束页面内容...")
    _emit_progress(progress_callback, 0.993, "assemble", "正在封装报告内容并写入缓存...")
    coverage_warnings = _reconcile_coverage_warnings(
        coverage_warnings,
        fixture=fixture,
        source_materials=source_materials,
        structure_dimension=structure_dimension,
        institutional_views=institutional_views,
    )
    coverage_warnings = _writer_humanize_support_lines(coverage_warnings)
    payload = {
        "payload_schema_version": REPORT_PAYLOAD_SCHEMA_VERSION,
        "full_coverage_required": full_coverage_mode,
        "company": company,
        "report_style": report_style,
        "section_meta": section_meta,
        "calendar_quarter": calendar_quarter,
        "fiscal_label": fixture["fiscal_label"],
        "headline": fixture["headline"],
        "release_date": fixture["release_date"],
        "period_end": fixture["period_end"],
        "money_symbol": money_symbol,
        "currency_code": company.get("currency_code", "USD"),
        "structure_dimension_used": structure_dimension,
        "coverage_warnings": coverage_warnings,
        "takeaways": takeaways,
        "layered_takeaways": layered_takeaways,
        "scoreboard": _build_scoreboard(company, fixture, history, structure_dimension, money_symbol),
        "latest_kpis": fixture["latest_kpis"],
        "guidance": fixture["guidance"],
        "guidance_title": guidance_title,
        "guidance_note": guidance_note,
        "historical_growth_title": historical_growth_title,
        "mix_page_title": mix_page_title,
        "guidance_panel": guidance_panel,
        "guidance_change_panel": guidance_change_panel,
        "expectation_panel": expectation_panel,
        "current_segments": fixture["current_segments"],
        "current_geographies": fixture["current_geographies"],
        "income_statement": income_statement,
        "current_detail_cards": current_detail_cards,
        "cash_panel": cash_panel,
        "call_panel": call_panel,
        "call_quote_cards": call_quote_cards,
        "narrative_provenance": narrative_provenance,
        "institutional_views": institutional_views,
        "institutional_digest": institutional_digest,
        "capital_allocation": capital_allocation,
        "expectation_reset": expectation_reset,
        "balance_quality": balance_quality,
        "validation_checklist": validation_checklist,
        "management_themes": fixture["management_themes"],
        "qna_themes": qna_topics,
        "risks": fixture["risks"],
        "catalysts": fixture["catalysts"],
        "risk_scenarios": risk_scenarios,
        "catalyst_scenarios": catalyst_scenarios,
        "evidence_cards": fixture["evidence_cards"],
        "transcript_summary": transcript_summary,
        "historical_cube": history,
        "historical_insights": generate_historical_insights(
            history,
            insights_fixture,
            money_symbol,
            profit_margin_label=company.get("historical_profit_margin_label", "净利率"),
        ),
        "historical_summary_cards": history_summary_cards,
        "comparison": {
            "latest": latest_history,
            "previous": previous_history,
            "year_ago": year_ago_history,
        },
        "visuals": visuals,
        "sources": merged_sources,
        "source_materials": source_materials,
        "page_count": 20,
        "generated_at": now_iso(),
    }
    payload = _refresh_guidance_from_cached_materials_if_needed(payload, company, history, money_symbol)
    quality_report = evaluate_report_payload(
        payload,
        history_window=history_window,
        require_full_coverage=full_coverage_mode,
    )
    payload["quality_report"] = quality_report
    payload["accuracy_report"] = _evaluate_report_accuracy(payload)
    return payload


def _accuracy_issue(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _display_value_missing(value: Any) -> bool:
    cleaned = str(value or "").strip()
    if not cleaned:
        return True
    normalized = cleaned.casefold()
    if normalized in {"-", "—", "–", "未披露", "官方展望", "经营口径"}:
        return True
    if any(token in cleaned for token in ("同比 -", "环比 -", "$-", "¥-", "€-")):
        return True
    return False


def _evaluate_report_accuracy(payload: dict[str, Any]) -> dict[str, Any]:
    latest_kpis = dict(payload.get("latest_kpis") or {})
    expectation_reset = dict(payload.get("expectation_reset") or {})
    guidance = dict(payload.get("guidance") or {})
    issues: list[dict[str, str]] = []

    revenue_bn = latest_kpis.get("revenue_bn")
    net_income_bn = latest_kpis.get("net_income_bn")
    operating_cash_flow_bn = latest_kpis.get("operating_cash_flow_bn")
    free_cash_flow_bn = latest_kpis.get("free_cash_flow_bn")
    capital_expenditures_bn = latest_kpis.get("capital_expenditures_bn")

    if revenue_bn is None:
        issues.append(_accuracy_issue("latest_revenue_missing", "最新季度收入缺失，无法输出准确报告。"))
    if net_income_bn is None:
        issues.append(_accuracy_issue("latest_net_income_missing", "最新季度净利润缺失，无法输出准确报告。"))
    if operating_cash_flow_bn is None:
        issues.append(_accuracy_issue("capital_allocation_ocf_missing", "经营现金流缺失，现金流与资本配置页无法准确输出。"))
    if free_cash_flow_bn is None:
        issues.append(_accuracy_issue("capital_allocation_fcf_missing", "自由现金流缺失，现金流与资本配置页无法准确输出。"))

    implied_capex_bn = capital_expenditures_bn
    if implied_capex_bn is None and operating_cash_flow_bn is not None and free_cash_flow_bn is not None:
        implied_capex_bn = round(max(float(operating_cash_flow_bn) - float(free_cash_flow_bn), 0.0), 3)
    if implied_capex_bn is None:
        issues.append(_accuracy_issue("capital_allocation_capex_missing", "CapEx 口径缺失，现金流与资本配置页无法准确输出。"))
    elif operating_cash_flow_bn is not None and free_cash_flow_bn is not None:
        fcf_gap = abs(float(operating_cash_flow_bn) - float(free_cash_flow_bn) - float(implied_capex_bn))
        tolerance = max(0.25, abs(float(implied_capex_bn)) * 0.08)
        if fcf_gap > tolerance:
            issues.append(_accuracy_issue("capital_allocation_formula_mismatch", "OCF、FCF 与 CapEx 之间的勾稽关系不成立，现金流页口径不可信。"))

    cards = [dict(item) for item in list(expectation_reset.get("cards") or []) if isinstance(item, dict)]
    previous_guidance_bn = expectation_reset.get("previous_guidance_bn")
    current_guidance_bn = expectation_reset.get("current_guidance_bn")
    current_guidance_has_verified_baseline = bool(guidance.get("revenue_derived_from_baseline")) and guidance.get("revenue_bn") is not None
    if len(cards) < 3:
        issues.append(_accuracy_issue("expectation_reset_cards_missing", "预期差与指引变化页卡片不完整，无法准确输出。"))
    else:
        if previous_guidance_bn is None or _display_value_missing(cards[0].get("value")):
            issues.append(_accuracy_issue("expectation_reset_previous_missing", "上一季可比口径缺失，预期差页面无法准确对照。"))
        if (current_guidance_bn is None and not current_guidance_has_verified_baseline) or _display_value_missing(cards[2].get("value")):
            issues.append(_accuracy_issue("expectation_reset_current_missing", "当前阶段口径缺失，预期差页面无法准确输出。"))

    status = "pass" if not issues else "fail"
    summary = "PASS | verified numeric sections complete" if status == "pass" else f"FAIL | {len(issues)} blocking accuracy issue(s)"
    return {
        "status": status,
        "issues": issues,
        "summary": summary,
    }


def _existing_report(company_id: str, calendar_quarter: str, history_window: int) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT * FROM reports
            WHERE company_id = ? AND calendar_quarter = ? AND history_window = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (company_id, calendar_quarter, history_window),
        ).fetchone()
    if row is None:
        return None
    record = dict(row)
    record["payload"] = json.loads(record["payload_json"])
    record["coverage_warnings"] = json.loads(record["coverage_warnings_json"])
    return record


def _payload_schema_version(payload: dict[str, Any]) -> int:
    try:
        return int(payload.get("payload_schema_version") or 0)
    except (TypeError, ValueError):
        return 0


def _latest_report_for_key(
    company_id: str,
    calendar_quarter: str,
    history_window: int,
) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT * FROM reports
            WHERE company_id = ? AND calendar_quarter = ? AND history_window = ?
            ORDER BY created_at DESC
            """,
            (company_id, calendar_quarter, history_window),
        ).fetchall()
    for row in rows:
        record = dict(row)
        record["payload"] = json.loads(record["payload_json"])
        record["coverage_warnings"] = json.loads(record["coverage_warnings_json"])
        if _payload_schema_version(record["payload"]) >= REPORT_PAYLOAD_SCHEMA_VERSION:
            return record
    return None


def resolve_canonical_report_id(report_id: str) -> Optional[str]:
    try:
        current = get_report(report_id)
    except KeyError:
        return None
    latest = _latest_report_for_key(
        str(current.get("company_id") or ""),
        str(current.get("calendar_quarter") or ""),
        int(current.get("history_window") or 12),
    )
    if latest is None:
        return report_id
    return str(latest["id"])


def _report_cache_is_fresh(record: dict[str, Any]) -> bool:
    updated_at = str(record.get("updated_at") or record.get("created_at") or "")
    if not updated_at:
        return False
    payload = dict(record.get("payload") or {})
    if _payload_schema_version(payload) < REPORT_PAYLOAD_SCHEMA_VERSION:
        return False
    accuracy_report = dict(payload.get("accuracy_report") or _evaluate_report_accuracy(payload))
    if str(accuracy_report.get("status") or "fail") != "pass":
        return False
    if _report_payload_has_financial_inconsistency(payload):
        return False
    if _payload_contains_placeholder_topic_cards(payload):
        return False
    history = [dict(item) for item in list(payload.get("historical_cube") or []) if isinstance(item, dict)]
    company = dict(payload.get("company") or {})
    if not company and payload.get("company_id"):
        company = get_company(str(payload.get("company_id") or ""))
    if company and history and _guidance_payload_looks_implausible(payload, company, history):
        return False
    try:
        built_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return False
    latest_dependency_mtime = max(path.stat().st_mtime for path in REPORT_CACHE_DEPENDENCIES if path.exists())
    if built_at < latest_dependency_mtime:
        return False
    release_date = _parse_date_token(str(payload.get("release_date") or ""))
    ttl_seconds = HISTORICAL_REPORT_CACHE_TTL_SECONDS
    if release_date is not None and abs((datetime.now().date() - release_date).days) <= 180:
        ttl_seconds = RECENT_REPORT_CACHE_TTL_SECONDS
    age_seconds = datetime.now().timestamp() - built_at
    return age_seconds <= ttl_seconds


def create_report(
    company_id: str,
    calendar_quarter: str,
    history_window: int = 12,
    manual_transcript_upload_id: Optional[str] = None,
    force_refresh: bool = False,
    progress_callback: Optional[ProgressCallback] = None,
) -> dict[str, Any]:
    queued_message = "任务已进入生成队列，正在检查可复用缓存..."
    if force_refresh:
        queued_message = "任务已进入生成队列，本次将强制刷新官方源与原文材料..."
    elif manual_transcript_upload_id:
        queued_message = "任务已进入生成队列，因附带手动 transcript，本次将重新生成报告..."
    _emit_progress(progress_callback, 0.02, "queued", queued_message)
    cache_allowed = not force_refresh and not manual_transcript_upload_id
    if cache_allowed:
        existing = _existing_report(company_id, calendar_quarter, history_window)
        if existing is not None and _report_cache_is_fresh(existing):
            _emit_progress(progress_callback, 1.0, "completed", "已命中近期报告缓存，报告可直接预览。")
            return existing

    payload = build_report_payload(
        company_id,
        calendar_quarter,
        history_window,
        manual_transcript_upload_id,
        refresh_source_materials=force_refresh,
        require_full_coverage=None,
        progress_callback=progress_callback,
    )
    accuracy = dict(payload.get("accuracy_report") or _evaluate_report_accuracy(payload))
    payload["accuracy_report"] = accuracy
    if payload.get("full_coverage_required") and str(accuracy.get("status") or "fail") != "pass":
        issue_messages = [
            str(item.get("message") or "").strip()
            for item in list(accuracy.get("issues") or [])
            if str(item.get("message") or "").strip()
        ]
        top_reasons = "；".join(issue_messages[:3]) if issue_messages else "数值准确性门禁未通过。"
        raise ValueError(f"Verified numeric gate failed: {top_reasons}")

    report_id = uuid.uuid4().hex
    created_at = now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO reports (
              id, company_id, calendar_quarter, history_window, structure_dimension_used,
              coverage_warnings_json, payload_json, html_path, pdf_path, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_id,
                company_id,
                calendar_quarter,
                history_window,
                payload["structure_dimension_used"],
                json_dumps(payload["coverage_warnings"]),
                json_dumps(payload),
                None,
                None,
                created_at,
                created_at,
            ),
        )
    record = get_report(report_id)
    _emit_progress(progress_callback, 1.0, "completed", "报告已生成完成。")
    return record


def get_report(report_id: str) -> dict[str, Any]:
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
    if row is None:
        raise KeyError(f"Report not found: {report_id}")
    record = dict(row)
    record["payload"] = json.loads(record["payload_json"])
    record["coverage_warnings"] = json.loads(record["coverage_warnings_json"])
    return record


def ensure_report_payload_defaults(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    company = dict(normalized.get("company") or {})
    if not company and normalized.get("company_id"):
        company = get_company(str(normalized.get("company_id") or ""))
        normalized["company"] = company
    history = list(normalized.get("historical_cube") or [])
    money_symbol = str(normalized.get("money_symbol") or company.get("money_symbol") or "$")
    if company:
        if history:
            normalized = _repair_report_payload_financial_sections(normalized, company, money_symbol)
            history = list(normalized.get("historical_cube") or history)
            normalized = _refresh_guidance_from_cached_materials_if_needed(normalized, company, history, money_symbol)
            history = list(normalized.get("historical_cube") or history)
            normalized = _refresh_topic_cards_from_cached_materials_if_needed(normalized, company, history)
        if "report_style" not in normalized:
            normalized["report_style"] = _build_report_style(company)
        section_meta = dict(normalized.get("section_meta") or {})
        base_section_meta = _build_section_meta(company)
        for key, value in base_section_meta.items():
            if key not in section_meta:
                section_meta[key] = value
                continue
            merged_value = dict(value)
            merged_value.update(dict(section_meta.get(key) or {}))
            section_meta[key] = merged_value
        guidance_note = str(normalized.get("guidance_note") or section_meta["guidance"]["note"])
        if guidance_note:
            section_meta["guidance"]["note"] = guidance_note
        normalized["section_meta"] = section_meta
        normalized["management_themes"] = _stabilize_topic_cards(
            [dict(item) for item in list(normalized.get("management_themes") or []) if isinstance(item, dict)],
            [dict(item) for item in list(normalized.get("qna_themes") or []) if isinstance(item, dict)]
            + [dict(item) for item in list(normalized.get("risks") or []) if isinstance(item, dict)]
            + [dict(item) for item in list(normalized.get("catalysts") or []) if isinstance(item, dict)],
            minimum=4,
        )
        normalized["qna_themes"] = _stabilize_topic_cards(
            [dict(item) for item in list(normalized.get("qna_themes") or []) if isinstance(item, dict)],
            list(normalized.get("management_themes") or [])
            + [dict(item) for item in list(normalized.get("risks") or []) if isinstance(item, dict)]
            + [dict(item) for item in list(normalized.get("catalysts") or []) if isinstance(item, dict)],
            minimum=4,
        )
        visuals = dict(normalized.get("visuals") or {})
        narrative_provenance = dict(normalized.get("narrative_provenance") or {})
        guidance = dict(normalized.get("guidance") or {})
        uses_official_context = _guidance_uses_official_context(guidance)
        if "company_brand" not in visuals:
            visuals["company_brand"] = render_company_wordmark_svg(
                str(company.get("id") or ""),
                str(company.get("english_name") or company.get("name") or ""),
                str(((company.get("brand") or {}).get("primary")) or "#0F172A"),
            )
        brand = dict(company.get("brand") or {})
        visuals["management_qna"] = render_dual_ranked_svg(
            "管理层重点" if uses_official_context else "研究关注重点",
            list(normalized.get("management_themes") or []),
            str(brand.get("primary") or "#0F172A"),
            str(narrative_provenance.get("qna_chart_title") or ("电话会里最重要的问题" if uses_official_context else "市场下一步会追问什么")),
            list(normalized.get("qna_themes") or []),
            str(brand.get("secondary") or "#94A3B8"),
            left_subtitle=str(
                narrative_provenance.get("management_chart_subtitle")
                or ("按管理层原文和财报披露整理" if uses_official_context else "结合当季披露和历史表现整理")
            ),
            right_subtitle=str(
                narrative_provenance.get("qna_chart_subtitle")
                or ("直接按电话会原文整理" if uses_official_context else "更像下一步该继续追的问题清单")
            ),
        )
        if history:
            if "capital_allocation" not in normalized:
                normalized["capital_allocation"] = _build_capital_allocation_snapshot(company, normalized, history, money_symbol)
            if "expectation_reset" not in normalized:
                normalized["expectation_reset"] = _build_expectation_reset_snapshot(
                    str(company.get("id") or normalized.get("company_id") or ""),
                    str(normalized.get("calendar_quarter") or ""),
                    normalized,
                    history,
                    money_symbol,
                )
            if "balance_quality" not in normalized:
                normalized["balance_quality"] = _build_balance_quality_snapshot(company, normalized, history, money_symbol)
            if "validation_checklist" not in normalized:
                normalized["validation_checklist"] = _build_validation_snapshot(
                    normalized,
                    list(normalized.get("qna_themes") or []),
                    list(normalized.get("risk_scenarios") or []),
                    list(normalized.get("catalyst_scenarios") or []),
                )
            if "capital_allocation" not in visuals:
                visuals["capital_allocation"] = render_capital_allocation_svg(
                    normalized.get("capital_allocation") or {},
                    str(brand.get("primary") or "#0F172A"),
                    str(brand.get("accent") or "#16A34A"),
                    money_symbol=money_symbol,
                )
            if "expectation_reset" not in visuals:
                visuals["expectation_reset"] = render_expectation_reset_svg(
                    normalized.get("expectation_reset") or {},
                    str(brand.get("primary") or "#0F172A"),
                    str(brand.get("secondary") or "#94A3B8"),
                    money_symbol=money_symbol,
                )
            if "balance_quality" not in visuals:
                visuals["balance_quality"] = render_balance_quality_svg(
                    normalized.get("balance_quality") or {},
                    str(brand.get("primary") or "#0F172A"),
                    str(brand.get("secondary") or "#94A3B8"),
                    money_symbol=money_symbol,
                )
            if "validation_checklist" not in visuals:
                visuals["validation_checklist"] = render_validation_checklist_svg(
                    normalized.get("validation_checklist") or {},
                    "#F97316",
                    str(brand.get("primary") or "#0F172A"),
                )
        normalized["visuals"] = visuals
    if "mix_page_title" not in normalized:
        normalized["mix_page_title"] = _mix_page_title(
            list(normalized.get("current_segments") or []),
            list(normalized.get("current_geographies") or []),
        )
    if not isinstance(normalized.get("quality_report"), dict):
        normalized["quality_report"] = evaluate_report_payload(normalized)
    if not isinstance(normalized.get("accuracy_report"), dict):
        normalized["accuracy_report"] = _evaluate_report_accuracy(normalized)
    normalized.setdefault("guidance_change_panel", {"title": "相对上一季口径变化", "bullets": [], "source_anchor": ""})
    normalized.setdefault("risk_scenarios", [])
    normalized.setdefault("catalyst_scenarios", [])
    normalized.setdefault("capital_allocation", {"title": "现金流与资本配置", "cards": [], "bullets": [], "signals": [], "framework": []})
    normalized.setdefault("expectation_reset", {"title": "预期差与指引变化", "cards": [], "bullets": [], "method": [], "source_anchor": ""})
    normalized.setdefault("balance_quality", {"title": "资产负债与经营质量", "cards": [], "signals": [], "bullets": [], "framework": []})
    normalized.setdefault("validation_checklist", {"title": "下一季验证清单", "positive_items": [], "negative_items": [], "bullets": [], "method": []})
    normalized["page_count"] = max(int(normalized.get("page_count") or 0), 20)
    return normalized


def update_report_artifacts(
    report_id: str,
    *,
    html_path: Optional[str] = None,
    pdf_path: Optional[str] = None,
) -> None:
    assignments: list[str] = []
    values: list[Any] = []
    if html_path is not None:
        assignments.append("html_path = ?")
        values.append(html_path)
    if pdf_path is not None:
        assignments.append("pdf_path = ?")
        values.append(pdf_path)
    if not assignments:
        return
    assignments.append("updated_at = ?")
    values.append(now_iso())
    values.append(report_id)
    with get_connection() as connection:
        connection.execute(
            f"UPDATE reports SET {', '.join(assignments)} WHERE id = ?",
            values,
        )


def update_report_pdf(report_id: str, pdf_path: str) -> None:
    update_report_artifacts(report_id, pdf_path=pdf_path)


def _serialize_report_job(row: dict[str, Any]) -> dict[str, Any]:
    report_id = row.get("report_id")
    return {
        "job_id": row["id"],
        "company_id": row["company_id"],
        "calendar_quarter": row["calendar_quarter"],
        "history_window": int(row["history_window"]),
        "status": row["status"],
        "progress": float(row["progress"] or 0.0),
        "stage": row["stage"],
        "message": row["message"],
        "report_id": report_id,
        "error": row.get("error"),
        "preview_url": f"/reports/{report_id}/preview" if report_id else None,
        "export_pdf_url": f"/reports/{report_id}/export.pdf" if report_id else None,
    }


def _remember_report_job_state(job: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(job)
    with REPORT_JOB_STATES_LOCK:
        REPORT_JOB_STATES[str(snapshot.get("job_id") or "")] = snapshot
    _persist_report_job_state(snapshot)
    return snapshot


def _report_job_state_path(job_id: str) -> Path:
    return REPORT_JOB_STATE_DIR / f"{job_id}.json"


def _persist_report_job_state(job: dict[str, Any]) -> None:
    job_id = str(job.get("job_id") or "")
    if not job_id:
        return
    with REPORT_JOB_STATE_FILE_LOCK:
        ensure_directories()
        REPORT_JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)
        payload = dict(job)
        path = _report_job_state_path(job_id)
        temp_path = path.with_name(f"{path.stem}.{threading.get_ident()}.json.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)


def _load_report_job_state_from_disk(job_id: str) -> Optional[dict[str, Any]]:
    path = _report_job_state_path(job_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload["job_id"] = str(payload.get("job_id") or job_id)
    return payload


def _get_report_job_state(job_id: str) -> Optional[dict[str, Any]]:
    with REPORT_JOB_STATES_LOCK:
        job = REPORT_JOB_STATES.get(job_id)
    if job:
        return dict(job)
    disk_job = _load_report_job_state_from_disk(job_id)
    if disk_job is None:
        return None
    return _remember_report_job_state(disk_job)


def _update_report_job_state(
    job_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[float] = None,
    stage: Optional[str] = None,
    message: Optional[str] = None,
    report_id: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    with REPORT_JOB_STATES_LOCK:
        existing = dict(REPORT_JOB_STATES.get(job_id) or {})
        if not existing:
            return
        if status is not None:
            existing["status"] = status
        if progress is not None:
            existing["progress"] = _progress_value(progress)
        if stage is not None:
            existing["stage"] = stage
        if message is not None:
            existing["message"] = message
        if report_id is not None:
            existing["report_id"] = report_id
            existing["preview_url"] = f"/reports/{report_id}/preview" if report_id else None
            existing["export_pdf_url"] = f"/reports/{report_id}/export.pdf" if report_id else None
        if error is not None:
            existing["error"] = error
        REPORT_JOB_STATES[job_id] = existing
    _persist_report_job_state(existing)


def _restore_report_job_row(job: dict[str, Any], *, connection: Any | None = None) -> None:
    job_id = str(job.get("job_id") or "")
    if not job_id:
        return
    created_at = str(job.get("created_at") or now_iso())
    updated_at = str(job.get("updated_at") or created_at)
    def _execute(target_connection: Any) -> None:
        target_connection.execute(
            """
            INSERT OR REPLACE INTO report_jobs (
              id, company_id, calendar_quarter, history_window, manual_transcript_upload_id,
              force_refresh, status, progress, stage, message, report_id, error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                str(job.get("company_id") or ""),
                str(job.get("calendar_quarter") or ""),
                int(job.get("history_window") or 12),
                job.get("manual_transcript_upload_id"),
                1 if job.get("force_refresh") else 0,
                str(job.get("status") or "queued"),
                _progress_value(job.get("progress")),
                str(job.get("stage") or "queued"),
                str(job.get("message") or ""),
                job.get("report_id"),
                job.get("error"),
                created_at,
                updated_at,
            ),
        )

    if connection is not None:
        _execute(connection)
        return
    with get_connection() as owned_connection:
        _execute(owned_connection)


def _report_job_has_live_future(job_id: str) -> bool:
    with REPORT_JOB_FUTURES_LOCK:
        future = REPORT_JOB_FUTURES.get(job_id)
        if future is None:
            return False
        future_done = getattr(future, "done", None)
        if callable(future_done) and future_done():
            REPORT_JOB_FUTURES.pop(job_id, None)
            return False
        return True


def _submit_report_job_from_state(job: dict[str, Any]) -> bool:
    job_id = str(job.get("job_id") or "")
    if not job_id:
        return False
    with REPORT_JOB_FUTURES_LOCK:
        existing = REPORT_JOB_FUTURES.get(job_id)
        existing_done = getattr(existing, "done", None) if existing is not None else None
        if existing is not None and (not callable(existing_done) or not existing_done()):
            return False
        future = REPORT_JOB_EXECUTOR.submit(
            _run_report_job,
            job_id,
            company_id=str(job.get("company_id") or ""),
            calendar_quarter=str(job.get("calendar_quarter") or ""),
            history_window=int(job.get("history_window") or 12),
            manual_transcript_upload_id=job.get("manual_transcript_upload_id"),
            force_refresh=bool(job.get("force_refresh")),
        )
        REPORT_JOB_FUTURES[job_id] = future
    return True


def _resume_report_job_if_needed(job: dict[str, Any]) -> dict[str, Any]:
    status = str(job.get("status") or "")
    if status not in {"queued", "running"}:
        return job
    job_id = str(job.get("job_id") or "")
    if not job_id or _report_job_has_live_future(job_id):
        return job
    _submit_report_job_from_state(job)
    return job


def get_report_job(job_id: str) -> dict[str, Any]:
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM report_jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        remembered = _get_report_job_state(job_id)
        if remembered is None:
            raise KeyError(f"Report job not found: {job_id}")
        _restore_report_job_row(remembered)
        return _resume_report_job_if_needed(remembered)
    return _resume_report_job_if_needed(_remember_report_job_state(_serialize_report_job(dict(row))))


def _update_report_job(
    job_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[float] = None,
    stage: Optional[str] = None,
    message: Optional[str] = None,
    report_id: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    _update_report_job_state(
        job_id,
        status=status,
        progress=progress,
        stage=stage,
        message=message,
        report_id=report_id,
        error=error,
    )
    assignments: list[str] = []
    values: list[Any] = []
    if status is not None:
        assignments.append("status = ?")
        values.append(status)
    if progress is not None:
        assignments.append("progress = ?")
        values.append(_progress_value(progress))
    if stage is not None:
        assignments.append("stage = ?")
        values.append(stage)
    if message is not None:
        assignments.append("message = ?")
        values.append(message)
    if report_id is not None:
        assignments.append("report_id = ?")
        values.append(report_id)
    if error is not None:
        assignments.append("error = ?")
        values.append(error)
    assignments.append("updated_at = ?")
    values.append(now_iso())
    values.append(job_id)
    with get_connection() as connection:
        cursor = connection.execute(f"UPDATE report_jobs SET {', '.join(assignments)} WHERE id = ?", values)
        if cursor.rowcount == 0:
            remembered = _get_report_job_state(job_id)
            if remembered is not None:
                _restore_report_job_row(remembered, connection=connection)
                connection.execute(f"UPDATE report_jobs SET {', '.join(assignments)} WHERE id = ?", values)


def _job_progress_callback(job_id: str) -> ProgressCallback:
    def _callback(progress: float, stage: str, message: str) -> None:
        current_job = get_report_job(job_id)
        status = current_job["status"]
        if status not in {"completed", "failed"}:
            status = "running" if stage != "queued" else "queued"
        _update_report_job(
            job_id,
            status=status,
            progress=progress,
            stage=stage,
            message=message,
        )

    return _callback


def _run_report_job(
    job_id: str,
    *,
    company_id: str,
    calendar_quarter: str,
    history_window: int,
    manual_transcript_upload_id: Optional[str],
    force_refresh: bool,
) -> None:
    callback = _job_progress_callback(job_id)
    try:
        _update_report_job(job_id, status="running", progress=0.03, stage="prepare", message="后台任务已启动。")
        record = create_report(
            company_id,
            calendar_quarter,
            history_window,
            manual_transcript_upload_id,
            force_refresh,
            progress_callback=callback,
        )
        _update_report_job(
            job_id,
            status="completed",
            progress=1.0,
            stage="completed",
            message="报告已生成完成，正在准备预览页面。",
            report_id=record["id"],
            error=None,
        )
    except Exception as exc:
        _update_report_job(
            job_id,
            status="failed",
            stage="failed",
            message="报告生成失败，请重试。",
            error=str(exc),
        )
    finally:
        with REPORT_JOB_FUTURES_LOCK:
            REPORT_JOB_FUTURES.pop(job_id, None)


def create_report_job(
    company_id: str,
    calendar_quarter: str,
    history_window: int = 12,
    manual_transcript_upload_id: Optional[str] = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    cache_allowed = not force_refresh and not manual_transcript_upload_id
    if cache_allowed:
        existing = _existing_report(company_id, calendar_quarter, history_window)
        if existing is not None and _report_cache_is_fresh(existing):
            job_id = uuid.uuid4().hex
            created_at = now_iso()
            with get_connection() as connection:
                connection.execute(
                    """
                    INSERT INTO report_jobs (
                      id, company_id, calendar_quarter, history_window, manual_transcript_upload_id,
                      force_refresh, status, progress, stage, message, report_id, error, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        company_id,
                        calendar_quarter,
                        history_window,
                        manual_transcript_upload_id,
                        1 if force_refresh else 0,
                "completed",
                        1.0,
                        "completed",
                        "已命中近期报告缓存，报告可直接预览。",
                        existing["id"],
                        None,
                        created_at,
                        created_at,
                    ),
                )
            return get_report_job(job_id)

    job_id = uuid.uuid4().hex
    created_at = now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO report_jobs (
              id, company_id, calendar_quarter, history_window, manual_transcript_upload_id,
              force_refresh, status, progress, stage, message, report_id, error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                company_id,
                calendar_quarter,
                history_window,
                manual_transcript_upload_id,
                1 if force_refresh else 0,
                "queued",
                0.02,
                "queued",
                "任务已创建，默认将智能复用近期缓存并按需补抓官方资料..." if not force_refresh else "任务已创建，本次将强制刷新官方源...",
                None,
                None,
                created_at,
                created_at,
            ),
        )
    initial_job = _remember_report_job_state(
        {
            "job_id": job_id,
            "company_id": company_id,
            "calendar_quarter": calendar_quarter,
            "history_window": history_window,
            "manual_transcript_upload_id": manual_transcript_upload_id,
            "force_refresh": force_refresh,
            "status": "queued",
            "progress": 0.02,
            "stage": "queued",
            "message": "任务已创建，默认将智能复用近期缓存并按需补抓官方资料..." if not force_refresh else "任务已创建，本次将强制刷新官方源...",
            "report_id": None,
            "error": None,
            "preview_url": None,
            "export_pdf_url": None,
            "created_at": created_at,
            "updated_at": created_at,
        }
    )
    _submit_report_job_from_state(initial_job)
    return get_report_job(job_id)


def company_cards() -> list[dict[str, Any]]:
    cards = []
    for company in list_companies():
        raw_supported_quarters = get_supported_quarters(str(company["id"]), 12, fetch_missing=False) or list(company["supported_quarters"])
        supported_quarters, _ready_map_applied = _filter_supported_quarters_by_full_coverage(
            str(company["id"]),
            list(raw_supported_quarters),
            12,
        )
        latest_quarter = (supported_quarters or raw_supported_quarters)[-1]
        fixture = get_quarter_fixture(company["id"], latest_quarter)
        cards.append(
            {
                "id": company["id"],
                "name": company["name"],
                "english_name": company["english_name"],
                "ticker": company["ticker"],
                "description": company["description"],
                "brand": company["brand"],
                "supported_quarters": supported_quarters,
                "full_coverage_required": _require_full_coverage_mode(),
                "structure_priority": company["structure_priority"],
                "money_symbol": company["money_symbol"],
                "currency_code": company["currency_code"],
                "headline": _compose_summary_headline(
                    company,
                    str(fixture.get("fiscal_label") or latest_quarter) if fixture else latest_quarter,
                    fixture.get("latest_kpis", {}) if fixture else {},
                    None,
                    fixture,
                ),
            }
        )
    return cards[:20]


def company_quarters(company_id: str, history_window: int = 12) -> dict[str, Any]:
    all_periods = get_company_periods(company_id, fetch_missing=True)
    supported_quarters = get_supported_quarters(company_id, history_window, fetch_missing=True)
    supported_quarters, ready_map_applied = _filter_supported_quarters_by_full_coverage(
        company_id,
        supported_quarters,
        history_window,
    )
    return {
        "company_id": company_id,
        "history_window": history_window,
        "all_quarters": all_periods,
        "supported_quarters": supported_quarters,
        "full_coverage_required": _require_full_coverage_mode(),
        "full_coverage_ready_map_applied": ready_map_applied,
    }
