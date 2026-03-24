from __future__ import annotations

import calendar as month_calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import threading
import uuid
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Optional

from ..config import CACHE_DIR, ensure_directories
from ..db import get_connection
from ..utils import json_dumps, now_iso
from .charts import (
    format_money_bn,
    format_pct,
    render_company_wordmark_svg,
    render_contribution_svg,
    render_current_quarter_svg,
    render_dual_ranked_svg,
    render_growth_overview_svg,
    render_guidance_svg,
    render_income_statement_svg,
    render_profitability_svg,
    render_segment_mix_svg,
    render_statement_translation_svg,
    render_structure_transition_svg,
)
from .institutional_views import get_institutional_views
from .local_data import (
    get_company,
    get_company_series,
    get_company_periods,
    get_quarter_fixture,
    get_segment_history,
    get_supported_quarters,
    list_companies,
)
from .official_materials import hydrate_source_materials
from .official_parsers import COMPANY_SEGMENT_PROFILES, parse_official_materials
from .official_source_resolver import resolve_official_sources
from .report_quality import evaluate_report_payload, quality_warnings_for_payload
from .uploads import get_upload


APP_DIR = Path(__file__).resolve().parents[1]
REPORT_CACHE_DEPENDENCIES = [
    APP_DIR / "static" / "app.css",
    APP_DIR / "templates" / "report_document.html",
    APP_DIR / "services" / "charts.py",
    APP_DIR / "services" / "official_fixtures.py",
    APP_DIR / "services" / "official_parsers.py",
    APP_DIR / "services" / "institutional_views.py",
    APP_DIR / "services" / "reports.py",
    APP_DIR / "services" / "seed_data.py",
]
REPORT_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=3, thread_name_prefix="report-job")
REPORT_JOB_FUTURES: dict[str, Any] = {}
REPORT_JOB_FUTURES_LOCK = threading.Lock()
ProgressCallback = Callable[[float, str, str], None]
RECENT_REPORT_CACHE_TTL_SECONDS = 6 * 60 * 60
HISTORICAL_REPORT_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60
REPORT_PAYLOAD_SCHEMA_VERSION = 12
FULL_COVERAGE_ENV = "EARNINGS_DIGEST_REQUIRE_FULL_COVERAGE"
FULL_COVERAGE_READY_MAP_PATH = APP_DIR.parent / "data" / "cache" / "full-coverage-ready-quarters.json"
HISTORICAL_OFFICIAL_QUARTER_CACHE_DIR = CACHE_DIR / "historical-official-quarter-cache"
HISTORICAL_OFFICIAL_QUARTER_CACHE_VERSION = 2
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
        "source_url": payload.get("source_url"),
        "source_date": payload.get("source_date"),
    }


def _load_historical_official_quarter_cache(company_id: str, calendar_quarter: str) -> Optional[dict[str, Any]]:
    cache_key = (str(company_id), str(calendar_quarter))
    with HISTORICAL_OFFICIAL_QUARTER_MEMORY_LOCK:
        cached = HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE.get(cache_key)
    if cached is not None:
        return _clone_cached_official_quarter_payload(cached)
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
        HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE[cache_key] = normalized
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
        HISTORICAL_OFFICIAL_QUARTER_MEMORY_CACHE[(str(company_id), str(calendar_quarter))] = normalized


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
    if guidance.get("mode") == "proxy":
        return dict(guidance)
    baseline = _build_generic_guidance(history)
    if guidance.get("mode") == "official_context":
        merged = dict(baseline)
        merged.update(guidance)
        merged["mode"] = "official_context"
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
    if net_income_bn is None and net_income_yoy_pct is None:
        return None
    if net_income_bn is not None and net_income_yoy_pct is not None:
        return f"净利润 {format_money_bn(net_income_bn, money_symbol)}，同比 {format_pct(net_income_yoy_pct, signed=True)}。"
    if net_income_bn is not None:
        return f"净利润 {format_money_bn(net_income_bn, money_symbol)}。"
    return f"净利润同比 {format_pct(net_income_yoy_pct, signed=True)}。"


def _clean_summary_fragment(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", str(text)).strip(" 。；;，,")
    return cleaned or None


def _segment_name_token(name: str) -> str:
    normalized = str(name or "").strip().casefold()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("/", " ")
    normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _normalize_segment_items(company: dict[str, Any], segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    company_id = str(company.get("id") or "")
    aliases = dict(company.get("segment_aliases") or {})
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
        for passthrough_key in ("yoy_pct", "margin_pct", "note", "logo_key", "display_name", "chip_labels", "color", "fill"):
            if existing.get(passthrough_key) is None and payload.get(passthrough_key) is not None:
                existing[passthrough_key] = payload[passthrough_key]
    if order_index:
        normalized.sort(
            key=lambda item: (
                order_index.get(str(item.get("name") or ""), len(order_index) + int(item.get("_original_index") or 0)),
                int(item.get("_original_index") or 0),
            )
        )
    if company_id == "apple":
        names = {str(item.get("name") or "") for item in normalized}
        detailed_apple_products = {"iPhone", "Mac", "iPad", "Wearables, Home and Accessories"}
        if "Products" in names and names.intersection(detailed_apple_products):
            normalized = [item for item in normalized if str(item.get("name") or "") != "Products"]
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
            "note": _note("同页并读业务结构与地区结构，让“增长来自哪里”不只停留在一句话上。", style["quarterly_lens"]),
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
        "先看收入与净利润，再看头部业务和头部地区是否共同支撑本季结果。"
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
            f"业务侧先看 {top_segment}，地区侧再看 {top_geo}；两边是否共振，比单看占比更重要。"
            if top_segment and top_geo
            else f"先抓头部结构 {top_segment or top_geo or '主线'}，再判断增长到底来自结构改善还是总量回升。"
        )
    )
    meta["income_statement"]["note"] = _short_section_text(
        (
            f"{top_segment or '头部业务'}把收入盘子撑到 {format_money_bn(revenue_bn, money_symbol)}，"
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
            else f"当前没有完整 transcript 时，先围绕 {top_qna or top_segment or '经营主线'} 读研究关注点。"
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
    return _structure_names_are_geography_like(
        company,
        [str(item.get("name") or "") for item in normalized],
    )


def _excerpt_text(text: Optional[str], limit: int) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip(" ,.;:") + "..."


def _prepare_call_quote_cards(cards: list[dict[str, Any]]) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    for index, item in enumerate(cards[:3]):
        prepared.append(
            {
                "speaker": re.sub(r"\s+", " ", str(item.get("speaker") or "Management")).strip(),
                "quote": re.sub(r"\s+", " ", str(item.get("quote") or "")).strip(),
                "analysis": re.sub(r"\s+", " ", str(item.get("analysis") or "")).strip(),
                "source_label": re.sub(r"\s+", " ", str(item.get("source_label") or "")).strip(),
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
    money_symbol = company["money_symbol"]
    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest_history.get("revenue_bn") if latest_history else None)
    net_income_bn = _value_or_history(latest_kpis.get("net_income_bn"), latest_history.get("net_income_bn") if latest_history else None)
    revenue_yoy_pct = _value_or_history(latest_kpis.get("revenue_yoy_pct"), latest_history.get("revenue_yoy_pct") if latest_history else None)
    net_income_yoy_pct = _value_or_history(latest_kpis.get("net_income_yoy_pct"), latest_history.get("net_income_yoy_pct") if latest_history else None)
    fallback_headline = _clean_summary_fragment((fixture or {}).get("headline")) or str(company.get("card_headline") or company["description"])
    if revenue_bn is None:
        return fallback_headline
    headline_parts = [f"{company['english_name']} {fiscal_label} 收入 {format_money_bn(revenue_bn, money_symbol)}"]
    if net_income_bn is not None:
        headline_parts.append(f"净利润 {format_money_bn(net_income_bn, money_symbol)}")
    headline = "，".join(headline_parts)
    growth_parts: list[str] = []
    if revenue_yoy_pct is not None:
        growth_parts.append(f"收入同比 {format_pct(revenue_yoy_pct, signed=True)}")
    if net_income_yoy_pct is not None:
        growth_parts.append(f"净利润同比 {format_pct(net_income_yoy_pct, signed=True)}")
    if growth_parts:
        headline += f"；{'，'.join(growth_parts)}"
    driver = _summary_driver_from_fixture(company, fiscal_label, fixture)
    if driver:
        headline += f"；{driver}"
    guidance = (fixture or {}).get("guidance") or {}
    guidance_revenue = guidance.get("revenue_bn")
    if guidance_revenue is not None and revenue_bn not in (None, 0):
        guidance_delta = (float(guidance_revenue) / float(revenue_bn) - 1) * 100
        guidance_label = "下一季指引中枢" if guidance.get("mode") == "official" else "下一阶段收入参考"
        headline += f"；{guidance_label} {format_money_bn(guidance_revenue, money_symbol)}，较本季 {format_pct(guidance_delta, signed=True)}"
    elif guidance.get("mode") == "official_context" and guidance.get("commentary"):
        headline += f"；{_excerpt_text(str(guidance.get('commentary') or ''), 28)}"
    return f"{headline}。"


def _build_layered_takeaways(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> list[dict[str, str]]:
    latest = history[-1]
    latest_kpis = fixture["latest_kpis"]
    guidance = fixture["guidance"]
    revenue_bn = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    revenue_yoy_pct = _value_or_history(latest_kpis.get("revenue_yoy_pct"), latest.get("revenue_yoy_pct"))
    revenue_qoq_pct = _value_or_history(latest_kpis.get("revenue_qoq_pct"), latest.get("revenue_qoq_pct"))
    net_income_bn = _value_or_history(latest_kpis.get("net_income_bn"), latest.get("net_income_bn"))
    top_segment = max(fixture.get("current_segments") or [], key=lambda item: float(item.get("value_bn") or 0.0), default=None)
    top_geo = max(fixture.get("current_geographies") or [], key=lambda item: float(item.get("value_bn") or 0.0), default=None)
    baseline = _build_generic_guidance(history)
    current_margin = _metric_or_fallback(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct")) or latest.get("net_margin_pct")
    baseline_margin = baseline.get("gaap_gross_margin_pct")
    current_margin_label = "毛利率" if latest.get("gross_margin_pct") is not None or latest_kpis.get("gaap_gross_margin_pct") is not None else company.get("historical_profit_margin_label", "净利率")

    happened = [
        f"收入 {format_money_bn(revenue_bn, money_symbol)}",
        f"净利润 {format_money_bn(net_income_bn, money_symbol)}" if net_income_bn is not None else None,
        (
            f"同比 {format_pct(revenue_yoy_pct, signed=True)} / 环比 {format_pct(revenue_qoq_pct, signed=True)}"
            if revenue_yoy_pct is not None or revenue_qoq_pct is not None
            else None
        ),
    ]
    why_body = f"{current_margin_label} {format_pct(current_margin)}"
    if baseline_margin is not None and current_margin is not None:
        why_body += f"，相对近四季中枢 {_format_delta(baseline_margin, current_margin)}"
    if top_segment and revenue_bn not in (None, 0):
        why_body += f"；{top_segment['name']} 占当季收入 {format_pct(float(top_segment.get('value_bn') or 0.0) / float(revenue_bn) * 100)}"
    elif top_geo:
        why_body += f"；地区侧以 {top_geo['name']} 为最大暴露"
    if not why_body.endswith("。"):
        why_body += "。"

    next_parts: list[str] = []
    if revenue_bn not in (None, 0) and baseline.get("revenue_bn") not in (None, 0):
        delta_to_baseline = (float(revenue_bn) / float(baseline.get("revenue_bn")) - 1) * 100
        next_parts.append(f"本季相对近四季收入中枢 {format_pct(delta_to_baseline, signed=True)}")
    if guidance.get("revenue_bn") is not None and revenue_bn not in (None, 0):
        next_delta = (float(guidance.get("revenue_bn")) / float(revenue_bn) - 1) * 100
        next_parts.append(
            f"{guidance.get('comparison_label') or ('下一季收入指引' if guidance.get('mode') == 'official' else '下一阶段收入参考')}较本季 {format_pct(next_delta, signed=True)}"
        )
    elif guidance.get("commentary"):
        next_parts.append(_excerpt_text(str(guidance.get("commentary") or ""), 44))
    next_body = "；".join(part for part in next_parts if part)
    if next_body and not next_body.endswith("。"):
        next_body += "。"
    if not next_body:
        next_body = "继续跟踪下一阶段收入基线、利润率方向与管理层语气是否同步改善。"

    return [
        {"title": "发生了什么", "body": "，".join(part for part in happened if part) + "。"},
        {"title": "为什么重要", "body": why_body},
        {"title": "接下来怎么看", "body": next_body},
    ]


def _build_expectation_panel(
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> dict[str, Any]:
    latest = history[-1]
    latest_kpis = fixture["latest_kpis"]
    guidance = fixture["guidance"]
    baseline = _build_generic_guidance(history)
    current_revenue = _value_or_history(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    current_margin = _metric_or_fallback(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct")) or latest.get("net_margin_pct")
    baseline_revenue = baseline.get("revenue_bn")
    baseline_margin = baseline.get("gaap_gross_margin_pct")
    bullets: list[str] = []
    chips: list[dict[str, str]] = []
    if current_revenue is not None and baseline_revenue not in (None, 0):
        revenue_gap = (float(current_revenue) / float(baseline_revenue) - 1) * 100
        bullets.append(f"本季收入相对近四季收入中枢 {format_pct(revenue_gap, signed=True)}，衡量本季是否超出经营常态。")
        chips.append(
            {
                "label": "常态偏离",
                "value": format_pct(revenue_gap, signed=True),
                "note": "相对近四季收入中枢",
            }
        )
    if current_margin is not None and baseline_margin is not None:
        bullets.append(f"本季利润率相对近四季中枢 {_format_delta(baseline_margin, current_margin)}，用于判断经营杠杆是否继续兑现。")
        chips.append(
            {
                "label": "利润兑现",
                "value": _format_delta(baseline_margin, current_margin),
                "note": "相对近四季利润率中枢",
            }
        )
    next_revenue = guidance.get("revenue_bn")
    if next_revenue is not None and current_revenue not in (None, 0):
        next_delta = (float(next_revenue) / float(current_revenue) - 1) * 100
        bullets.append(
            f"{guidance.get('comparison_label') or ('下一季收入指引' if guidance.get('mode') == 'official' else '下一阶段收入参考')} "
            f"{format_money_bn(next_revenue, money_symbol)}，较本季 {format_pct(next_delta, signed=True)}。"
        )
        chips.append(
            {
                "label": "展望方向",
                "value": format_pct(next_delta, signed=True),
                "note": guidance.get("comparison_label") or "下一阶段收入参考",
            }
        )
    commentary = _clean_summary_fragment(str(guidance.get("commentary") or ""))
    if commentary:
        bullets.append(_excerpt_text(commentary, 82) + ("。" if not commentary.endswith("。") else ""))
    if len(chips) < 3 and commentary:
        chips.append(
            {
                "label": "官方语境",
                "value": "管理层表述",
                "note": _excerpt_text(commentary, 26),
            }
        )
    if not bullets:
        bullets.append("当前未接入明确市场一致预期，因此本页先用近四季经营中枢与官方语境替代预期差框架。")
    return {"title": "预期差与指引拆解", "bullets": bullets[:4], "chips": chips[:3]}


def _build_institutional_digest(institutional_views: list[dict[str, Any]]) -> dict[str, Any]:
    if not institutional_views:
        return {
            "title": "街头共识速览",
            "bullets": [
                "当前季度尚未稳定抓到足够可追溯的 sell-side 条目，因此本页保留机构观点卡片的说明框架。",
                "系统只展示带机构名、发布时间和原始链接的公开转述，不会伪造一致预期。",
            ],
        }

    stance_counts = Counter(str(item.get("stance_label") or "参考") for item in institutional_views)
    positive_count = stance_counts.get("偏积极", 0)
    cautious_count = stance_counts.get("偏谨慎", 0)
    neutral_count = stance_counts.get("中性", 0)
    reference_count = stance_counts.get("参考", 0)

    theme_map = {
        "AI / 云兑现": ("ai", "cloud", "azure", "aws", "copilot", "hpc", "inference"),
        "指引与经营边界": ("guidance", "outlook", "forecast", "expectation", "boundary"),
        "利润率与现金流": ("margin", "profit", "earnings", "cash flow", "fcf"),
        "估值与目标价": ("price target", "target", "valuation", "multiple"),
        "资本开支 / 供给": ("capex", "capacity", "supply", "shipment", "yield"),
    }
    theme_counts: Counter[str] = Counter()
    for item in institutional_views:
        combined = " ".join(
            str(item.get(key) or "")
            for key in ("headline", "summary", "description")
        ).lower()
        for label, keywords in theme_map.items():
            if any(keyword in combined for keyword in keywords):
                theme_counts[label] += 1
    common_themes = [label for label, _count in theme_counts.most_common(2)]

    bullets = [
        "已收集 "
        f"{len(institutional_views)} 家机构转述观点，其中 {positive_count} 家偏积极、"
        f"{cautious_count} 家偏谨慎、{neutral_count} 家中性，另有 {reference_count} 家以事件跟踪为主。"
    ]
    if common_themes:
        bullets.append("机构反复讨论的焦点集中在 " + "、".join(common_themes) + "。")
    if positive_count >= max(2, len(institutional_views) - 1):
        bullets.append("整体卖方口径偏正面，说明这季财报更多是在强化原有多头逻辑，而不是只靠估值弹性。")
    elif cautious_count >= 2:
        bullets.append("卖方反馈偏谨慎，意味着财报兑现之后，市场仍在重新校准后续增速与盈利可持续性。")
    else:
        bullets.append("机构态度仍有分化，更适合把卖方条目当作问题清单，而不是直接当成结论。")
    return {"title": "街头共识速览", "bullets": bullets[:3]}


def _normalize_takeaways(
    takeaways: list[str],
    latest_kpis: dict[str, Any],
    latest_history: dict[str, Any],
    money_symbol: str,
) -> list[str]:
    normalized: list[str] = []
    if takeaways:
        normalized.append(takeaways[0])
    profit_line = _build_profit_growth_takeaway(latest_kpis, latest_history, money_symbol)
    if profit_line:
        normalized.append(profit_line)
    for item in takeaways[1:]:
        if profit_line and "净利润" in item:
            continue
        normalized.append(item)
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
    else:
        selected_periods = []
        sparse_window = True

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
        fiscal_label = period
        segments_payload: Optional[list[dict[str, Any]]] = None
        source_type = "structured_financial_series"
        source_url = ""
        if period in segment_history:
            history_entry = segment_history[period]
            segments_payload = history_entry["segments"]
            source_type = history_entry["source_type"]
            source_url = history_entry["source_url"]

        if period == calendar_quarter and fixture:
            fiscal_label = fixture["fiscal_label"]
            period_end = fixture["period_end"]
            release_date = fixture["release_date"]
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
    canonical_segments = _normalize_segment_items(company, segments)
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
        normalized.append(payload)
    return normalized


def _normalize_historical_geographies(
    geographies: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    total = sum(float(item.get("value_bn") or 0.0) for item in geographies if item.get("value_bn") is not None)
    if total <= 0:
        return normalized
    denominator = revenue_bn if revenue_bn not in (None, 0) and 0.55 <= total / float(revenue_bn) <= 1.35 else total
    for item in geographies:
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
    frequency: Counter[str] = Counter()
    for entry in history:
        normalized = _normalize_historical_segments(company, list(entry.get("segments") or []), entry.get("revenue_bn"))
        names = [str(item.get("name") or "") for item in normalized if str(item.get("name") or "")]
        if not names:
            continue
        lowered = {name.casefold() for name in names}
        if lowered and lowered.issubset(REGIONAL_STRUCTURE_NAMES):
            continue
        observed_lists.append(names)
        frequency.update(dict.fromkeys(names, 1))

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
    stable_threshold = max(2, round(len(observed_lists) * 0.4)) if observed_lists else 0
    core_names = [
        name
        for name in reference_names
        if frequency.get(name, 0) >= stable_threshold
    ][:2]
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
    if _structure_names_are_geography_like(
        company,
        [str(item.get("name") or "") for item in normalized],
    ):
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
    if _structure_names_are_geography_like(
        company,
        [str(item.get("name") or "") for item in normalized],
    ):
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
        candidate_map: dict[str, float] = {}
        if prev_index is not None and next_index is not None:
            span = max(1, next_index - prev_index)
            weight = (index - prev_index) / span
            prev_map = anchor_maps[prev_index]
            next_map = anchor_maps[next_index]
            for name in expected_names:
                candidate_map[name] = prev_map.get(name, 0.0) * (1 - weight) + next_map.get(name, 0.0) * weight
        elif prev_index is not None:
            prev_map = anchor_maps[prev_index]
            candidate_map = {name: prev_map.get(name, 0.0) for name in expected_names}
        elif next_index is not None:
            next_map = anchor_maps[next_index]
            candidate_map = {name: next_map.get(name, 0.0) for name in expected_names}

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
        candidate_map: dict[str, float] = {}
        if prev_index is not None and next_index is not None:
            span = max(1, next_index - prev_index)
            weight = (index - prev_index) / span
            prev_map = anchor_maps[prev_index]
            next_map = anchor_maps[next_index]
            for name in expected_names:
                candidate_map[name] = prev_map.get(name, 0.0) * (1 - weight) + next_map.get(name, 0.0) * weight
        elif prev_index is not None:
            prev_map = anchor_maps[prev_index]
            candidate_map = {name: prev_map.get(name, 0.0) for name in expected_names}
        elif next_index is not None:
            next_map = anchor_maps[next_index]
            candidate_map = {name: next_map.get(name, 0.0) for name in expected_names}

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


def _recompute_history_derivatives(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    recomputed = [dict(entry) for entry in history]
    for index, entry in enumerate(recomputed):
        previous = recomputed[index - 1] if index > 0 else None
        year_ago = recomputed[index - 4] if index > 3 else None
        if previous and entry.get("revenue_bn") not in (None, 0) and previous.get("revenue_bn") not in (None, 0):
            entry["revenue_qoq_pct"] = (float(entry["revenue_bn"]) / float(previous["revenue_bn"]) - 1) * 100
        else:
            entry["revenue_qoq_pct"] = None
        if year_ago and entry.get("net_income_bn") is not None and year_ago.get("net_income_bn") not in (None, 0):
            entry["net_income_yoy_pct"] = (float(entry["net_income_bn"]) / float(year_ago["net_income_bn"]) - 1) * 100
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
            or (needs_metric_enrichment and _entry_needs_official_metric_enrichment(entry))
        )
    ]
    total_entries = max(len(target_indexes), 1)
    if not target_indexes:
        return enriched
    reference_profile = _historical_segment_reference_profile(company, enriched)

    def _enrich_single(index: int) -> tuple[int, dict[str, Any]]:
        entry = dict(enriched[index])
        period = str(entry["quarter_label"])
        cached_parsed = _load_historical_official_quarter_cache(str(company["id"]), period)
        if cached_parsed is None:
            fixture = get_quarter_fixture(str(company["id"]), period)
            base_sources = list(fixture.get("sources") or []) if fixture else []
            period_end = _resolved_period_end(
                period,
                entry_period_end=str(entry.get("period_end") or ""),
                fixture_period_end=str(fixture.get("period_end") or "") if fixture else None,
            )
            sources = resolve_official_sources(
                company,
                period,
                period_end,
                base_sources,
                refresh=False,
                prefer_sec_only=_history_prefers_sec_only_sources(company),
            )
            if not sources:
                return (index, entry)
            source_materials = hydrate_source_materials(
                str(company["id"]),
                period,
                sources,
                refresh=False,
            )
            parsed = parse_official_materials(
                company,
                _quarter_fallback_for_structure(entry, sources),
                source_materials,
            )
            cached_parsed = {
                "latest_kpis": dict(parsed.get("latest_kpis") or {}),
                "current_segments": [dict(item) for item in list(parsed.get("current_segments") or []) if isinstance(item, dict)],
                "current_geographies": [dict(item) for item in list(parsed.get("current_geographies") or []) if isinstance(item, dict)],
                "source_url": str(sources[0].get("url") or "") if sources else "",
                "source_date": str(sources[0].get("date") or "") if sources else "",
            }
            _store_historical_official_quarter_cache(str(company["id"]), period, cached_parsed)
        latest_kpis = dict(cached_parsed.get("latest_kpis") or {})
        revenue_bn = _historical_metric_candidate(entry.get("revenue_bn"), latest_kpis.get("revenue_bn"))
        net_income_bn = _historical_metric_candidate(entry.get("net_income_bn"), latest_kpis.get("net_income_bn"), min_ratio=0.25, max_ratio=2.8)
        gross_margin_pct = latest_kpis.get("gaap_gross_margin_pct")
        revenue_yoy_pct = latest_kpis.get("revenue_yoy_pct")
        net_income_yoy_pct = latest_kpis.get("net_income_yoy_pct")
        ending_equity_bn = latest_kpis.get("ending_equity_bn")
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
            latest_kpis.get("revenue_bn") is not None
            or latest_kpis.get("net_income_bn") is not None
            or latest_kpis.get("ending_equity_bn") is not None
            or parsed_segments
            or parsed_geographies
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
                "body": "由于缺少连续 12 季业务分部披露，结构迁移与增量贡献页采用总量成长与结构限制说明。",
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
                "body": _excerpt_text(str(guidance.get("commentary") or ""), 118),
                "evidence": f"下一季收入指引 {format_money_bn(guidance.get('revenue_bn'), money_symbol)}。",
            }
        )
    elif guidance.get("mode") == "official_context":
        insights.append(
            {
                "title": "管理层展望口径",
                "body": _excerpt_text(str(guidance.get("commentary") or ""), 118),
                "evidence": "公司未给出数值 top-line 指引，因此本页以官方表述叠加经营基线阅读。",
            }
        )
    else:
        insights.append(
            {
                "title": "经营基线对照",
                "body": _excerpt_text(str(guidance.get("commentary") or ""), 118),
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
    paragraphs = [chunk.strip() for chunk in text.splitlines() if chunk.strip()]
    highlights = paragraphs[:3]
    topics = _keyword_topics(text)
    return {
        "upload_id": upload_id,
        "source_type": "manual_transcript",
        "filename": upload["filename"],
        "highlights": highlights,
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


def _looks_like_transcript_text(text: str, label: str = "") -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    lowered = normalized.lower()
    label_lower = str(label or "").lower()
    transcript_tokens = (
        "question-and-answer",
        "question and answer",
        "prepared remarks",
        "conference call",
        "earnings call",
        "operator",
        "analyst",
    )
    if any(token in label_lower for token in ("transcript", "prepared remarks")):
        return True
    signal_count = sum(1 for token in transcript_tokens if token in lowered)
    speaker_count = len(
        re.findall(
            r"\b(?:Operator|Analyst|Question(?:er)?|CEO|CFO|COO|President|Chief Executive Officer|Chief Financial Officer)\b",
            normalized,
            flags=re.IGNORECASE,
        )
    )
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
        if len(chunk) < 80:
            continue
        if any(token in lowered for token in business_tokens):
            preferred.append(chunk)
        else:
            fallback.append(chunk)
    selected = preferred[:3] if preferred else fallback[:3]
    return [_excerpt_text(item, 260) for item in selected[:3]]


def _automatic_transcript_summary(source_materials: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    candidates: list[tuple[int, int, dict[str, Any], str]] = []
    for item in source_materials:
        if item.get("status") not in {"fetched", "cached"}:
            continue
        if item.get("kind") != "call_summary" and item.get("role") != "earnings_call":
            continue
        text_path = str(item.get("text_path") or "").strip()
        if not text_path or not Path(text_path).exists():
            continue
        text = Path(text_path).read_text(encoding="utf-8", errors="ignore").strip()
        if len(text) < 400:
            continue
        label = str(item.get("label") or "")
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
    call_material_score = 100 if transcript_summary else (42 if "call_summary" in materials else 18)
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


def _source_material_warnings(source_materials: list[dict[str, Any]]) -> list[str]:
    if not source_materials:
        return []
    extracted = [item for item in source_materials if item.get("status") in {"fetched", "cached"} and item.get("text_length", 0) > 0]
    fetched = [item for item in source_materials if item.get("status") == "fetched"]
    cached = [item for item in source_materials if item.get("status") == "cached"]
    failed = [item for item in source_materials if item.get("status") == "error"]
    disabled = [item for item in source_materials if item.get("status") == "disabled"]
    warnings: list[str] = []
    if extracted:
        if fetched and cached:
            warnings.append(f"本次新抓取 {len(fetched)} 份、复用缓存 {len(cached)} 份官方材料，并已完成统一文本提取。")
        elif fetched:
            warnings.append(f"本次已自动抓取 {len(fetched)} 份官方材料并完成文本提取，可直接生成深度版报告。")
        else:
            warnings.append(f"本次已复用 {len(cached)} 份官方材料缓存，可继续沿缓存原文做自动化解析。")
    if failed:
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
    has_call_material = transcript_source_type == "official_call_material"
    has_official_material = bool(
        any(
            item.get("kind") in {"official_release", "presentation", "sec_filing"}
            for item in extracted
        )
        or any(token in materials for token in {"official_release", "presentation", "sec_filing", "earnings_release", "earnings_presentation", "earnings_commentary"})
    )
    quote_cards = list(fixture.get("call_quote_cards") or [])
    synthesized_quotes = _quote_cards_are_synthesized(quote_cards)

    if transcript_source_type == "manual_transcript":
        qna = _narrative_entry("manual_transcript")
    elif has_call_material:
        qna = _narrative_entry("official_call_material")
    elif has_official_material:
        qna = _narrative_entry("official_material_inferred")
    else:
        qna = _narrative_entry("structured_fallback")

    narrative_status = "official_call_material" if has_call_material else "official_material_inferred" if has_official_material else "structured_fallback"
    management = _narrative_entry(narrative_status)
    risks = _narrative_entry(narrative_status)
    catalysts = _narrative_entry(narrative_status)

    if quote_cards and not synthesized_quotes:
        quotes = _narrative_entry("official_quote_excerpt")
    elif quote_cards:
        quotes = _narrative_entry("synthesized_quote")
    else:
        quotes = _narrative_entry("no_quote_excerpt")

    qna_chart_title = "真实问答主题" if not qna["is_inferred"] else "推断问答主题"
    qna_chart_subtitle = (
        "按 transcript / 电话会原文整理"
        if qna["status"] in {"manual_transcript", "official_call_material"}
        else "当前缺少完整电话会问答，按官方材料动态提炼"
    )
    management_chart_subtitle = (
        "优先依据电话会与管理层原文提炼"
        if management["status"] == "official_call_material"
        else "按官方材料与经营数据动态提炼"
        if management["status"] == "official_material_inferred"
        else "当前仍带有通用研究 fallback"
    )

    return {
        "qna": qna,
        "management": management,
        "risks": risks,
        "catalysts": catalysts,
        "quotes": quotes,
        "call_panel_meta_lines": [
            f"问答主题来源：{qna['label']}。{qna['detail']}",
            f"管理层锚点来源：{quotes['label']}。{quotes['detail']}",
        ],
        "risk_meta_lines": [
            f"风险视角来源：{risks['label']}。{risks['detail']}",
            f"催化剂视角来源：{catalysts['label']}。{catalysts['detail']}",
        ],
        "quote_panel_title": "官方管理层锚点" if quotes["status"] == "official_quote_excerpt" else "管理层语境锚点",
        "qna_chart_title": qna_chart_title,
        "qna_chart_subtitle": qna_chart_subtitle,
        "management_chart_subtitle": management_chart_subtitle,
    }


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
    if latest_kpis.get("ending_equity_bn") is not None and float(latest_kpis["ending_equity_bn"]) > 0:
        latest["equity_bn"] = float(latest_kpis["ending_equity_bn"])

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
    if year_ago and latest.get("net_income_bn") is not None and year_ago.get("net_income_bn") not in (None, 0):
        latest["net_income_yoy_pct"] = (float(latest["net_income_bn"]) / float(year_ago["net_income_bn"]) - 1) * 100

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
    history_net_income = latest_history.get("net_income_bn")
    history_gross_margin = latest_history.get("gross_margin_pct")
    history_revenue_yoy = latest_history.get("revenue_yoy_pct")
    history_net_income_yoy = latest_history.get("net_income_yoy_pct")

    latest_kpis["revenue_bn"] = _fallback_to_history_if_outlier(
        latest_kpis.get("revenue_bn"),
        history_revenue,
        min_ratio=0.6,
        max_ratio=1.6,
        min_abs_delta=max(0.75, float(history_revenue or 0.0) * 0.18),
    )
    latest_kpis["net_income_bn"] = _fallback_to_history_if_outlier(
        latest_kpis.get("net_income_bn"),
        history_net_income,
        min_ratio=0.35,
        max_ratio=2.2,
        min_abs_delta=max(0.35, float(abs(history_net_income or 0.0)) * 0.22),
    )
    revenue_yoy_pct = latest_kpis.get("revenue_yoy_pct")
    if revenue_yoy_pct is not None and history_revenue_yoy is not None and abs(float(revenue_yoy_pct) - float(history_revenue_yoy)) > 35:
        latest_kpis["revenue_yoy_pct"] = history_revenue_yoy
    net_income_yoy_pct = latest_kpis.get("net_income_yoy_pct")
    if net_income_yoy_pct is not None and history_net_income_yoy is not None and abs(float(net_income_yoy_pct) - float(history_net_income_yoy)) > 55:
        latest_kpis["net_income_yoy_pct"] = history_net_income_yoy

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

    trusted_revenue = _value_or_history(latest_kpis.get("revenue_bn"), history_revenue)
    current_segments = list(sanitized.get("current_segments") or [])
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

    sanitized["latest_kpis"] = latest_kpis
    return sanitized


def _quarterize_fixture_geographies(fixture: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(fixture)
    geographies = list(normalized.get("current_geographies") or [])
    revenue_bn = (dict(normalized.get("latest_kpis") or {})).get("revenue_bn")
    if not geographies or revenue_bn in (None, 0):
        return normalized
    annual_items = [
        item
        for item in geographies
        if str(item.get("scope") or "").casefold() == "annual_filing" and float(item.get("value_bn") or 0.0) > 0
    ]
    if len(annual_items) < 2:
        return normalized
    annual_total = sum(float(item.get("value_bn") or 0.0) for item in annual_items)
    if annual_total <= 0:
        return normalized

    quarter_revenue = float(revenue_bn)
    mapped: list[dict[str, Any]] = []
    for item in geographies:
        entry = dict(item)
        if str(item.get("scope") or "").casefold() == "annual_filing":
            annual_value = max(float(item.get("value_bn") or 0.0), 0.0)
            share = annual_value / annual_total
            mapped_value = round(quarter_revenue * share, 3)
            if mapped_value <= 0 and annual_value > 0 and quarter_revenue > 0:
                mapped_value = 0.001
            entry["value_bn"] = mapped_value
            entry["share_pct"] = share * 100
            entry["scope"] = "quarterly_mapped_from_official_geography"
        mapped.append(entry)
    normalized["current_geographies"] = mapped
    coverage_notes = list(normalized.get("coverage_notes") or [])
    if not any("季度化映射" in str(note) for note in coverage_notes):
        coverage_notes.append("地区结构已按官方地理披露占比完成季度化映射，确保与当季收入口径一致。")
    normalized["coverage_notes"] = coverage_notes
    return normalized


def _promote_geographies_as_segments(
    fixture: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(fixture)
    segments = list(normalized.get("current_segments") or [])
    geographies = list(normalized.get("current_geographies") or [])
    if segments or len(geographies) < 2:
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
        if not segments and len(geographies) >= 2:
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
    baseline_revenue = _history_average(history, "revenue_bn", 4) or latest["revenue_bn"]
    use_gross_margin = latest.get("gross_margin_pct") is not None or _history_average(history, "gross_margin_pct", 4) is not None
    margin_key = "gross_margin_pct" if use_gross_margin else "net_margin_pct"
    margin_label = "毛利率" if use_gross_margin else "净利率"
    latest_margin = latest.get(margin_key) or 0.0
    baseline_margin = _history_average(history, margin_key, 4) or latest_margin
    return {
        "mode": "proxy",
        "revenue_bn": baseline_revenue,
        "comparison_revenue_bn": latest["revenue_bn"],
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


def _generic_sources(company: dict[str, Any], period_end: str) -> list[dict[str, Any]]:
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
        "当前报告会优先使用自动发现的官方源；若部分字段仍未披露，再回退到结构化季度财务序列与统一研究模板。",
        "当前未接入完整电话会 transcript，因此电话会页展示研究关注主题与证据卡片，而不是逐段原文摘录。",
        "由于缺少连续 12 季分部披露，结构迁移与增量贡献页将自动降级为总量成长、盈利质量与结构限制说明。",
    ]
    if company.get("currency_code") != "USD":
        coverage_notes.append(f"金额按 {company['currency_code']} 报告币种展示，未做外汇折算。")
    return {
        "fiscal_label": calendar_quarter,
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
        "sources": _generic_sources(company, period_end),
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
    total = latest.get("revenue_bn") or 1
    cards: list[dict[str, str]] = []
    if current_segments:
        ranked_segments = sorted(current_segments, key=lambda item: float(item["value_bn"]), reverse=True)
        top_segment = ranked_segments[0]
        cards.append(
            {
                "title": f"头部业务 | {top_segment['name']}",
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
                    "title": f"高增业务 | {fastest_segment['name']}",
                    "value": format_pct(fastest_segment.get("yoy_pct"), signed=True),
                    "note": f"当季收入 {format_money_bn(float(fastest_segment['value_bn']), money_symbol)}",
                }
            )
    if current_geographies:
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
            title = f"重点业务 | {segment['name']}"
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

    commentary = str(guidance.get("commentary") or "").strip()
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
    qna_status = str(((narrative_provenance or {}).get("qna") or {}).get("status") or "")
    if transcript_summary:
        return {
            "title": "手动 transcript 摘要" if transcript_summary.get("source_type") == "manual_transcript" else "自动电话会摘要",
            "meta_lines": narrative_provenance.get("call_panel_meta_lines") or [],
            "bullets": [re.sub(r"\s+", " ", str(item or "")).strip() for item in transcript_summary["highlights"][:2]],
        }
    if qna_status in {"manual_transcript", "official_call_material"}:
        return {
            "title": "当前电话会摘要",
            "meta_lines": narrative_provenance.get("call_panel_meta_lines") or [],
            "bullets": [re.sub(r"\s+", " ", str(item.get("note") or "")).strip() for item in qna_topics[:2]],
        }
    return {
        "title": "当前无完整电话会实录，展示推断问答主题",
        "meta_lines": narrative_provenance.get("call_panel_meta_lines") or [],
        "bullets": [re.sub(r"\s+", " ", str(item.get("note") or "")).strip() for item in qna_topics[:2]],
    }


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


def _build_scoreboard(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    structure_dimension: str,
    money_symbol: str,
) -> list[dict[str, str]]:
    latest = fixture["latest_kpis"]
    top_segment = max(fixture["current_segments"], key=lambda item: float(item["value_bn"])) if fixture.get("current_segments") else None
    if latest.get("free_cash_flow_bn") is None:
        profitability_card = {
            "title": "净利润",
            "value": format_money_bn(latest.get("net_income_bn"), money_symbol),
            "subvalue": f"净利率 {format_pct(history[-1].get('net_margin_pct'))}",
        }
    else:
        profitability_card = {
            "title": "自由现金流",
            "value": format_money_bn(latest["free_cash_flow_bn"], money_symbol),
            "subvalue": f"FCF margin {format_pct(latest['free_cash_flow_bn'] / latest['revenue_bn'] * 100)}",
        }
    structure_card = {
        "title": "头部分部",
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
    periods, series = get_company_series(company_id)
    _emit_progress(progress_callback, 0.06, "prepare", "公司基础数据已加载，正在读取历史时间轴...")
    _emit_progress(progress_callback, 0.10, "history", "正在构建近 12 季历史数据与增长结构...")
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
    _emit_progress(progress_callback, 0.79, "normalize", "正在合并官方解析结果并校验异常值...")
    fixture = _merge_fixture_payload(fixture, parsed_fixture)
    fixture["current_segments"] = _normalize_segment_items(company, list(fixture.get("current_segments") or []))
    fixture = _sanitize_fixture_payload(company, fixture, history[-1])
    fixture = _quarterize_fixture_geographies(fixture)
    fixture = _promote_geographies_as_segments(fixture)
    history = _refresh_latest_history_entry(company, history, fixture)
    _emit_progress(progress_callback, 0.81, "history", "正在用官方结构补齐近 12 季口径...")
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
    fixture = _promote_geographies_as_segments(fixture)
    fixture = _quarterize_fixture_geographies(fixture)
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
    qna_topics = _ensure_minimum_qna_topics(
        qna_topics,
        list(fixture.get("management_themes") or []),
        list(fixture.get("risks") or []),
        list(fixture.get("catalysts") or []),
    )
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
    if narrative_provenance["qna"]["status"] == "official_call_material":
        coverage_warnings.insert(0, "当前问答主题优先依据官方电话会材料整理，并非静态模板。")
    elif narrative_provenance["qna"]["status"] == "official_material_inferred":
        coverage_warnings.insert(0, "当前未获取到当季完整电话会 transcript / Q&A，问答主题基于官方财报材料动态推断。")
    elif narrative_provenance["qna"]["status"] == "structured_fallback":
        coverage_warnings.insert(0, "当前缺少可解析的官方电话会与财报原文材料，问答主题仍带有统一研究 fallback 性质。")
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
    coverage_warnings = _reconcile_coverage_warnings(
        coverage_warnings,
        fixture=fixture,
        source_materials=source_materials,
        structure_dimension=structure_dimension,
        institutional_views=institutional_views,
    )

    brand = company["brand"]
    money_symbol = company.get("money_symbol", "$")
    metric_rows = _build_metric_rows(company, history, money_symbol)
    current_detail_cards = _build_current_detail_cards(company, fixture, history, structure_dimension, money_symbol)
    cash_panel = _build_cash_quality_panel(company, fixture, history[-1], money_symbol)
    guidance_panel = _build_guidance_panel(fixture, history[-1], money_symbol)
    expectation_panel = _build_expectation_panel(fixture, history, money_symbol)
    call_panel = _build_call_panel(fixture, transcript_summary, qna_topics, narrative_provenance)
    history_summary_cards = _build_history_summary_cards(history, money_symbol)
    takeaways = _normalize_takeaways(fixture["takeaways"], fixture["latest_kpis"], history[-1], money_symbol)
    layered_takeaways = _build_layered_takeaways(company, fixture, history, money_symbol)
    institutional_digest = _build_institutional_digest(institutional_views)
    income_statement = _build_income_statement_snapshot(company, fixture, history)
    uses_official_context = _guidance_uses_official_context(fixture["guidance"])
    if fixture["guidance"]["mode"] == "official":
        guidance_title = "业绩指引页 · 下一季指引"
        guidance_note = "将本季已兑现结果与公司正式给出的下一季数值指引放到一页内对照阅读。"
        guidance_chart_title = "当前业绩与下一季指引"
    elif fixture["guidance"]["mode"] == "official_context":
        guidance_title = "业绩指引页 · 官方展望语境"
        guidance_note = "公司未给出明确数值指引时，本页保留官方展望语境，并用经营基线补足对照面。"
        guidance_chart_title = "当前季度与官方展望语境"
    else:
        guidance_title = "业绩指引页 · 经营基线"
        guidance_note = "当官方下一季指引尚未接入时，使用最近四季经营基线来模拟下一阶段经营参照。"
        guidance_chart_title = "当前业绩与经营基线对照"
    guidance_note = f"{guidance_note} 重点看指引与本季兑现是否同向。"
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
            narrative_provenance["qna_chart_title"] if uses_official_context else "研究问题与追踪点",
            qna_topics,
            brand["secondary"],
            left_subtitle=narrative_provenance["management_chart_subtitle"] if uses_official_context else "按结构化财务数据自动提炼",
            right_subtitle=narrative_provenance["qna_chart_subtitle"] if uses_official_context else "按研究关注度与潜在拐点排序",
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
        "growth_overview": render_growth_overview_svg(history, brand["segment_colors"], brand["primary"], money_symbol=money_symbol),
        "structure_transition": render_structure_transition_svg(
            history,
            brand["segment_colors"],
            brand["primary"],
            "由于缺少连续 12 季分部披露，本页改为结构限制说明与管理层结构判断。",
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
            "当前公司缺少连续季度分部明细，因此增量拆解降级为总量与利润质量分析。",
            money_symbol=money_symbol,
        ),
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
        "guidance_panel": guidance_panel,
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
        "management_themes": fixture["management_themes"],
        "qna_themes": qna_topics,
        "risks": fixture["risks"],
        "catalysts": fixture["catalysts"],
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
        "page_count": 16,
        "generated_at": now_iso(),
    }
    quality_report = evaluate_report_payload(
        payload,
        history_window=history_window,
        require_full_coverage=full_coverage_mode,
    )
    payload["quality_report"] = quality_report
    quality_warnings = quality_warnings_for_payload(quality_report)
    if quality_warnings:
        payload["coverage_warnings"] = _reconcile_coverage_warnings(
            list(payload["coverage_warnings"]) + quality_warnings,
            fixture=fixture,
            source_materials=source_materials,
            structure_dimension=structure_dimension,
            institutional_views=institutional_views,
        )
    return payload


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
    quality = dict(payload.get("quality_report") or {})
    if payload.get("full_coverage_required") and str(quality.get("status") or "fail") != "pass":
        issue_messages = [
            str(item.get("message") or "").strip()
            for item in list(quality.get("issues") or [])
            if str(item.get("message") or "").strip()
        ]
        top_reasons = "；".join(issue_messages[:3]) if issue_messages else "质量门禁未通过。"
        raise ValueError(f"Full coverage quality gate failed: {top_reasons}")

    report_id = uuid.uuid4().hex
    created_at = now_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO reports (
              id, company_id, calendar_quarter, history_window, structure_dimension_used,
              coverage_warnings_json, payload_json, pdf_path, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    if company:
        if "report_style" not in normalized:
            normalized["report_style"] = _build_report_style(company)
        if "section_meta" not in normalized:
            section_meta = _build_section_meta(company)
            guidance_note = str(normalized.get("guidance_note") or section_meta["guidance"]["note"])
            if guidance_note:
                section_meta["guidance"]["note"] = guidance_note
            normalized["section_meta"] = section_meta
        visuals = dict(normalized.get("visuals") or {})
        if "company_brand" not in visuals:
            visuals["company_brand"] = render_company_wordmark_svg(
                str(company.get("id") or ""),
                str(company.get("english_name") or company.get("name") or ""),
                str(((company.get("brand") or {}).get("primary")) or "#0F172A"),
            )
            normalized["visuals"] = visuals
    if not isinstance(normalized.get("quality_report"), dict):
        normalized["quality_report"] = evaluate_report_payload(normalized)
    return normalized


def update_report_pdf(report_id: str, pdf_path: str) -> None:
    with get_connection() as connection:
        connection.execute(
            "UPDATE reports SET pdf_path = ?, updated_at = ? WHERE id = ?",
            (pdf_path, now_iso(), report_id),
        )


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


def get_report_job(job_id: str) -> dict[str, Any]:
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM report_jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise KeyError(f"Report job not found: {job_id}")
    return _serialize_report_job(dict(row))


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
    future = REPORT_JOB_EXECUTOR.submit(
        _run_report_job,
        job_id,
        company_id=company_id,
        calendar_quarter=calendar_quarter,
        history_window=history_window,
        manual_transcript_upload_id=manual_transcript_upload_id,
        force_refresh=force_refresh,
    )
    with REPORT_JOB_FUTURES_LOCK:
        REPORT_JOB_FUTURES[job_id] = future
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
    return cards


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
