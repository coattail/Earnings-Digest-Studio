from __future__ import annotations

import csv
import json
import re
from functools import lru_cache
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from ..config import CACHE_DIR, NVIDIA_SEGMENT_HISTORY_PATH, TECH_ANALYSIS_DATA_PATH, ensure_directories
from .seed_data import COMPANY_REGISTRY, QUARTER_FIXTURES


REQUEST_HEADERS = {
    "user-agent": "EarningsDigestStudio/0.1 (+local-user)",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
}

SEC_COMPANYFACTS_DIR = CACHE_DIR / "sec-companyfacts"
RECENT_COMPANYFACTS_TTL_SECONDS = 12 * 60 * 60
HISTORICAL_COMPANYFACTS_TTL_SECONDS = 30 * 24 * 60 * 60


@lru_cache(maxsize=1)
def load_financial_source_data() -> dict[str, Any]:
    raw_text = Path(TECH_ANALYSIS_DATA_PATH).read_text(encoding="utf-8")
    prefix = "window.FINANCIAL_SOURCE_DATA = "
    payload = raw_text[raw_text.index(prefix) + len(prefix) :].strip().rstrip(";")
    return json.loads(payload)


def list_companies() -> list[dict[str, Any]]:
    return sorted((dict(value) for value in COMPANY_REGISTRY.values()), key=lambda item: item["display_order"])


def get_company(company_id: str) -> dict[str, Any]:
    try:
        return dict(COMPANY_REGISTRY[company_id])
    except KeyError as exc:
        raise KeyError(f"Unknown company id: {company_id}") from exc


def get_quarter_fixture(company_id: str, calendar_quarter: str) -> Optional[dict[str, Any]]:
    fixture = QUARTER_FIXTURES.get((company_id, calendar_quarter))
    return dict(fixture) if fixture else None


def _cache_path(company_id: str) -> Path:
    ensure_directories()
    return CACHE_DIR / f"{company_id}-quarterly-series.json"


def _companyfacts_cache_path(company_id: str) -> Path:
    ensure_directories()
    SEC_COMPANYFACTS_DIR.mkdir(parents=True, exist_ok=True)
    return SEC_COMPANYFACTS_DIR / f"{company_id}-companyfacts.json"


def _parse_period(period: str) -> Tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", period)
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))


def _sort_periods(periods: List[str]) -> List[str]:
    return sorted(periods, key=_parse_period)


def _remap_period_label(period: str, mode: str) -> str:
    year, quarter = _parse_period(period)
    if not year:
        return period
    if mode == "same_year_previous_quarter":
        mapped = {1: 4, 2: 1, 3: 2, 4: 3}[quarter]
        return f"{year}Q{mapped}"
    return period


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + delta
    shifted_year = index // 12
    shifted_month = index % 12 + 1
    return (shifted_year, shifted_month)


def _calendar_quarter_from_period_end(date_key: str) -> Optional[str]:
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", date_key)
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    counts: dict[str, int] = {}
    for offset in (-2, -1, 0):
        shifted_year, shifted_month = _shift_month(year, month, offset)
        quarter = (shifted_month - 1) // 3 + 1
        label = f"{shifted_year}Q{quarter}"
        counts[label] = counts.get(label, 0) + 1
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _remap_series_labels(periods: list[str], series: dict[str, Any], mode: str) -> tuple[list[str], dict[str, Any]]:
    if mode == "by_period_end_majority":
        period_meta = series.get("periodMeta", {}) if isinstance(series.get("periodMeta"), dict) else {}
        period_map = {
            period: _calendar_quarter_from_period_end(str(period_meta.get(period, {}).get("date_key") or "")) or period
            for period in periods
        }
        remapped_series: dict[str, Any] = {}
        for key, value in series.items():
            if isinstance(value, dict):
                remapped_series[key] = {period_map.get(period, period): metric for period, metric in value.items()}
            else:
                remapped_series[key] = value
        remapped_periods = _sort_periods(list({period_map.get(period, period) for period in periods}))
        return remapped_periods, remapped_series
    remapped_series: dict[str, Any] = {}
    for key, value in series.items():
        if isinstance(value, dict):
            remapped_series[key] = {_remap_period_label(period, mode): metric for period, metric in value.items()}
        else:
            remapped_series[key] = value
    remapped_periods = _sort_periods([_remap_period_label(period, mode) for period in periods])
    return remapped_periods, remapped_series


def _to_quarter_label(date_key: str) -> Optional[str]:
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", date_key)
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        return None
    quarter = (month - 1) // 3 + 1
    return f"{year}Q{quarter}"


def _to_quarter_label_with_fiscal_quarter(date_key: str, fiscal_quarter_token: str) -> Optional[str]:
    date_match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", date_key)
    quarter_match = re.fullmatch(r"Q([1-4])", str(fiscal_quarter_token or ""))
    if not date_match or not quarter_match:
        return None
    year = int(date_match.group(1))
    month = int(date_match.group(2))
    quarter = int(quarter_match.group(1))
    if quarter == 4 and month <= 3:
        year -= 1
    return f"{year}Q{quarter}"


def _extract_financial_block(html: str) -> str:
    match = re.search(r"financialData:\{([\s\S]*?)\},map:\[", html)
    if not match:
        raise ValueError("Unable to locate financialData block.")
    return match.group(1)


def _extract_array_raw(block: str, key: str) -> str:
    match = re.search(rf"{re.escape(key)}:\[([^\]]*)\]", block)
    if not match:
        raise ValueError(f"Missing field: {key}")
    return match.group(1).strip()


def _extract_first_array_raw(block: str, keys: List[str]) -> str:
    for key in keys:
        try:
            return _extract_array_raw(block, key)
        except ValueError:
            continue
    raise ValueError(f"Missing fields: {', '.join(keys)}")


def _parse_string_array(raw: str) -> list[str]:
    if not raw:
        return []
    return json.loads(f"[{raw}]")


def _parse_number_array(raw: str) -> list[Optional[float]]:
    values: list[Optional[float]] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if token in {"null", "undefined", "void 0"}:
            values.append(None)
            continue
        value = float(token)
        values.append(value if value == value else None)
    return values


def _extract_optional_number_array(block: str, keys: List[str], length: int) -> list[Optional[float]]:
    try:
        values = _parse_number_array(_extract_first_array_raw(block, keys))
    except ValueError:
        return [None] * length
    if len(values) < length:
        values.extend([None] * (length - len(values)))
    return values[:length]


def _extract_financial_currency(html: str) -> str:
    match = re.search(r'curr:\{[^}]*financial:"([A-Z]{3})"', html)
    return match.group(1) if match else "USD"


def _extract_financial_series(html: str) -> list[dict[str, Any]]:
    block = _extract_financial_block(html)
    date_keys = _parse_string_array(_extract_array_raw(block, "datekey"))
    fiscal_quarter = _parse_string_array(_extract_array_raw(block, "fiscalQuarter"))
    revenue = _parse_number_array(_extract_array_raw(block, "revenue"))
    net_income = _parse_number_array(_extract_first_array_raw(block, ["netinc", "netIncome"]))
    gross_margin_ratio = _extract_optional_number_array(block, ["grossMargin"], len(date_keys))
    max_length = min(len(date_keys), len(fiscal_quarter), len(revenue), len(net_income), len(gross_margin_ratio))
    rows = []
    for index in range(max_length):
        period = _to_quarter_label_with_fiscal_quarter(date_keys[index], fiscal_quarter[index]) or _to_quarter_label(date_keys[index])
        if not period:
            continue
        rows.append(
            {
                "period": period,
                "date_key": date_keys[index],
                "revenue": revenue[index],
                "net_income": net_income[index],
                "gross_margin_pct": gross_margin_ratio[index] * 100 if gross_margin_ratio[index] is not None else None,
            }
        )
    rows.sort(key=lambda item: _parse_period(item["period"]))
    return rows


def _build_remote_series(company: dict[str, Any], html: str) -> dict[str, Any]:
    rows = _extract_financial_series(html)
    currency_code = _extract_financial_currency(html)
    revenue = {}
    earnings = {}
    gross_margin = {}
    period_meta = {}
    periods = []
    for row in rows:
        period = row["period"]
        periods.append(period)
        if row["revenue"] is not None:
            revenue[period] = int(row["revenue"])
        if row["net_income"] is not None:
            earnings[period] = int(row["net_income"])
        if row["gross_margin_pct"] is not None:
            gross_margin[period] = float(row["gross_margin_pct"])
        period_meta[period] = {"date_key": row["date_key"]}

    periods = _sort_periods(periods)
    revenue_growth = {}
    for index, period in enumerate(periods):
        if index < 4:
            continue
        previous_period = periods[index - 4]
        current_revenue = revenue.get(period)
        previous_revenue = revenue.get(previous_period)
        if current_revenue and previous_revenue:
            revenue_growth[period] = ((current_revenue - previous_revenue) / abs(previous_revenue)) * 100

    return {
        "periods": periods,
        "series": {
            "revenue": revenue,
            "earnings": earnings,
            "grossMargin": gross_margin,
            "revenueGrowth": revenue_growth,
            "roe": {},
            "periodMeta": period_meta,
            "currency_code": currency_code,
        },
    }


def _companyfacts_cache_is_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists():
        return False
    age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age_seconds <= ttl_seconds


def _companyfacts_ttl_seconds() -> int:
    return RECENT_COMPANYFACTS_TTL_SECONDS if date.today().year >= 2025 else HISTORICAL_COMPANYFACTS_TTL_SECONDS


def _load_companyfacts(company: dict[str, Any], refresh: bool = False) -> Optional[dict[str, Any]]:
    sec_cik = str(company.get("official_source", {}).get("sec_cik") or "")
    if not sec_cik:
        return None
    cache_path = _companyfacts_cache_path(str(company["id"]))
    if cache_path.exists() and not refresh and _companyfacts_cache_is_fresh(cache_path, _companyfacts_ttl_seconds()):
        return json.loads(cache_path.read_text(encoding="utf-8"))
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{sec_cik}.json"
    try:
        with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
    except Exception:
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        return None
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _concept_unit_items(payload: dict[str, Any], concept_name: str) -> list[dict[str, Any]]:
    concept = payload.get("facts", {}).get("us-gaap", {}).get(concept_name)
    if not isinstance(concept, dict):
        return []
    items: list[dict[str, Any]] = []
    units = concept.get("units", {})
    for unit_name, values in units.items():
        if not str(unit_name).upper().startswith("USD"):
            continue
        if isinstance(values, list):
            items.extend(value for value in values if isinstance(value, dict))
    return items


def _fact_duration_days(item: dict[str, Any]) -> Optional[int]:
    start = str(item.get("start") or "")
    end = str(item.get("end") or "")
    try:
        return (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    except ValueError:
        return None


def _fact_form_priority(form: str) -> int:
    order = {"10-Q": 0, "10-K": 1, "6-K": 2, "20-F": 3, "8-K": 4}
    return order.get(str(form or "").upper(), 9)


def _fact_quarter_label(item: dict[str, Any]) -> Optional[str]:
    end = str(item.get("end") or "")
    return _calendar_quarter_from_period_end(end)


def _instant_concept_map(payload: dict[str, Any], concept_names: list[str]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for concept_name in concept_names:
        for item in _concept_unit_items(payload, concept_name):
            period = _fact_quarter_label(item)
            value = item.get("val")
            if period is None or value is None:
                continue
            score = 0
            frame = str(item.get("frame") or "")
            if period in frame:
                score += 30
            score -= _fact_form_priority(str(item.get("form") or "")) * 3
            filed = str(item.get("filed") or "")
            candidate = dict(item)
            candidate["_score"] = score
            candidate["_concept"] = concept_name
            current = selected.get(period)
            if current is None:
                selected[period] = candidate
                continue
            current_key = (
                int(current.get("_score") or 0),
                str(current.get("filed") or ""),
                -_fact_form_priority(str(current.get("form") or "")),
            )
            candidate_key = (
                int(candidate.get("_score") or 0),
                filed,
                -_fact_form_priority(str(candidate.get("form") or "")),
            )
            if candidate_key > current_key:
                selected[period] = candidate
    return selected


def _compute_ttm_roe_series(periods: list[str], earnings: dict[str, int], equity: dict[str, int]) -> dict[str, float]:
    roe: dict[str, float] = {}
    for index, period in enumerate(periods):
        if index < 3:
            continue
        earnings_window = [earnings.get(periods[position]) for position in range(index - 3, index + 1)]
        if any(value is None for value in earnings_window):
            continue
        equity_window = [equity.get(periods[position]) for position in range(max(0, index - 4), index + 1)]
        equity_values = [float(value) for value in equity_window if value not in (None, 0)]
        if len(equity_values) < 2:
            continue
        avg_equity = sum(equity_values) / len(equity_values)
        if avg_equity <= 0:
            continue
        ttm_earnings = sum(float(value) for value in earnings_window if value is not None)
        roe_value = ttm_earnings / avg_equity * 100
        if -100 < roe_value < 250:
            roe[period] = roe_value
    return roe


def _quarterly_concept_map(payload: dict[str, Any], concept_names: list[str]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for concept_name in concept_names:
        for item in _concept_unit_items(payload, concept_name):
            period = _fact_quarter_label(item)
            duration_days = _fact_duration_days(item)
            value = item.get("val")
            if period is None or value is None or duration_days is None:
                continue
            if duration_days < 70 or duration_days > 110:
                continue
            score = 0
            frame = str(item.get("frame") or "")
            if period in frame:
                score += 30
            score += max(0, 24 - abs(duration_days - 91))
            score -= _fact_form_priority(str(item.get("form") or "")) * 3
            filed = str(item.get("filed") or "")
            candidate = dict(item)
            candidate["_score"] = score
            candidate["_concept"] = concept_name
            current = selected.get(period)
            if current is None:
                selected[period] = candidate
                continue
            current_key = (
                int(current.get("_score") or 0),
                str(current.get("filed") or ""),
                -_fact_form_priority(str(current.get("form") or "")),
            )
            candidate_key = (
                int(candidate.get("_score") or 0),
                filed,
                -_fact_form_priority(str(candidate.get("form") or "")),
            )
            if candidate_key > current_key:
                selected[period] = candidate
    return selected


def _build_companyfacts_series(payload: dict[str, Any]) -> dict[str, Any]:
    revenue_candidates = _quarterly_concept_map(
        payload,
        [
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
            "Revenues",
            "Revenue",
        ],
    )
    earnings_candidates = _quarterly_concept_map(payload, ["NetIncomeLoss", "ProfitLoss"])
    gross_profit_candidates = _quarterly_concept_map(payload, ["GrossProfit"])
    equity_candidates = _instant_concept_map(
        payload,
        [
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
            "CommonStockholdersEquity",
            "PartnersCapitalIncludingPortionAttributableToNoncontrollingInterest",
        ],
    )

    revenue: dict[str, int] = {}
    earnings: dict[str, int] = {}
    gross_margin: dict[str, float] = {}
    equity: dict[str, int] = {}
    period_meta: dict[str, dict[str, str]] = {}

    for period, item in revenue_candidates.items():
        value = item.get("val")
        if value is None:
            continue
        revenue[period] = int(float(value))
        period_meta[period] = {
            "date_key": str(item.get("end") or ""),
            "filed": str(item.get("filed") or ""),
            "form": str(item.get("form") or ""),
            "accn": str(item.get("accn") or ""),
        }
    for period, item in earnings_candidates.items():
        value = item.get("val")
        if value is None:
            continue
        earnings[period] = int(float(value))
        period_meta.setdefault(
            period,
            {
                "date_key": str(item.get("end") or ""),
                "filed": str(item.get("filed") or ""),
                "form": str(item.get("form") or ""),
                "accn": str(item.get("accn") or ""),
            },
        )
    for period, item in gross_profit_candidates.items():
        gross_profit_value = item.get("val")
        revenue_value = revenue.get(period)
        if gross_profit_value is None or revenue_value in (None, 0):
            continue
        gross_margin[period] = float(gross_profit_value) / float(revenue_value) * 100
        period_meta.setdefault(
            period,
            {
                "date_key": str(item.get("end") or ""),
                "filed": str(item.get("filed") or ""),
                "form": str(item.get("form") or ""),
                "accn": str(item.get("accn") or ""),
            },
        )
    for period, item in equity_candidates.items():
        value = item.get("val")
        if value is None:
            continue
        equity[period] = int(float(value))
        period_meta.setdefault(
            period,
            {
                "date_key": str(item.get("end") or ""),
                "filed": str(item.get("filed") or ""),
                "form": str(item.get("form") or ""),
                "accn": str(item.get("accn") or ""),
            },
        )

    periods = _sort_periods(sorted({*revenue.keys(), *earnings.keys(), *gross_margin.keys(), *equity.keys()}))
    revenue_growth: dict[str, float] = {}
    for index, period in enumerate(periods):
        if index < 4:
            continue
        previous_period = periods[index - 4]
        current_revenue = revenue.get(period)
        previous_revenue = revenue.get(previous_period)
        if current_revenue not in (None, 0) and previous_revenue not in (None, 0):
            revenue_growth[period] = ((float(current_revenue) - float(previous_revenue)) / abs(float(previous_revenue))) * 100
    roe = _compute_ttm_roe_series(periods, earnings, equity)

    return {
        "periods": periods,
        "series": {
            "revenue": revenue,
            "earnings": earnings,
            "grossMargin": gross_margin,
            "revenueGrowth": revenue_growth,
            "roe": roe,
            "equity": equity,
            "periodMeta": period_meta,
        },
    }


def _merge_official_series(
    periods: list[str],
    series: dict[str, Any],
    official_payload: Optional[dict[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    if not official_payload:
        return periods, series
    official_periods = list(official_payload.get("periods") or [])
    official_series = dict(official_payload.get("series") or {})
    if not official_periods:
        return periods, series

    merged = dict(series)
    for metric_key in ("revenue", "earnings", "grossMargin", "revenueGrowth", "roe", "equity", "periodMeta"):
        merged[metric_key] = dict(merged.get(metric_key) or {})

    for period in official_periods:
        if period not in periods:
            periods.append(period)
        official_revenue = official_series.get("revenue", {}).get(period)
        if merged["revenue"].get(period) is None and official_revenue is not None:
            merged["revenue"][period] = official_revenue
        official_earnings = official_series.get("earnings", {}).get(period)
        if merged["earnings"].get(period) is None and official_earnings is not None:
            merged["earnings"][period] = official_earnings
        official_margin = official_series.get("grossMargin", {}).get(period)
        if merged["grossMargin"].get(period) is None and official_margin is not None:
            merged["grossMargin"][period] = official_margin
        official_growth = official_series.get("revenueGrowth", {}).get(period)
        if merged["revenueGrowth"].get(period) is None and official_growth is not None:
            merged["revenueGrowth"][period] = official_growth
        official_equity = official_series.get("equity", {}).get(period)
        if official_equity is not None:
            merged["equity"][period] = official_equity
        if period not in merged["periodMeta"] and official_series.get("periodMeta", {}).get(period):
            merged["periodMeta"][period] = dict(official_series["periodMeta"][period])

    authoritative_roe = _compute_ttm_roe_series(_sort_periods(periods), merged["earnings"], merged["equity"])
    if authoritative_roe:
        merged["roe"] = authoritative_roe
    else:
        merged["roe"] = {}

    return _sort_periods(periods), merged


def _fetch_remote_company_series(company: dict[str, Any]) -> dict[str, Any]:
    url = f"https://stockanalysis.com/stocks/{company['slug']}/financials/?p=quarterly"
    with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = _build_remote_series(company, response.text)
    cache_path = _cache_path(company["id"])
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _load_cached_remote_series(company_id: str) -> Optional[dict[str, Any]]:
    cache_path = _cache_path(company_id)
    if not cache_path.exists():
        return None
    return json.loads(cache_path.read_text(encoding="utf-8"))


def get_company_series(company_id: str) -> tuple[list[str], dict[str, Any]]:
    company = get_company(company_id)
    if company["data_provider"] == "local":
        dataset = load_financial_source_data()
        periods = dataset["periods"]
        series = dict(dataset["companies"][company["series_key"]])
        series["equity"] = dict(series.get("equity") or {})
        series["currency_code"] = company.get("currency_code", "USD")
        companyfacts_payload = _load_companyfacts(company)
        if companyfacts_payload:
            official_series = _build_companyfacts_series(companyfacts_payload)
            periods, series = _merge_official_series(list(periods), series, official_series)
        series["roe"] = _compute_ttm_roe_series(_sort_periods(list(periods)), dict(series.get("earnings") or {}), dict(series.get("equity") or {}))
        available_periods = [period for period in periods if period in series["revenue"]]
        if company.get("quarter_label_mode") not in (None, "natural"):
            available_periods, series = _remap_series_labels(available_periods, series, str(company["quarter_label_mode"]))
        return available_periods, series

    cached = _load_cached_remote_series(company_id)
    payload = cached or _fetch_remote_company_series(company)
    periods = list(payload["periods"])
    series = dict(payload["series"])
    series["equity"] = dict(series.get("equity") or {})
    companyfacts_payload = _load_companyfacts(company)
    if companyfacts_payload:
        official_series = _build_companyfacts_series(companyfacts_payload)
        periods, series = _merge_official_series(periods, series, official_series)
    series["roe"] = _compute_ttm_roe_series(_sort_periods(list(periods)), dict(series.get("earnings") or {}), dict(series.get("equity") or {}))
    if company.get("quarter_label_mode") not in (None, "natural"):
        periods, series = _remap_series_labels(periods, series, str(company["quarter_label_mode"]))
    return periods, series


def get_company_periods(company_id: str, fetch_missing: bool = True) -> list[str]:
    company = get_company(company_id)
    if company["data_provider"] == "local":
        return get_company_series(company_id)[0]
    if fetch_missing:
        return get_company_series(company_id)[0]
    cached = _load_cached_remote_series(company_id)
    if cached:
        periods = _sort_periods(list(cached.get("periods") or []))
        series = dict(cached.get("series") or {})
        if company.get("quarter_label_mode") not in (None, "natural"):
            periods, _ = _remap_series_labels(periods, series, str(company["quarter_label_mode"]))
        return periods
    return list(company.get("supported_quarters") or [])


def get_supported_quarters(
    company_id: str,
    history_window: int = 12,
    fetch_missing: bool = True,
) -> list[str]:
    periods = get_company_periods(company_id, fetch_missing=fetch_missing)
    if history_window <= 1:
        return periods
    if len(periods) < history_window:
        return []
    return periods[history_window - 1 :]


@lru_cache(maxsize=4)
def load_nvidia_segment_history() -> dict[str, dict[str, Any]]:
    periods, _ = get_company_series("nvidia")
    with open(NVIDIA_SEGMENT_HISTORY_PATH, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    relevant_periods = periods[-len(rows) :]
    mapping: dict[str, dict[str, Any]] = {}
    for period, row in zip(relevant_periods, rows):
        segments = []
        for name in ["Data Center", "Gaming", "Professional Visualization", "Automotive", "OEM and Other"]:
            value_bn = float(row[name]) / 1000
            segments.append(
                {
                    "name": name,
                    "value_bn": value_bn,
                    "share_pct": float(row[f"{name}_share"]),
                }
            )
        mapping[period] = {
            "segments": segments,
            "source_type": row["source_type"],
            "source_url": row["source_url"],
        }
    return mapping


def get_segment_history(company_id: str) -> dict[str, dict[str, Any]]:
    if company_id == "nvidia":
        return load_nvidia_segment_history()
    return {}
