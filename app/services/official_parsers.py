from __future__ import annotations

import calendar
import copy
from html import unescape
from itertools import combinations
import re
from pathlib import Path
import threading
import time
from typing import Any, Callable, Optional

from .charts import format_money_bn, format_pct
from .local_data import get_company
from .official_materials import hydrate_source_materials
from .official_source_resolver import resolve_official_sources


ParserProgressCallback = Callable[[float, str], None]


def _scaled_progress_callback(
    progress_callback: Optional[ParserProgressCallback],
    *,
    start: float,
    end: float,
) -> Callable[[float, str], None]:
    span = max(0.0, end - start)

    def _callback(progress: float, message: str) -> None:
        if progress_callback is None:
            return
        scaled = start + span * max(0.0, min(1.0, float(progress)))
        progress_callback(scaled, message)

    return _callback


def _parser_progress_heartbeat(
    progress_callback: Optional[ParserProgressCallback],
    stop_event: threading.Event,
) -> None:
    if progress_callback is None:
        return
    checkpoints = [
        (0.34, "正在提取当季 KPI 与利润表主线..."),
        (0.52, "正在整理业务结构、地区结构与分部口径..."),
        (0.68, "正在提炼管理层表述、风险与催化剂..."),
        (0.82, "正在收束官方解析结果并准备合并字段..."),
    ]
    for progress, message in checkpoints:
        if stop_event.wait(1.1):
            return
        progress_callback(progress, message)


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _clean_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            nested = _clean_mapping(value)
            if nested:
                cleaned[key] = nested
            continue
        if _has_value(value):
            cleaned[key] = value
    return cleaned


def _merge_parsed_payload(primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    if not primary:
        return dict(fallback)
    merged = dict(primary)
    list_prefer_fallback_keys = {
        "current_segments",
        "current_geographies",
        "management_themes",
        "qna_themes",
        "risks",
        "catalysts",
        "call_quote_cards",
        "evidence_cards",
    }
    for key, value in fallback.items():
        if not _has_value(value):
            continue
        existing = merged.get(key)
        if key == "coverage_notes" and isinstance(value, list):
            combined: list[Any] = []
            seen: set[str] = set()
            for item in list(existing or []) + list(value):
                normalized = str(item).strip()
                if not normalized or normalized in seen:
                    continue
                combined.append(item)
                seen.add(normalized)
            merged[key] = combined
            continue
        if key in list_prefer_fallback_keys and isinstance(existing, list) and isinstance(value, list):
            existing_len = len(existing)
            fallback_len = len(value)
            if fallback_len > existing_len and (existing_len <= 1 or (existing_len <= 2 and fallback_len >= existing_len + 2)):
                merged[key] = value
            continue
        if isinstance(existing, dict) and isinstance(value, dict):
            nested = dict(existing)
            for nested_key, nested_value in value.items():
                if not _has_value(nested_value):
                    continue
                if not _has_value(nested.get(nested_key)):
                    nested[nested_key] = nested_value
            merged[key] = nested
            continue
        if not _has_value(existing):
            merged[key] = value
    management_themes = [dict(item) for item in list(merged.get("management_themes") or []) if isinstance(item, dict)]
    qna_themes = [dict(item) for item in list(merged.get("qna_themes") or []) if isinstance(item, dict)]
    risks = [dict(item) for item in list(merged.get("risks") or []) if isinstance(item, dict)]
    catalysts = [dict(item) for item in list(merged.get("catalysts") or []) if isinstance(item, dict)]
    if management_themes or qna_themes:
        qna_themes = _ensure_minimum_qna_themes(qna_themes, management_themes, risks, catalysts)
        management_themes = _ensure_minimum_management_themes(management_themes, qna_themes)
        merged["qna_themes"] = qna_themes
        merged["management_themes"] = management_themes
    return merged


def _clean_text(text: str) -> str:
    return (
        text.replace("\u00a0", " ")
        .replace("\u2009", " ")
        .replace("\u202f", " ")
        .replace("\r", " ")
        .replace("’", "'")
        .replace("–", "-")
        .replace("—", "-")
    )


def _flatten_text(text: str) -> str:
    return re.sub(r"\s+", " ", _clean_text(text)).strip()


def _normalize_html_table_key(text: str) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(text or "").lower())


def _extract_html_tables(html_text: str) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for table_html in re.findall(r"<table\b.*?</table>", str(html_text or ""), re.IGNORECASE | re.DOTALL):
        rows: list[list[str]] = []
        for row_html in re.findall(r"<tr\b.*?</tr>", table_html, re.IGNORECASE | re.DOTALL):
            cells: list[str] = []
            for _, cell_html in re.findall(r"<t[dh]\b([^>]*)>(.*?)</t[dh]>", row_html, re.IGNORECASE | re.DOTALL):
                cleaned = re.sub(r"<br\s*/?>", " ", cell_html, flags=re.IGNORECASE)
                cleaned = re.sub(r"<[^>]+>", " ", cleaned)
                cleaned = unescape(cleaned)
                cleaned = re.sub(r"\s+", " ", cleaned).strip()
                if cleaned:
                    cells.append(cleaned)
            normalized_cells: list[str] = []
            for cell in cells:
                if cell in {")", "%"} and normalized_cells:
                    normalized_cells[-1] = f"{normalized_cells[-1]}{cell}"
                    continue
                normalized_cells.append(cell)
            if normalized_cells:
                rows.append(normalized_cells)
        if rows:
            tables.append(rows)
    return tables


def _material_raw_html_text(material: dict[str, Any]) -> str:
    cached = material.get("_raw_html_text")
    if isinstance(cached, str):
        return cached
    raw_path = Path(str(material.get("raw_path") or ""))
    if not raw_path.exists() or raw_path.suffix.lower() not in {".html", ".htm"}:
        material["_raw_html_text"] = ""
        return ""
    try:
        html_text = raw_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        html_text = ""
    material["_raw_html_text"] = html_text
    return html_text


def _material_html_tables(material: dict[str, Any]) -> list[list[list[str]]]:
    cached = material.get("_html_tables")
    if isinstance(cached, list):
        return cached
    html_text = _material_raw_html_text(material)
    tables = _extract_html_tables(html_text) if html_text else []
    material["_html_tables"] = tables
    return tables


def _html_table_scale(rows: list[list[str]]) -> float:
    header_text = " ".join(" ".join(row[:4]) for row in rows[:6]).lower()
    if "in trillions" in header_text or "trillions" in header_text or " trillion" in header_text:
        return 1_000_000_000_000
    if "in billions" in header_text or "billions" in header_text:
        return 1_000_000_000
    if "in millions" in header_text or "millions" in header_text:
        return 1_000_000
    if "in thousands" in header_text or "thousands" in header_text:
        return 1_000
    return 1


def _infer_html_table_scale(values: list[float], base_scale: float) -> float:
    if base_scale != 1 or not values:
        return base_scale
    max_abs = max(abs(value) for value in values)
    if max_abs >= 100_000:
        return 1_000
    if max_abs >= 100:
        return 1_000_000
    return 1


def _find_html_table_row(
    rows: list[list[str]],
    labels: list[str],
) -> list[str] | None:
    row_map = {
        _normalize_html_table_key(row[0]): row
        for row in rows
        if row and len(row) > 1 and str(row[0] or "").strip()
    }
    normalized_labels = [_normalize_html_table_key(label) for label in labels if str(label or "").strip()]
    for normalized_label in normalized_labels:
        for key, row in row_map.items():
            if key == normalized_label or key.startswith(normalized_label):
                return row
    return None


def _html_row_numeric_series(row: list[str] | None) -> list[float]:
    if row is None:
        return []
    values: list[float] = []
    for cell in row[1:]:
        cell_text = str(cell or "")
        if "%" in cell_text:
            continue
        parsed = _parse_number(cell_text)
        if parsed is None:
            continue
        values.append(parsed)
    if len(values) >= 3 and abs(values[0] - round(values[0])) < 1e-9 and 0 <= values[0] <= 999:
        trailing_values = values[1:]
        if trailing_values and max(abs(value) for value in trailing_values) > max(abs(values[0]) * 100, 10_000):
            values = trailing_values
    return values


def _extract_html_table_metric_from_rows(
    rows: list[list[str]],
    labels: list[str],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    row = _find_html_table_row(rows, labels)
    series = _html_row_numeric_series(row)
    if len(series) < 2:
        return (None, None, None)
    scale = _infer_html_table_scale(series[:2], _html_table_scale(rows))
    current = round(float(series[0]) * scale / 1_000_000_000, 3)
    prior = round(float(series[1]) * scale / 1_000_000_000, 3)
    return (current, prior, _pct_change(current, prior))


def _extract_html_table_metric_from_material(
    material: dict[str, Any],
    labels: list[str],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    best: tuple[Optional[float], Optional[float], Optional[float]] = (None, None, None)
    best_score: tuple[int, float] = (-1, -1.0)
    for rows in _material_html_tables(material):
        current, prior, yoy = _extract_html_statement_metric_from_rows(rows, labels, None)
        if current is None:
            continue
        score = (int(prior is not None), float(current))
        if score > best_score:
            best = (current, prior, yoy)
            best_score = score
    return best


def _statement_span_from_html_header(cell_text: str) -> Optional[int]:
    normalized = _normalize_html_table_key(cell_text)
    if not normalized:
        return None
    if "threemonthsended" in normalized or "quarterended" in normalized:
        return 1
    if "sixmonthsended" in normalized:
        return 2
    if "ninemonthsended" in normalized:
        return 3
    if "yearended" in normalized or "twelvemonthsended" in normalized or "twelvemonthsended" in normalized:
        return 4
    chinese_match = re.search(r"(12|[369]|十二|三|六|九)个月", str(cell_text or ""))
    if chinese_match:
        token = chinese_match.group(1)
        return {"3": 1, "三": 1, "6": 2, "六": 2, "9": 3, "九": 3, "12": 4, "十二": 4}.get(token)
    return None


def _calendar_quarter_from_html_date_label(cell_text: str) -> Optional[str]:
    cell = str(cell_text or "").strip()
    if not cell:
        return None
    quarter_match = re.fullmatch(r"Q([1-4])\s+(\d{4})", cell, re.IGNORECASE)
    if quarter_match:
        return f"{int(quarter_match.group(2))}Q{int(quarter_match.group(1))}"
    month_match = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2})(?:,)?\s+(\d{4})", cell)
    if month_match:
        month_token = month_match.group(1).strip().lower()[:3]
        month_lookup = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        month = month_lookup.get(month_token)
        if month is not None:
            return f"{int(month_match.group(3))}Q{((month - 1) // 3) + 1}"
    chinese_match = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", cell)
    if chinese_match:
        year = int(chinese_match.group(1))
        month = int(chinese_match.group(2))
        if 1 <= month <= 12:
            return f"{year}Q{((month - 1) // 3) + 1}"
    iso_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", cell)
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        if 1 <= month <= 12:
            return f"{year}Q{((month - 1) // 3) + 1}"
    return None


def _distribute_html_statement_periods(
    quarters: list[str],
    spans: list[int],
) -> list[dict[str, Any]]:
    if not quarters:
        return []
    normalized_spans = [int(span) for span in spans if int(span or 0) > 0]
    if not normalized_spans:
        normalized_spans = [1]
    if len(normalized_spans) > len(quarters):
        normalized_spans = normalized_spans[: len(quarters)]
    columns: list[dict[str, Any]] = []
    cursor = 0
    remaining_groups = len(normalized_spans)
    for index, span in enumerate(normalized_spans):
        remaining_quarters = len(quarters) - cursor
        if remaining_quarters <= 0:
            break
        slots = 1 if remaining_groups <= 1 else max(1, remaining_quarters // remaining_groups)
        if index == len(normalized_spans) - 1:
            slots = remaining_quarters
        for quarter in quarters[cursor : cursor + slots]:
            columns.append({"quarter": quarter, "span": span})
        cursor += slots
        remaining_groups -= 1
    if cursor < len(quarters):
        columns.extend({"quarter": quarter, "span": normalized_spans[-1]} for quarter in quarters[cursor:])
    return columns


def _extract_html_statement_period_columns(rows: list[list[str]]) -> list[dict[str, Any]]:
    header_rows = rows[:6]
    for row_index, row in enumerate(header_rows):
        spans = [span for span in (_statement_span_from_html_header(cell) for cell in row) if span is not None]
        row_quarters = [quarter for quarter in (_calendar_quarter_from_html_date_label(cell) for cell in row) if quarter]
        if spans and row_quarters:
            return _distribute_html_statement_periods(row_quarters, spans)
        if spans and row_index + 1 < len(header_rows):
            next_row_quarters = [
                quarter
                for quarter in (_calendar_quarter_from_html_date_label(cell) for cell in header_rows[row_index + 1])
                if quarter
            ]
            if next_row_quarters:
                return _distribute_html_statement_periods(next_row_quarters, spans)
    fallback_quarters = [
        quarter
        for row in header_rows
        for quarter in (_calendar_quarter_from_html_date_label(cell) for cell in row)
        if quarter
    ]
    return [{"quarter": quarter, "span": 1} for quarter in fallback_quarters]


def _previous_year_quarter(calendar_quarter: str) -> Optional[str]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(calendar_quarter or ""))
    if not match:
        return None
    return f"{int(match.group(1)) - 1}Q{int(match.group(2))}"


def _choose_html_statement_column_indexes(
    columns: list[dict[str, Any]],
    target_calendar_quarter: Optional[str],
) -> Optional[tuple[int, int]]:
    direct_columns = [
        (index, str(column.get("quarter") or ""))
        for index, column in enumerate(columns)
        if int(column.get("span") or 1) == 1 and str(column.get("quarter") or "")
    ]
    if target_calendar_quarter:
        prior_quarter = _previous_year_quarter(target_calendar_quarter)
        current_index = next((index for index, quarter in direct_columns if quarter == target_calendar_quarter), None)
        prior_index = next((index for index, quarter in direct_columns if quarter == prior_quarter), None)
        if current_index is not None and prior_index is not None:
            return (current_index, prior_index)
    if len(direct_columns) >= 2:
        return (direct_columns[0][0], direct_columns[1][0])
    return None


def _extract_html_statement_metric_from_rows(
    rows: list[list[str]],
    labels: list[str],
    target_calendar_quarter: Optional[str],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    row = _find_html_table_row(rows, labels)
    if row is None:
        return (None, None, None)
    columns = _extract_html_statement_period_columns(rows)
    if columns:
        series = _html_row_numeric_series(row)
        if len(series) >= len(columns):
            series = series[: len(columns)]
            indexes = _choose_html_statement_column_indexes(columns, target_calendar_quarter)
            if indexes is not None:
                current_raw = series[indexes[0]]
                prior_raw = series[indexes[1]]
                scale = _infer_html_table_scale([current_raw, prior_raw], _html_table_scale(rows))
                current = round(float(current_raw) * scale / 1_000_000_000, 3)
                prior = round(float(prior_raw) * scale / 1_000_000_000, 3)
                return (current, prior, _pct_change(current, prior))
    return _extract_html_table_metric_from_rows(rows, labels)


GENERIC_STATEMENT_ROW_ALIASES: dict[str, tuple[str, ...]] = {
    "revenue": ("Revenue", "Revenues", "Total revenue", "Total revenues", "Net revenue", "Net revenues", "Net sales", "Total net sales"),
    "cost_of_revenue": ("Cost of revenue", "Cost of revenues", "Cost of sales", "Total cost of revenue", "Total cost of sales"),
    "gross_profit": ("Gross profit", "Gross margin"),
    "sales_marketing": ("Sales and marketing", "Sales and marketing expense", "Selling and marketing"),
    "general_admin": ("General and administrative", "General and administrative expense", "Administrative expense"),
    "sgna": ("Selling, general and administrative", "Selling general and administrative", "Selling, general, and administrative"),
    "rnd": ("Research and development", "Research and development expense", "Research and development costs"),
    "fulfillment": ("Fulfillment", "Fulfillment expense"),
    "operating_expenses": ("Operating expenses", "Total operating expenses", "Costs and expenses"),
    "operating_income": ("Operating income", "Income from operations", "Operating profit"),
    "pretax_income": ("Income before taxes", "Income before income taxes", "Income before tax", "Pretax income", "Profit before tax"),
    "tax": ("Income tax expense", "Provision for income taxes", "Tax expense"),
    "net_income": ("Net income", "Net earnings", "Profit", "Net income attributable to common shareholders"),
}


def _extract_generic_statement_from_html_tables(
    material: dict[str, Any],
    target_calendar_quarter: Optional[str] = None,
) -> dict[str, Any]:
    best: dict[str, Any] = {}
    best_score = -1

    for rows in _material_html_tables(material):
        extracted: dict[str, Any] = {}
        for field_name, labels in GENERIC_STATEMENT_ROW_ALIASES.items():
            current_bn, prior_bn, yoy_pct = _extract_html_statement_metric_from_rows(
                rows,
                list(labels),
                target_calendar_quarter,
            )
            if current_bn is None:
                continue
            extracted[f"{field_name}_bn"] = current_bn
            extracted[f"{field_name}_prior_bn"] = prior_bn
            extracted[f"{field_name}_yoy_pct"] = yoy_pct

        revenue_bn = extracted.get("revenue_bn")
        if revenue_bn is None:
            continue

        sgna_bn = extracted.get("sgna_bn")
        if sgna_bn is None:
            sgna_parts = [
                extracted.get("sales_marketing_bn"),
                extracted.get("general_admin_bn"),
                extracted.get("fulfillment_bn"),
            ]
            if any(value is not None for value in sgna_parts):
                sgna_bn = round(sum(float(value or 0.0) for value in sgna_parts), 3)
                extracted["sgna_bn"] = sgna_bn

        gross_profit_bn = extracted.get("gross_profit_bn")
        cost_of_revenue_bn = extracted.get("cost_of_revenue_bn")
        if gross_profit_bn is None and revenue_bn is not None and cost_of_revenue_bn is not None:
            gross_profit_bn = round(float(revenue_bn) - float(cost_of_revenue_bn), 3)
            extracted["gross_profit_bn"] = gross_profit_bn
        if cost_of_revenue_bn is None and revenue_bn is not None and gross_profit_bn is not None:
            cost_of_revenue_bn = round(float(revenue_bn) - float(gross_profit_bn), 3)
            extracted["cost_of_revenue_bn"] = cost_of_revenue_bn

        operating_income_bn = extracted.get("operating_income_bn")
        operating_expenses_bn = extracted.get("operating_expenses_bn")
        if operating_expenses_bn is None and gross_profit_bn is not None and operating_income_bn is not None:
            operating_expenses_bn = round(float(gross_profit_bn) - float(operating_income_bn), 3)
            extracted["operating_expenses_bn"] = operating_expenses_bn

        annotations: list[dict[str, Any]] = []
        if gross_profit_bn is not None and revenue_bn not in (None, 0):
            gross_margin_pct = round(float(gross_profit_bn) / float(revenue_bn) * 100, 1)
            extracted["gross_margin_pct"] = gross_margin_pct
            annotations.append({"title": "毛利率", "value": format_pct(gross_margin_pct), "note": "来自官方利润表行项目。", "color": "#2563EB"})
        if operating_income_bn is not None and revenue_bn not in (None, 0):
            operating_margin_pct = round(float(operating_income_bn) / float(revenue_bn) * 100, 1)
            extracted["operating_margin_pct"] = operating_margin_pct
            annotations.append({"title": "营业利润率", "value": format_pct(operating_margin_pct), "note": "由官方收入与营业利润计算。", "color": "#0EA5E9"})
        if extracted.get("net_income_bn") is not None and revenue_bn not in (None, 0):
            net_margin_pct = round(float(extracted["net_income_bn"]) / float(revenue_bn) * 100, 1)
            extracted["net_margin_pct"] = net_margin_pct
            annotations.append({"title": "净利率", "value": format_pct(net_margin_pct), "note": "由官方收入与净利润计算。", "color": "#14B8A6"})

        opex_breakdown: list[dict[str, Any]] = []
        for name, value_bn, color in (
            ("Research and development", extracted.get("rnd_bn"), "#E11D48"),
            ("Selling, general and administrative", extracted.get("sgna_bn"), "#F43F5E"),
            ("Sales and marketing", extracted.get("sales_marketing_bn"), "#FB7185"),
            ("General and administrative", extracted.get("general_admin_bn"), "#FDA4AF"),
        ):
            if value_bn is None or revenue_bn in (None, 0):
                continue
            opex_breakdown.append(
                {
                    "name": name,
                    "value_bn": round(float(value_bn), 3),
                    "pct_of_revenue": round(float(value_bn) / float(revenue_bn) * 100, 1),
                    "color": color,
                }
            )

        extracted["income_statement"] = _clean_mapping(
            {
                "subtitle": "利润表页优先采用官方 HTML 财报表格行项目，并自动计算毛利率与费用占比。",
                "sources": [],
                "opex_breakdown": opex_breakdown,
                "annotations": annotations[:3],
            }
        )
        score = sum(1 for key in ("revenue_bn", "cost_of_revenue_bn", "gross_profit_bn", "operating_income_bn", "pretax_income_bn", "net_income_bn") if extracted.get(key) is not None)
        if score > best_score:
            best = extracted
            best_score = score

    return best


def _load_materials(
    source_materials: list[dict[str, Any]],
    progress_callback: Optional[ParserProgressCallback] = None,
) -> list[dict[str, Any]]:
    materials: list[dict[str, Any]] = []
    total = max(len(source_materials), 1)
    for index, item in enumerate(source_materials, start=1):
        if progress_callback is not None:
            progress_callback((index - 1) / total, f"正在读取材料 {index}/{total}：{item.get('label', 'Source')}")
        if item.get("status") not in {"fetched", "cached"}:
            continue
        text_path = item.get("text_path")
        if not text_path:
            continue
        path = Path(str(text_path))
        if not path.exists():
            continue
        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        if not raw_text.strip():
            continue
        materials.append(
            {
                **item,
                "raw_text": raw_text,
                "flat_text": _flatten_text(raw_text),
            }
        )
    if progress_callback is not None:
        progress_callback(1.0, f"已载入 {len(materials)} 份可解析材料。")
    return materials


def _ensure_loaded_materials(materials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not materials:
        return []
    if all("flat_text" in item for item in materials):
        return materials
    return _load_materials(materials)


def _pick_material(
    materials: list[dict[str, Any]],
    *,
    kind: Optional[str] = None,
    role: Optional[str] = None,
    label_contains: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    candidates = materials
    if kind is not None:
        candidates = [item for item in candidates if item.get("kind") == kind]
    if role is not None:
        candidates = [item for item in candidates if item.get("role") == role]
    if label_contains is not None:
        needle = label_contains.lower()
        candidates = [item for item in candidates if needle in str(item.get("label") or "").lower()]
    if not candidates:
        return None
    def _score(item: dict[str, Any]) -> tuple[int, int, int]:
        raw_length = len(str(item.get("raw_text") or ""))
        label = str(item.get("label") or "").lower()
        annual = _is_annual_material(item)

        filing_priority = 0
        if kind == "sec_filing" and label_contains is None:
            # Default to quarter-level filings when both 10-Q / 6-K and 10-K / 20-F
            # are present, otherwise old quarters can accidentally parse annual totals.
            filing_priority += 3 if not annual else 0
            if "10-q" in label or "6-k" in label or "quarterly" in label:
                filing_priority += 2
            if "10-k/a" in label or "20-f/a" in label:
                filing_priority -= 1

        role_priority = 0
        if kind == "official_release" and role is None:
            if str(item.get("role") or "") == "earnings_release":
                role_priority += 2
            raw_path = str(item.get("raw_path") or "")
            url = str(item.get("url") or "")
            label_text = str(item.get("label") or "").lower()
            if any(token in label_text for token in ("nyse", "303a", "governance", "annual meeting")):
                role_priority -= 4
            if raw_path.lower().endswith(".pdf") or url.lower().endswith(".pdf"):
                role_priority += 2
            if any(token in label_text for token in ("data summary", "view pdf", "download pdf", "pdf")):
                role_priority += 1
        if kind == "presentation" and role is None:
            if str(item.get("role") or "") == "earnings_presentation":
                role_priority += 1
            if str(item.get("role") or "") == "earnings_commentary":
                role_priority += 2

        return (filing_priority, role_priority, raw_length)

    return max(candidates, key=_score)


def _ordered_narrative_materials(materials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    preferred_roles = (
        "earnings_release",
        "earnings_commentary",
        "earnings_presentation",
        "call_summary",
    )
    preferred_kinds = ("official_release", "presentation", "call_summary")
    ordered: list[dict[str, Any]] = []
    for role in preferred_roles:
        for item in materials:
            if item in ordered or item.get("role") != role:
                continue
            ordered.append(item)
    for kind in preferred_kinds:
        for item in materials:
            if item in ordered or item.get("kind") != kind:
                continue
            ordered.append(item)
    for item in materials:
        if item not in ordered:
            ordered.append(item)
    return ordered


def _search(pattern: str, text: str) -> Optional[re.Match[str]]:
    return re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)


def _parse_number(token: Optional[str]) -> Optional[float]:
    if token is None:
        return None
    cleaned = (
        str(token)
        .replace("$", "")
        .replace(",", "")
        .replace("%", "")
        .replace("(", "-")
        .replace(")", "")
        .replace("−", "-")
        .replace("—", "-")
        .replace(" ", "")
        .strip()
    )
    if cleaned in {"", "-", "--"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _bn_from_billions(token: Optional[str]) -> Optional[float]:
    return _parse_number(token)


def _bn_from_millions(token: Optional[str]) -> Optional[float]:
    value = _parse_number(token)
    if value is None:
        return None
    return value / 1000


def _coalesce_number(*values: Any) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _pct_value(token: Optional[str]) -> Optional[float]:
    return _parse_number(token)


def _pct_change(current: Optional[float], prior: Optional[float]) -> Optional[float]:
    if current is None or prior in (None, 0):
        return None
    return (float(current) / float(prior) - 1) * 100


def _safe_ratio_pct(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator) * 100


def _directional_pct(direction: Optional[str], value: Optional[str]) -> Optional[float]:
    normalized = str(direction or "").lower()
    if normalized == "relatively unchanged":
        return 0.0
    parsed = _pct_value(value)
    if parsed is None:
        return None
    positive_tokens = ("up", "increase", "increased", "grew", "grow", "rose", "higher")
    negative_tokens = ("down", "decrease", "decreased", "declined", "decline", "fell", "lower")
    if any(token in normalized for token in positive_tokens):
        return parsed
    if any(token in normalized for token in negative_tokens):
        return -parsed
    return parsed


def _midpoint(low: Optional[float], high: Optional[float]) -> Optional[float]:
    if low is None or high is None:
        return None
    return (low + high) / 2


def _table_label_pattern(label: str) -> str:
    return rf"{re.escape(label)}(?:\s*\([^)]*\)\s*)*"


def _table_pct_pattern() -> str:
    return r"(\(?\s*-?[0-9]+(?:\.[0-9]+)?\s*\)?)"


def _millions_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{_table_label_pattern(label)}"
        r"\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+"
        rf"{_table_pct_pattern()}\s*%"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None)
    return (_bn_from_millions(match.group(1)), _bn_from_millions(match.group(2)), _pct_value(match.group(3)))


def _millions_row_no_pct(flat_text: str, label: str) -> tuple[Optional[float], Optional[float]]:
    pattern = rf"{_table_label_pattern(label)}\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)"
    match = _search(pattern, flat_text)
    if not match:
        return (None, None)
    return (_bn_from_millions(match.group(1)), _bn_from_millions(match.group(2)))


def _percent_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float]]:
    pattern = rf"{_table_label_pattern(label)}\s+([0-9]+(?:\.[0-9]+)?)\s*%\s+([0-9]+(?:\.[0-9]+)?)\s*%"
    match = _search(pattern, flat_text)
    if not match:
        return (None, None)
    return (_pct_value(match.group(1)), _pct_value(match.group(2)))


def _five_quarter_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{re.escape(label)}\s+(?:\([^)]+\)\s+)*"
        r"([0-9,]+(?:\.[0-9]+)?|—)\s+([0-9,]+(?:\.[0-9]+)?|—)\s+([0-9,]+(?:\.[0-9]+)?|—)\s+"
        r"([0-9,]+(?:\.[0-9]+)?|—)\s+([0-9,]+(?:\.[0-9]+)?|—)\s+(\(?-?[0-9]+(?:\.[0-9]+)?\)?)%"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None)
    current = _bn_from_millions(match.group(5))
    prior = _bn_from_millions(match.group(1))
    yoy = _pct_value(match.group(6))
    return (current, prior, yoy)


def _five_quarter_pct_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{re.escape(label)}\s+(?:\([^)]+\)\s+)*"
        r"([0-9]+(?:\.[0-9]+)?)%\s+([0-9]+(?:\.[0-9]+)?)%\s+([0-9]+(?:\.[0-9]+)?)%\s+"
        r"([0-9]+(?:\.[0-9]+)?)%\s+([0-9]+(?:\.[0-9]+)?)%\s+(\(?-?[0-9]+(?:\.[0-9]+)?\)?)\s*(?:bp|%)"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None)
    current = _pct_value(match.group(5))
    prior = _pct_value(match.group(1))
    yoy = _pct_change(current, prior)
    return (current, prior, yoy)


def _reversed_millions_row_no_pct(flat_text: str, label: str) -> tuple[Optional[float], Optional[float]]:
    pattern = rf"{_table_label_pattern(label)}\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)"
    match = _search(pattern, flat_text)
    if not match:
        return (None, None)
    return (_bn_from_millions(match.group(2)), _bn_from_millions(match.group(1)))


def _reversed_millions_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{_table_label_pattern(label)}"
        r"\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+"
        rf"{_table_pct_pattern()}\s*%"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None)
    return (_bn_from_millions(match.group(2)), _bn_from_millions(match.group(1)), _pct_value(match.group(3)))


def _nvidia_gaap_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{re.escape(label)}\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+"
        r"(\(?-?[0-9]+(?:\.[0-9]+)?\)?)\s*%\s+(\(?-?[0-9]+(?:\.[0-9]+)?\)?)\s*%"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None, None, None)
    return (
        _bn_from_millions(match.group(1)),
        _bn_from_millions(match.group(2)),
        _bn_from_millions(match.group(3)),
        _pct_value(match.group(4)),
        _pct_value(match.group(5)),
    )


def _quote_card(speaker: str, quote: str, analysis: str, source_label: str) -> dict[str, str]:
    return {
        "speaker": speaker,
        "quote": quote.strip(),
        "analysis": analysis.strip(),
        "source_label": source_label,
    }


def _theme(label: str, score: float, note: str) -> dict[str, Any]:
    return {
        "label": label,
        "score": int(max(32, min(98, round(score)))),
        "note": note.strip(),
    }


def _segment(name: str, value_bn: Optional[float], yoy_pct: Optional[float]) -> Optional[dict[str, Any]]:
    if value_bn is None:
        return None
    return {
        "name": name,
        "value_bn": round(float(value_bn), 3),
        "yoy_pct": None if yoy_pct is None else round(float(yoy_pct), 1),
    }


def _segment_list(*segments: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in segments if item]


def _segment_family(name: str) -> str:
    lowered = str(name or "").casefold()
    geography_like = {
        "north america",
        "international",
        "united states",
        "united states and canada",
        "americas",
        "europe",
        "asia-pacific",
        "asia pacific",
        "apac",
        "apj",
        "greater china",
        "japan",
        "rest of asia pacific",
        "rest of world",
        "emea",
    }
    return "geography" if lowered in geography_like else "business"


def _aggregate_segment_candidates(company_id: str) -> set[str]:
    return {
        "apple": {"products"},
        "alphabet": {"google services"},
    }.get(str(company_id), set())


def _prune_overlapping_segments(
    company_id: str,
    segments: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    positive = [dict(item) for item in segments if float(item.get("value_bn") or 0.0) > 0]
    if str(company_id) == "visa":
        # Visa discloses revenue pools before client incentives; totals intentionally
        # do not reconcile 1:1 to net revenue. Keep the official pool set as-is.
        return positive
    if len(positive) <= 1:
        return positive

    aggregate_names = _aggregate_segment_candidates(str(company_id))
    best_subset = positive
    best_score = float("-inf")
    target = float(revenue_bn) if revenue_bn not in (None, 0) else None

    for size in range(1, len(positive) + 1):
        for indexes in combinations(range(len(positive)), size):
            subset = [positive[index] for index in indexes]
            total = sum(float(item.get("value_bn") or 0.0) for item in subset)
            if total <= 0:
                continue
            if target is not None:
                ratio = total / target
                if not 0.45 <= ratio <= 1.18:
                    continue
                closeness = 1 - abs(target - total) / max(target, 1.0)
            else:
                closeness = 0.6
            aggregate_count = sum(str(item.get("name") or "").casefold() in aggregate_names for item in subset)
            business_count = sum(_segment_family(str(item.get("name") or "")) == "business" for item in subset)
            geography_count = len(subset) - business_count
            family_bonus = 0.06 if business_count == len(subset) or geography_count == len(subset) else -0.08
            detail_bonus = len(subset) * 0.03 + business_count * 0.015 - aggregate_count * 0.09
            score = closeness + family_bonus + detail_bonus
            if score > best_score:
                best_score = score
                best_subset = subset

    if str(company_id) in {"micron", "visa"}:
        # Micron quarterly tables are already in official BU order (CMBU/CDBU/MCBU/AEBU).
        # Visa revenue pools are also disclosed in a stable official order.
        # Keep this sequence stable instead of re-sorting by value.
        return best_subset

    best_subset.sort(key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
    return best_subset


def _geography_list(*geographies: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [item for item in geographies if item],
        key=lambda item: float(item.get("value_bn") or 0.0),
        reverse=True,
    )


def _top_segment(segments: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not segments:
        return None
    return max(segments, key=lambda item: float(item.get("value_bn") or 0.0))


def _fastest_segment(segments: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    ranked = [item for item in segments if item.get("yoy_pct") is not None]
    if not ranked:
        return None
    return max(ranked, key=lambda item: float(item.get("yoy_pct") or 0.0))


def _segment_share(segment: Optional[dict[str, Any]], revenue_bn: Optional[float]) -> Optional[float]:
    if segment is None or revenue_bn in (None, 0):
        return None
    return float(segment["value_bn"]) / float(revenue_bn) * 100


def _segments_reasonable_for_revenue(
    segments: list[dict[str, Any]],
    revenue_bn: Optional[float],
    *,
    lower_ratio: float = 0.55,
    upper_ratio: float = 1.35,
) -> bool:
    if not segments or revenue_bn in (None, 0):
        return False
    total = sum(float(item.get("value_bn") or 0.0) for item in segments)
    ratio = total / float(revenue_bn)
    return lower_ratio <= ratio <= upper_ratio


def _geographies_look_suspicious(geographies: list[dict[str, Any]], revenue_bn: Optional[float]) -> bool:
    if not geographies:
        return False
    values = [max(float(item.get("value_bn") or 0.0), 0.0) for item in geographies]
    total = sum(values)
    if total <= 0:
        return True
    shares = [value / total for value in values]
    share_map = {
        str(item.get("name") or "").casefold(): max(float(item.get("value_bn") or 0.0), 0.0) / total
        for item in geographies
    }
    extreme_micro_item = any(
        share <= 0.01 and abs(float(item.get("yoy_pct") or 0.0)) >= 300
        for item, share in zip(geographies, shares)
    )
    if max(shares) >= 0.95 and min(shares) <= 0.02 and extreme_micro_item:
        return True
    for us_label in ("united states", "u.s.", "u.s"):
        us_share = share_map.get(us_label)
        if us_share is None:
            continue
        paired_labels = [
            "international",
            "other international",
            "other countries",
            "non-u.s.",
        ]
        if any(label in share_map for label in paired_labels) and us_share < 0.02:
            return True
    if revenue_bn not in (None, 0):
        ratio = total / float(revenue_bn)
        if ratio < 0.08:
            return True
    return False


def _material_has_geography_context(text: str) -> bool:
    flat = str(text or "").casefold()
    return any(
        phrase in flat
        for phrase in [
            "geographic",
            "geography",
            "region",
            "customers were located",
            "revenue by geographic",
            "geographic area",
            "geographic region",
        ]
    )


def _ambiguous_geography_profile(profile: list[dict[str, Any]]) -> bool:
    names = {str(item.get("name") or "").casefold() for item in profile}
    return names in (
        {"north america", "international"},
        {"united states", "international"},
        {"united states", "canada", "other international"},
    )


def _geography_names(geographies: list[dict[str, Any]]) -> set[str]:
    return {str(item.get("name") or "").strip().casefold() for item in geographies if str(item.get("name") or "").strip()}


def _geography_detail_score(geographies: list[dict[str, Any]]) -> int:
    if not geographies:
        return -100
    names = _geography_names(geographies)
    score = len(names) * 10
    if _ambiguous_geography_profile([{"name": name} for name in names]):
        score -= 12
    coarse_penalty_tokens = ("international", "other countries", "rest of world", "non-u.s.")
    score -= sum(2 for name in names if any(token in name for token in coarse_penalty_tokens))
    score += sum(
        1
        for name in names
        if name
        not in {
            "united states",
            "u.s.",
            "u.s",
            "north america",
            "international",
            "asia",
            "europe",
            "americas",
            "japan",
            "emea",
            "apac",
            "asia-pacific",
        }
    )
    return score


def _prefer_richer_geographies(
    current: list[dict[str, Any]],
    candidate: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    if not candidate:
        return list(current)
    if _geographies_look_suspicious(list(candidate), revenue_bn):
        return list(current)
    if not current or _geographies_look_suspicious(list(current), revenue_bn):
        return list(candidate)
    current_score = _geography_detail_score(list(current))
    candidate_score = _geography_detail_score(list(candidate))
    current_names = _geography_names(list(current))
    candidate_names = _geography_names(list(candidate))
    if len(candidate_names) >= 3 and len(current_names) <= 2:
        return list(candidate)
    if len(candidate_names) >= len(current_names) + 2 and candidate_score > current_score:
        return list(candidate)
    if candidate_score >= current_score + 8:
        return list(candidate)
    return list(current)


def _guidance_midpoint_commentary(
    low_bn: Optional[float],
    high_bn: Optional[float],
    revenue_yoy_low: Optional[float] = None,
    revenue_yoy_high: Optional[float] = None,
    extra: Optional[str] = None,
) -> Optional[str]:
    if low_bn is None or high_bn is None:
        return None
    text = f"下一季收入指引区间为 {format_money_bn(low_bn)} 到 {format_money_bn(high_bn)}。"
    if revenue_yoy_low is not None and revenue_yoy_high is not None:
        text += f" 对应同比增速区间约为 {format_pct(revenue_yoy_low, signed=True)} 到 {format_pct(revenue_yoy_high, signed=True)}。"
    if extra:
        text += f" {extra.strip()}"
    return text


def _bn_from_unit(token: Optional[str], unit: Optional[str]) -> Optional[float]:
    value = _parse_number(token)
    if value is None:
        return None
    normalized_unit = str(unit or "").lower()
    if normalized_unit.startswith("trillion") or normalized_unit in {"tn", "tln"}:
        return value * 1000
    if normalized_unit.startswith("billion") or normalized_unit == "bn":
        return value
    if normalized_unit.startswith("million") or normalized_unit == "mn":
        return value / 1000
    if normalized_unit.startswith("thousand") or normalized_unit == "k":
        return value / 1_000_000
    return value


def _percent_token_value(token: Optional[str]) -> Optional[float]:
    parsed = _parse_number(token)
    if parsed is not None:
        return parsed
    normalized = re.sub(r"[^a-z-]+", " ", str(token or "").lower()).strip()
    if not normalized:
        return None
    word_map = {
        "zero": 0.0,
        "one": 1.0,
        "two": 2.0,
        "three": 3.0,
        "four": 4.0,
        "five": 5.0,
        "six": 6.0,
        "seven": 7.0,
        "eight": 8.0,
        "nine": 9.0,
        "ten": 10.0,
    }
    return word_map.get(normalized)


def _extract_growth_from_context(text: str) -> Optional[float]:
    increased_match = _search(r"(?:up|increased|grew|rose)\s+([0-9]+(?:\.[0-9]+)?)\s*%", text)
    if increased_match:
        return _pct_value(increased_match.group(1))
    decreased_match = _search(r"(?:down|decreased|declined|fell)\s+([0-9]+(?:\.[0-9]+)?)\s*%", text)
    if decreased_match:
        value = _pct_value(decreased_match.group(1))
        return None if value is None else -value
    return None


def _unit_scale_to_bn_from_text(text: str) -> Optional[float]:
    normalized = _flatten_text(str(text or "")).lower()
    if not normalized:
        return None
    if re.search(r"\b(?:trillion|trillions|tn|tln)\b", normalized):
        return 1000.0
    if re.search(r"\b(?:billion|billions|bn)\b", normalized):
        return 1.0
    if re.search(r"\b(?:million|millions|mn)\b", normalized):
        return 0.001
    if re.search(r"\b(?:thousand|thousands|k)\b", normalized):
        return 0.000001
    return None


def _line_numeric_values_without_pct(text: str) -> list[float]:
    values: list[float] = []
    for match in re.finditer(r"\(?-?[0-9]+(?:\.[0-9]+)?\)?", str(text or "")):
        suffix = str(text or "")[match.end() : match.end() + 2]
        if "%" in suffix:
            continue
        parsed = _parse_number(match.group(0))
        if parsed is not None:
            values.append(parsed)
    return values


def _quarter_header_sort_key(value: str) -> tuple[int, int]:
    token = str(value or "").strip().upper()
    if not token:
        return (0, 0)
    patterns = (
        r"(?P<quarter>[1-4])Q(?P<year>\d{2,4})",
        r"Q(?P<quarter>[1-4])(?:FY)?(?P<year>\d{2,4})",
        r"(?P<year>\d{4})Q(?P<quarter>[1-4])",
    )
    for pattern in patterns:
        match = re.fullmatch(pattern, re.sub(r"[^A-Z0-9]", "", token))
        if not match:
            continue
        year = int(match.group("year"))
        if year < 100:
            year += 2000
        return (year, int(match.group("quarter")))
    year_match = re.fullmatch(r"(?:FY)?(?P<year>\d{2,4})", re.sub(r"[^A-Z0-9]", "", token))
    if year_match:
        year = int(year_match.group("year"))
        if year < 100:
            year += 2000
        return (year, 5)
    return (0, 0)


def _parse_text_metric_headers(line: str) -> list[str]:
    headers: list[str] = []
    for token in re.split(r"\s+", _clean_text(str(line or "")).strip()):
        stripped = token.strip(",:;()[]{}")
        if not stripped:
            continue
        if _quarter_header_sort_key(stripped) != (0, 0):
            headers.append(stripped)
    return headers


def _pick_text_table_current_prior_indexes(headers: list[str]) -> tuple[Optional[int], Optional[int]]:
    if len(headers) < 2:
        return (None, None)
    quarter_positions: list[tuple[int, tuple[int, int]]] = []
    annual_positions: list[tuple[int, tuple[int, int]]] = []
    for index, header in enumerate(headers):
        year, quarter = _quarter_header_sort_key(header)
        if not year:
            continue
        if quarter == 5:
            annual_positions.append((index, (year, quarter)))
        else:
            quarter_positions.append((index, (year, quarter)))
    if quarter_positions:
        current_index, current_key = max(quarter_positions, key=lambda item: item[1])
        prior_index: Optional[int] = None
        for candidate_index, candidate_key in quarter_positions:
            if candidate_key == (current_key[0] - 1, current_key[1]):
                prior_index = candidate_index
                break
        if prior_index is None:
            earlier = [item for item in quarter_positions if item[1] < current_key]
            if earlier:
                prior_index = max(earlier, key=lambda item: item[1])[0]
        return (current_index, prior_index)
    if len(annual_positions) >= 2:
        current_index, _ = max(annual_positions, key=lambda item: item[1])
        earlier = [item for item in annual_positions if item[0] != current_index]
        if earlier:
            return (current_index, max(earlier, key=lambda item: item[1])[0])
    return (len(headers) - 1, len(headers) - 2)


def _extract_labeled_text_table_metric(
    raw_text: str,
    labels: list[str],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    lines = [_flatten_text(line) for line in str(raw_text or "").splitlines() if _flatten_text(line)]
    if not lines:
        return (None, None, None)
    best: tuple[Optional[float], Optional[float], Optional[float]] = (None, None, None)
    best_score: tuple[int, float] = (-1, -1.0)
    for index, line in enumerate(lines):
        matched_label = next(
            (
                label
                for label in labels
                if re.match(rf"^{_table_label_pattern(label)}(?=\s+\(?-?[0-9])", line, flags=re.IGNORECASE)
            ),
            None,
        )
        if matched_label is None:
            continue
        header_line = next(
            (
                lines[candidate]
                for candidate in range(max(0, index - 3), index)
                if len(_parse_text_metric_headers(lines[candidate])) >= 2
            ),
            "",
        )
        headers = _parse_text_metric_headers(header_line)
        if len(headers) < 2:
            continue
        current_index, prior_index = _pick_text_table_current_prior_indexes(headers)
        if current_index is None:
            continue
        suffix = re.sub(rf"^{_table_label_pattern(matched_label)}", "", line, count=1, flags=re.IGNORECASE).strip()
        values = _line_numeric_values_without_pct(suffix)
        if len(values) < len(headers):
            continue
        scale_to_bn = _unit_scale_to_bn_from_text(" ".join(lines[max(0, index - 3) : index + 1]))
        if scale_to_bn is None:
            scale_to_bn = 0.001 if max(abs(value) for value in values[: min(4, len(values))]) >= 1_000 else 1.0
        current = round(float(values[current_index]) * scale_to_bn, 3)
        prior = round(float(values[prior_index]) * scale_to_bn, 3) if prior_index is not None and prior_index < len(values) else None
        yoy = _pct_change(current, prior)
        score = (int(prior is not None), float(current))
        if score > best_score:
            best = (current, prior, yoy)
            best_score = score
    return best


def _extract_narrative_metric(flat_text: str, label_pattern: str) -> tuple[Optional[float], Optional[float]]:
    patterns = [
        rf"(?:reported\s+)?{label_pattern}(?:\s+for\s+the\s+quarter)?(?:\s+(?:was|were|of|totaled|reached|came in at|amounted to))?\s+\$?(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>trillion|billion|million|thousand|tn|bn|mn|k)",
        rf"\$?(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>trillion|billion|million|thousand|tn|bn|mn|k)\s+(?:of\s+)?{label_pattern}",
    ]
    for pattern in patterns:
        match = _search(pattern, flat_text)
        if not match:
            continue
        amount = _bn_from_unit(match.group("amount"), match.group("unit"))
        context = flat_text[max(0, match.start() - 20) : min(len(flat_text), match.end() + 220)]
        return (amount, _extract_growth_from_context(context))
    return (None, None)


SPECIAL_ITEM_CONTEXT_MARKERS = (
    "special item",
    "special items",
    "excluding special",
    "including special",
    "one-time",
    "one time",
    "restructuring",
    "acquisition",
    "integration",
    "legal entity reorganization",
    "impairment",
    "settlement",
    "tax reform",
    "tax benefit",
    "tax charge",
    "remeasurement",
)


def _match_excerpt(flat_text: str, match: re.Match[str], *, before: int = 48, after: int = 180) -> str:
    start = max(0, match.start() - before)
    end = min(len(flat_text), match.end() + after)
    return flat_text[start:end]


def _extract_company_level_narrative_metric(flat_text: str, label_pattern: str) -> tuple[Optional[float], Optional[float]]:
    sentence_lead = r"(?:^|[.;:!?]\s+|•\s+|\u2022\s+)"
    patterns = [
        rf"{sentence_lead}(?:reported|generated|posted|delivered|recorded)?\s*(?:the company's\s+)?(?P<label>{label_pattern})(?:\s+for\s+the\s+(?:quarter|period))?(?:\s+(?:was|were|of|totaled|reached|came in at|amounted to))?\s+\$?(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>billion|million)",
        rf"{sentence_lead}(?:reported|generated|posted|delivered|recorded)\s+\$?(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>billion|million)\s+(?:of\s+)?(?P<label>{label_pattern})",
    ]
    best: tuple[int, Optional[float], Optional[float]] = (-10_000, None, None)
    for pattern in patterns:
        for match in re.finditer(pattern, flat_text, flags=re.IGNORECASE | re.DOTALL):
            amount = _bn_from_unit(match.group("amount"), match.group("unit"))
            if amount is None:
                continue
            context = _match_excerpt(flat_text, match).lower()
            score = 0
            if "quarter" in context or "period" in context:
                score += 3
            if any(token in context for token in ("total revenue", "net revenue", "net operating revenue", "total sales")):
                score += 4
            if any(token in context for token in ("full-year", "fiscal year", "annual")):
                score -= 7
            if score > best[0]:
                best = (score, amount, _extract_growth_from_context(context))
    return (best[1], best[2])


def _statement_value_pair(flat_text: str, label_pattern: str) -> tuple[Optional[float], Optional[float]]:
    match = _search(
        rf"{label_pattern}\s+\$?\s*(?P<current>[0-9,]+(?:\.[0-9]+)?)\s+\$?\s*(?P<prior>[0-9,]+(?:\.[0-9]+)?)",
        flat_text,
    )
    if not match:
        return (None, None)
    return (_parse_number(match.group("current")), _parse_number(match.group("prior")))


def _merge_profit_signal(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if key == "special_item_detected":
            merged[key] = bool(merged.get(key)) or bool(value)
            continue
        if not _has_value(merged.get(key)) and _has_value(value):
            merged[key] = value
    return merged


def _extract_profit_signal(flat_text: str) -> dict[str, Any]:
    signal: dict[str, Any] = {
        "special_item_detected": any(marker in flat_text.lower() for marker in SPECIAL_ITEM_CONTEXT_MARKERS),
    }
    reported_current, reported_prior = _statement_value_pair(
        flat_text,
        r"Net income,\s+as reported(?:\s*\([0-9]+\))?",
    )
    adjusted_current, adjusted_prior = _statement_value_pair(
        flat_text,
        r"Net income,\s+as adjusted(?:\s*\([0-9]+\))?",
    )
    reported_eps, reported_prior_eps = _statement_value_pair(
        flat_text,
        r"Diluted earnings per share,\s+as reported(?:\s*\([0-9]+\))?",
    )
    adjusted_eps, adjusted_prior_eps = _statement_value_pair(
        flat_text,
        r"Diluted earnings per share,\s+as adjusted(?:\s*\([0-9]+\))?",
    )
    signal.update(
        _clean_mapping(
            {
                "reported_net_income_bn": _bn_from_millions(reported_current) if reported_current is not None and reported_current > 100 else reported_current,
                "reported_prior_net_income_bn": _bn_from_millions(reported_prior) if reported_prior is not None and reported_prior > 100 else reported_prior,
                "reported_eps": reported_eps,
                "reported_prior_eps": reported_prior_eps,
                "adjusted_net_income_bn": _bn_from_millions(adjusted_current) if adjusted_current is not None and adjusted_current > 100 else adjusted_current,
                "adjusted_prior_net_income_bn": _bn_from_millions(adjusted_prior) if adjusted_prior is not None and adjusted_prior > 100 else adjusted_prior,
                "adjusted_eps": adjusted_eps,
                "adjusted_prior_eps": adjusted_prior_eps,
            }
        )
    )

    narrative_patterns = [
        (
            "reported",
            rf"(?:^|[.;:!?]\s+|•\s+|\u2022\s+)GAAP(?:\s+\w+){{0,4}}\s+net income(?:[^.;]{{0,140}}?)\$\s*(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>billion|million)(?:[^.;]{{0,120}}?)(?:or|and)\s+\$?(?P<eps>[0-9]+(?:\.[0-9]+)?)\s+per share",
        ),
        (
            "adjusted",
            rf"(?:^|[.;:!?]\s+|•\s+|\u2022\s+)(?:Adjusted|Non-GAAP)(?:\s+\w+){{0,4}}\s+net income(?:[^.;]{{0,140}}?)\$\s*(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>billion|million)(?:[^.;]{{0,120}}?)(?:or|and)\s+\$?(?P<eps>[0-9]+(?:\.[0-9]+)?)\s+per share",
        ),
        (
            "reported",
            rf"(?:^|[.;:!?]\s+|•\s+|\u2022\s+)(?:Net income|Net earnings|Earnings attributable to [A-Za-z& .]+)(?:[^.;]{{0,120}}?)\$\s*(?P<amount>[0-9,]+(?:\.[0-9]+)?)\s*(?P<unit>billion|million)(?:[^.;]{{0,120}}?)(?:or|and)\s+\$?(?P<eps>[0-9]+(?:\.[0-9]+)?)\s+per share",
        ),
    ]
    for bucket, pattern in narrative_patterns:
        match = _search(pattern, flat_text)
        if not match:
            continue
        amount = _bn_from_unit(match.group("amount"), match.group("unit"))
        eps = _parse_number(match.group("eps"))
        excerpt = _match_excerpt(flat_text, match)
        prefix = "adjusted" if bucket == "adjusted" else "reported"
        signal = _merge_profit_signal(
            signal,
            {
                f"{prefix}_net_income_bn": amount,
                f"{prefix}_eps": eps,
                f"{prefix}_context": excerpt,
            },
        )
    return signal


def _should_prefer_adjusted_profit_signal(
    signal: dict[str, Any],
    revenue_bn: Optional[float],
) -> bool:
    adjusted = signal.get("adjusted_net_income_bn")
    if adjusted is None:
        return False
    reported = signal.get("reported_net_income_bn")
    if reported is None:
        return bool(signal.get("special_item_detected"))
    if not bool(signal.get("special_item_detected")):
        return False
    gap = abs(float(adjusted) - float(reported))
    gap_threshold = max(0.35, float(revenue_bn or 0.0) * 0.08)
    if gap < gap_threshold:
        return False
    if abs(float(reported)) < 1e-6:
        return True
    ratio = abs(float(adjusted)) / max(abs(float(reported)), 1e-6)
    if ratio >= 1.35:
        return True
    if revenue_bn not in (None, 0):
        reported_margin = abs(float(reported)) / float(revenue_bn)
        adjusted_margin = abs(float(adjusted)) / float(revenue_bn)
        if reported_margin <= 0.12 and adjusted_margin >= reported_margin + 0.08:
            return True
    return False


def _extract_narrative_eps(flat_text: str) -> tuple[Optional[float], Optional[float]]:
    patterns = [
        r"(?:diluted\s+)?earnings per share(?:\s+\(gaap\))?(?:\s+(?:was|were|of))?\s+\$?(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<context>.{0,120})",
        r"(?:gaap\s+)?eps(?:\s+(?:was|were|of))?\s+\$?(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<context>.{0,120})",
    ]
    for pattern in patterns:
        match = _search(pattern, flat_text)
        if not match:
            continue
        return (_parse_number(match.group("value")), _extract_growth_from_context(match.group("context")))
    return (None, None)


def _per_share_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float]]:
    pattern = rf"{re.escape(label)}\s+\$?\s*([0-9]+(?:\.[0-9]+)?)\s+\$?\s*([0-9]+(?:\.[0-9]+)?)"
    match = _search(pattern, flat_text)
    if not match:
        return (None, None)
    return (_parse_number(match.group(1)), _parse_number(match.group(2)))


def _extract_table_metric(
    flat_text: str,
    labels: list[str],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    def normalize_yoy(current: Optional[float], prior: Optional[float], yoy: Optional[float]) -> Optional[float]:
        if yoy is not None and abs(float(yoy)) <= 500:
            return yoy
        derived = _pct_change(current, prior)
        if derived is not None and abs(float(derived)) <= 500:
            return derived
        return None

    for label in labels:
        current, prior, yoy = _millions_row(flat_text, label)
        if current is not None:
            return (current, prior, normalize_yoy(current, prior, yoy))
        current, prior = _millions_row_no_pct(flat_text, label)
        if current is not None:
            return (current, prior, normalize_yoy(current, prior, None))
        current, prior, yoy = _reversed_millions_row(flat_text, label)
        if current is not None:
            return (current, prior, normalize_yoy(current, prior, yoy))
        current, prior = _reversed_millions_row_no_pct(flat_text, label)
        if current is not None:
            return (current, prior, normalize_yoy(current, prior, None))
        current, prior, yoy = _five_quarter_row(flat_text, label)
        if current is not None:
            return (current, prior, normalize_yoy(current, prior, yoy))
    return (None, None, None)


def _extract_pct_metric(flat_text: str, labels: list[str]) -> Optional[float]:
    for label in labels:
        current, _ = _percent_row(flat_text, label)
        if current is not None:
            return current
        match = _search(rf"{re.escape(label)}(?:\s+(?:was|were|of))?\s+([0-9]+(?:\.[0-9]+)?)\s*%", flat_text)
        if match:
            return _pct_value(match.group(1))
    return None


def _extract_scaled_label_value(
    text: str,
    labels: list[str],
    *,
    scale: float,
) -> Optional[float]:
    for label in labels:
        match = _search(
            rf"{_table_label_pattern(label)}"
            r"\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)",
            text,
        )
        if not match:
            continue
        parsed = _parse_number(match.group(1))
        if parsed is not None:
            return float(parsed) * scale
    return None


def _extract_ending_equity_bn(materials: list[dict[str, Any]]) -> Optional[float]:
    labels = [
        "Total stockholders' equity",
        "Total stockholders equity",
        "Total shareholders' equity",
        "Total shareholders equity",
        "Total equity",
        "Stockholders' equity",
        "Shareholders' equity",
        "Equity attributable to shareholders of the parent",
        "Equity attributable to owners of the company",
        "Equity attributable to owners of parent",
    ]
    for material in _ordered_narrative_materials(materials):
        raw_text = str(material.get("raw_text") or "")
        flat_text = str(material.get("flat_text") or raw_text)
        if not flat_text:
            continue
        lowered = flat_text.lower()
        if not any(
            marker in lowered
            for marker in (
                "balance sheets",
                "balance sheet",
                "financial position",
                "selected items from balance sheets",
            )
        ):
            continue
        for text in (raw_text, flat_text):
            if not text:
                continue
            if _search(r"in\s+(?:nt\$|us\$)?\s*billions", text) or _search(r"\(in\s+billions\)", text):
                equity_bn = _extract_scaled_label_value(text, labels, scale=1.0)
                if equity_bn is not None:
                    return equity_bn
            if _search(r"in\s+millions", text) or _search(r"\(millions\)", text):
                equity_bn = _extract_scaled_label_value(text, labels, scale=0.001)
                if equity_bn is not None:
                    return equity_bn
            if _search(r"in\s+thousands", text):
                equity_bn = _extract_scaled_label_value(text, labels, scale=0.000001)
                if equity_bn is not None:
                    return equity_bn
    return None


def _is_annual_material(material: Optional[dict[str, Any]]) -> bool:
    if material is None:
        return False
    label = str(material.get("label") or "").lower()
    return "10-k" in label or "20-f" in label or "annual report" in label


COMPANY_SEGMENT_PROFILES: dict[str, list[dict[str, Any]]] = {
    "nvidia": [
        {"name": "Data Center", "labels": ["Data Center", "Datacenter"]},
        {"name": "Gaming", "labels": ["Gaming"]},
        {"name": "Professional Visualization", "labels": ["Professional Visualization"]},
        {"name": "Automotive", "labels": ["Automotive"]},
        {"name": "OEM and Other", "labels": ["OEM and Other", "OEM and IP", "OEM and Other and IP"]},
    ],
    "microsoft": [
        {"name": "Productivity and Business Processes", "labels": ["Productivity and Business Processes"]},
        {"name": "Intelligent Cloud", "labels": ["Intelligent Cloud"]},
        {"name": "More Personal Computing", "labels": ["More Personal Computing"]},
    ],
    "apple": [
        {"name": "iPhone", "labels": ["iPhone"]},
        {"name": "iPhone and related products and services", "labels": ["iPhone and related products and services", "iPhone and related products and services (f)"]},
        {"name": "Mac", "labels": ["Mac", "Total Mac net sales"]},
        {"name": "iPad", "labels": ["iPad"]},
        {"name": "Wearables, Home and Accessories", "labels": ["Wearables, Home and Accessories", "Wearables, Home & Accessories"]},
        {"name": "Services", "labels": ["Services"]},
        {"name": "Products", "labels": ["Products"]},
        {"name": "iPod", "labels": ["iPod"]},
        {"name": "Other music related products and services", "labels": ["Other music related products and services", "Other music related products and services (e)"]},
        {"name": "Peripherals and other hardware", "labels": ["Peripherals and other hardware", "Peripherals and other hardware (g)"]},
        {"name": "Software, service, and other sales", "labels": ["Software, service, and other sales", "Software, service and other sales", "Software, service, and other sales (h)"]},
    ],
    "alphabet": [
        {"name": "Google properties", "labels": ["Google properties", "Google properties revenues"]},
        {"name": "Google Search & other", "labels": ["Google Search & other", "Google Search and other"]},
        {"name": "YouTube ads", "labels": ["YouTube ads", "YouTube advertising"]},
        {"name": "Google Network", "labels": ["Google Network"]},
        {"name": "Google other", "labels": ["Google other", "Google other revenues"]},
        {
            "name": "Google subscriptions, platforms, and devices",
            "labels": [
                "Google subscriptions, platforms, and devices",
                "Google subscriptions, platforms and devices",
                "Subscriptions, platforms, and devices",
                "Subscriptions, platforms and devices",
            ],
        },
        {"name": "Google Cloud", "labels": ["Google Cloud"]},
        {"name": "Other Bets", "labels": ["Other Bets"]},
    ],
    "amazon": [
        {"name": "North America", "labels": ["North America"]},
        {"name": "International", "labels": ["International"]},
        {"name": "AWS", "labels": ["AWS", "Amazon Web Services"]},
    ],
    "berkshire": [
        {"name": "Insurance underwriting", "labels": ["Insurance underwriting", "Insurance-underwriting"]},
        {"name": "Insurance investment income", "labels": ["Insurance investment income", "Insurance-investment income"]},
        {"name": "BNSF", "labels": ["BNSF", "BNSF Railway"]},
        {"name": "Berkshire Hathaway Energy", "labels": ["Berkshire Hathaway Energy", "Berkshire Hathaway Energy Company"]},
        {"name": "Manufacturing, service and retailing", "labels": ["Manufacturing, service and retailing", "Manufacturing", "Service", "Retailing"]},
    ],
    "meta": [
        {"name": "Family of Apps", "labels": ["Family of Apps"]},
        {"name": "Reality Labs", "labels": ["Reality Labs"]},
    ],
    "tsla": [
        {"name": "Automotive", "labels": ["Automotive revenues", "Automotive"]},
        {"name": "Energy Generation and Storage", "labels": ["Energy generation and storage revenues", "Energy Generation and Storage"]},
        {"name": "Services and Other", "labels": ["Services and other revenues", "Services and Other"]},
    ],
    "avgo": [
        {"name": "Semiconductor Solutions", "labels": ["Semiconductor solutions", "Semiconductor Solutions"]},
        {"name": "Infrastructure Software", "labels": ["Infrastructure software", "Infrastructure Software"]},
    ],
    "walmart": [
        {"name": "Walmart U.S.", "labels": ["Walmart U.S."]},
        {"name": "Walmart International", "labels": ["Walmart International"]},
        {"name": "Sam's Club U.S.", "labels": ["Sam's Club U.S.", "Sam's Club"]},
    ],
    "oracle": [
        {"name": "Cloud", "labels": ["Cloud"]},
        {"name": "Software", "labels": ["Software"]},
        {"name": "Hardware", "labels": ["Hardware"]},
        {"name": "Services", "labels": ["Services"]},
    ],
    "costco": [
        {"name": "Net sales", "labels": ["Net sales"]},
        {"name": "Membership fees", "labels": ["Membership fees"]},
    ],
    "jnj": [
        {"name": "Consumer Health", "labels": ["Consumer Health"]},
        {"name": "Pharmaceutical", "labels": ["Pharmaceutical", "Innovative Medicine"]},
        {"name": "MedTech", "labels": ["MedTech", "Medical Devices"]},
    ],
    "jpm": [
        {"name": "Consumer & Community Banking", "labels": ["Consumer & Community Banking"]},
        {"name": "Commercial & Investment Bank", "labels": ["Commercial & Investment Bank"]},
        {"name": "Asset & Wealth Management", "labels": ["Asset & Wealth Management"]},
        {"name": "Commercial Banking", "labels": ["Commercial Banking"]},
    ],
    "micron": [
        {"name": "Compute and Networking Business Unit", "labels": ["Compute and Networking Business Unit", "CNBU"]},
        {"name": "Mobile Business Unit", "labels": ["Mobile Business Unit", "MBU"]},
        {"name": "Storage Business Unit", "labels": ["Storage Business Unit", "SBU"]},
        {"name": "Embedded Business Unit", "labels": ["Embedded Business Unit", "EBU"]},
        {"name": "Cloud Memory Business Unit", "labels": ["Cloud Memory Business Unit", "CMBU"]},
        {"name": "Core Data Center Business Unit", "labels": ["Core Data Center Business Unit", "CDBU"]},
        {"name": "Mobile and Client Business Unit", "labels": ["Mobile and Client Business Unit", "MCBU"]},
        {"name": "Automotive and Embedded Business Unit", "labels": ["Automotive and Embedded Business Unit", "AEBU"]},
    ],
    "asml": [
        {"name": "Net system sales", "labels": ["Net system sales"]},
        {"name": "Installed Base Management", "labels": ["Installed Base Management"]},
    ],
    "tsmc": [
        {"name": "HPC", "labels": ["HPC"]},
        {"name": "Smartphone", "labels": ["Smartphone"]},
        {"name": "Internet of Things", "labels": ["Internet of Things", "IoT"]},
        {"name": "Automotive", "labels": ["Automotive"]},
        {"name": "DCE", "labels": ["DCE"]},
        {"name": "Others", "labels": ["Others"]},
    ],
    "lly": [
        {"name": "Mounjaro", "labels": ["Mounjaro"]},
        {"name": "Zepbound", "labels": ["Zepbound"]},
        {"name": "Verzenio", "labels": ["Verzenio"]},
        {"name": "Jardiance", "labels": ["Jardiance"]},
    ],
    "visa": [
        {"name": "Service revenue", "labels": ["Service revenue", "service revenue"]},
        {"name": "Data processing revenue", "labels": ["Data processing revenue", "data processing revenue"]},
        {"name": "International transaction revenue", "labels": ["International transaction revenue", "international transaction revenue"]},
        {"name": "Other revenue", "labels": ["Other revenue", "other revenue"]},
    ],
}


def _segment_label_variants(label: str) -> list[str]:
    normalized = " ".join(str(label or "").split()).strip()
    if not normalized:
        return []
    variants = [normalized]
    replacements = [
        (" & ", " and "),
        (" and ", " & "),
        ("-", " "),
        (" ", "-"),
        (", and ", " and "),
        (", ", " "),
    ]
    for old, new in replacements:
        if old in normalized:
            variants.append(" ".join(normalized.replace(old, new).split()).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for item in variants:
        key = item.casefold()
        if not item or key in seen:
            continue
        deduped.append(item)
        seen.add(key)
    return deduped


def _segment_profiles_for_company(company_id: str) -> list[dict[str, Any]]:
    predefined = list(COMPANY_SEGMENT_PROFILES.get(company_id, []))
    try:
        company = get_company(company_id)
    except Exception:
        company = {}

    alias_map = {
        str(alias).strip(): str(canonical).strip()
        for alias, canonical in dict(company.get("segment_aliases") or {}).items()
        if str(alias).strip() and str(canonical).strip()
    }
    canonical_aliases: dict[str, list[str]] = {}
    for alias, canonical in alias_map.items():
        canonical_aliases.setdefault(canonical, []).append(alias)

    merged: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    def add_profile(name: str, labels: list[str]) -> None:
        normalized_name = str(name or "").strip()
        if not normalized_name or normalized_name.casefold() in seen_names:
            return
        expanded_labels: list[str] = []
        seen_labels: set[str] = set()
        for label in labels:
            for variant in _segment_label_variants(label):
                key = variant.casefold()
                if key in seen_labels:
                    continue
                expanded_labels.append(variant)
                seen_labels.add(key)
        if not expanded_labels:
            return
        merged.append({"name": normalized_name, "labels": expanded_labels})
        seen_names.add(normalized_name.casefold())

    for profile in predefined:
        profile_name = str(profile.get("name") or "").strip()
        profile_labels = [str(item).strip() for item in list(profile.get("labels") or []) if str(item).strip()]
        add_profile(profile_name, profile_labels + canonical_aliases.get(profile_name, []))

    for segment_name in list(company.get("segment_order") or []):
        canonical_name = str(segment_name or "").strip()
        add_profile(canonical_name, [canonical_name] + canonical_aliases.get(canonical_name, []))

    return merged

COMPANY_SEGMENT_RATIO_BOUNDS: dict[str, tuple[float, float]] = {
    "visa": (0.55, 1.55),
}

GEOGRAPHY_PROFILES: list[list[dict[str, Any]]] = [
    [
        {"name": "Americas", "labels": ["Americas", "Americas net sales", "Americas revenue"]},
        {"name": "Europe", "labels": ["Europe", "Europe net sales", "Europe revenue"]},
        {"name": "Japan", "labels": ["Japan", "Japan net sales", "Japan revenue"]},
        {"name": "Asia-Pacific", "labels": ["Asia-Pacific", "Asia Pacific", "Asia-Pacific net sales", "Asia Pacific net sales", "Asia-Pacific revenue"]},
        {"name": "Retail", "labels": ["Retail", "Retail net sales", "Retail revenue"]},
    ],
    [
        {"name": "United States and Canada", "labels": ["United States and Canada", "United States & Canada", "US & Canada"]},
        {"name": "Europe", "labels": ["Europe", "Europe net sales", "Europe revenue"]},
        {"name": "Asia-Pacific", "labels": ["Asia-Pacific", "Asia Pacific", "Asia-Pacific net sales", "Asia Pacific net sales"]},
        {"name": "Rest of World", "labels": ["Rest of World"]},
    ],
    [
        {"name": "Americas", "labels": ["Americas", "Americas net sales", "Americas revenue"]},
        {"name": "Europe", "labels": ["Europe", "Europe net sales", "Europe revenue"]},
        {"name": "Greater China", "labels": ["Greater China", "Greater China net sales", "Greater China revenue"]},
        {"name": "Japan", "labels": ["Japan", "Japan net sales", "Japan revenue"]},
        {"name": "Rest of Asia Pacific", "labels": ["Rest of Asia Pacific", "Rest of Asia-Pacific", "Rest of Asia Pacific net sales", "Rest of Asia-Pacific net sales"]},
    ],
    [
        {"name": "United States", "labels": ["United States", "U.S."]},
        {"name": "EMEA", "labels": ["EMEA", "Europe, the Middle East and Africa", "Europe, Middle East and Africa"]},
        {"name": "Asia Pacific", "labels": ["APAC", "Asia Pacific", "Asia-Pacific"]},
        {"name": "Americas Excluding U.S.", "labels": ["Other Americas", "Americas Excluding United States", "Americas Excluding U.S."]},
    ],
    [
        {"name": "United States", "labels": ["United States", "U.S."]},
        {"name": "International", "labels": ["International", "Other countries"]},
    ],
    [
        {"name": "United States", "labels": ["United States", "U.S.", "United States "]},
        {"name": "Canada", "labels": ["Canada"]},
        {"name": "Other International", "labels": ["Other International"]},
    ],
    [
        {"name": "North America", "labels": ["North America"]},
        {"name": "International", "labels": ["International"]},
    ],
    [
        {"name": "United States", "labels": ["United States", "U.S."]},
        {"name": "EMEA", "labels": ["EMEA", "Europe, the Middle East and Africa", "Europe, Middle East and Africa"]},
        {"name": "APAC", "labels": ["APAC", "Asia Pacific", "Asia-Pacific"]},
    ],
    [
        {"name": "Americas", "labels": ["Americas"]},
        {"name": "EMEA", "labels": ["EMEA", "Europe, the Middle East and Africa", "Europe, Middle East and Africa"]},
        {"name": "APJ", "labels": ["APJ", "Asia Pacific and Japan"]},
    ],
    [
        {"name": "Americas", "labels": ["Americas"]},
        {"name": "Asia Pacific", "labels": ["Asia Pacific", "Asia-Pacific", "APAC"]},
        {"name": "Europe, the Middle East and Africa", "labels": ["Europe, the Middle East and Africa", "Europe, Middle East and Africa", "EMEA"]},
    ],
]

GENERIC_GEOGRAPHY_LABELS: list[dict[str, Any]] = [
    {"name": "United States", "labels": ["United States", "U.S.", "U.S", "US"]},
    {"name": "Canada", "labels": ["Canada"]},
    {"name": "United States and Canada", "labels": ["United States and Canada", "United States & Canada", "US & Canada"]},
    {"name": "North America", "labels": ["North America"]},
    {"name": "Americas", "labels": ["Americas"]},
    {"name": "Americas Excluding U.S.", "labels": ["Other Americas", "Americas Excluding United States", "Americas Excluding U.S."]},
    {"name": "Latin America / Caribbean", "labels": ["Latin America/Caribbean", "Latin America / Caribbean"]},
    {"name": "Europe", "labels": ["Europe"]},
    {
        "name": "Europe, the Middle East and Africa",
        "labels": [
            "Europe, the Middle East and Africa",
            "Europe, Middle East and Africa",
            "Europe/Middle East/Africa",
            "Europe / Middle East / Africa",
        ],
    },
    {"name": "EMEA", "labels": ["EMEA"]},
    {"name": "Asia Pacific", "labels": ["Asia Pacific", "Asia-Pacific", "APAC"]},
    {"name": "APJ", "labels": ["APJ", "Asia Pacific and Japan"]},
    {"name": "Japan", "labels": ["Japan"]},
    {"name": "Greater China", "labels": ["Greater China"]},
    {"name": "China", "labels": ["China"]},
    {"name": "Taiwan", "labels": ["Taiwan"]},
    {"name": "South Korea", "labels": ["South Korea", "Korea"]},
    {"name": "Rest of Asia Pacific", "labels": ["Rest of Asia Pacific", "Rest of Asia-Pacific"]},
    {"name": "Rest of World", "labels": ["Rest of World"]},
    {"name": "International", "labels": ["International"]},
    {"name": "Other International", "labels": ["Other International"]},
    {"name": "Other countries", "labels": ["Other countries"]},
]


def _geography(name: str, value_bn: Optional[float], yoy_pct: Optional[float]) -> Optional[dict[str, Any]]:
    if value_bn is None:
        return None
    return {
        "name": name,
        "value_bn": round(float(value_bn), 3),
        "yoy_pct": None if yoy_pct is None else round(float(yoy_pct), 1),
    }


def _annual_geography(name: str, current_bn: Optional[float], prior_bn: Optional[float]) -> Optional[dict[str, Any]]:
    item = _geography(name, current_bn, _pct_change(current_bn, prior_bn))
    if item is None:
        return None
    item["scope"] = "annual_filing"
    return item


def _extract_generic_geographies_from_html_tables(
    material: dict[str, Any],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for rows in _material_html_tables(material):
        extracted: list[dict[str, Any]] = []
        for region in GENERIC_GEOGRAPHY_LABELS:
            current_bn, prior_bn, yoy_pct = _extract_html_table_metric_from_rows(rows, list(region["labels"]))
            if current_bn is None:
                continue
            item = _geography(str(region["name"]), current_bn, yoy_pct if yoy_pct is not None else _pct_change(current_bn, prior_bn))
            if item is not None:
                extracted.append(item)
        if len(extracted) < 2:
            continue
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in extracted:
            name = str(item.get("name") or "").casefold()
            if not name or name in seen:
                continue
            deduped.append(item)
            seen.add(name)
        if len(deduped) < 2:
            continue
        ranked = sorted(deduped, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
        best = _prefer_richer_geographies(best, ranked, revenue_bn)
    if best and _is_annual_material(material):
        return [{**item, "scope": "annual_filing"} for item in best]
    return best


def _extract_generic_geographies_from_text_tables(
    material: dict[str, Any],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    raw_text = str(material.get("raw_text") or "")
    if not raw_text or not _material_has_geography_context(raw_text):
        return []
    extracted: list[dict[str, Any]] = []
    for region in GENERIC_GEOGRAPHY_LABELS:
        current_bn, prior_bn, yoy_pct = _extract_labeled_text_table_metric(raw_text, list(region["labels"]))
        if current_bn is None:
            continue
        item = _geography(
            str(region["name"]),
            current_bn,
            yoy_pct if yoy_pct is not None else _pct_change(current_bn, prior_bn),
        )
        if item is not None:
            extracted.append(item)
    if len(extracted) < 2:
        return []
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in extracted:
        name = str(item.get("name") or "").casefold()
        if not name or name in seen:
            continue
        deduped.append(item)
        seen.add(name)
    ranked = sorted(deduped, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
    if not _segments_reasonable_for_revenue(ranked, revenue_bn) and revenue_bn not in (None, 0):
        return []
    if ranked and _is_annual_material(material):
        return [{**item, "scope": "annual_filing"} for item in ranked]
    return ranked


def _calendar_quarter_from_fallback(fallback: dict[str, Any]) -> Optional[str]:
    explicit = str(fallback.get("calendar_quarter") or fallback.get("fiscal_label") or "")
    if re.fullmatch(r"\d{4}Q[1-4]", explicit):
        return explicit

    counts: dict[str, int] = {}
    for token in list(fallback.get("coverage_months") or []):
        match = re.fullmatch(r"(\d{4})-(\d{2})", str(token))
        if not match:
            continue
        year = int(match.group(1))
        month = int(match.group(2))
        quarter = (month - 1) // 3 + 1
        label = f"{year}Q{quarter}"
        counts[label] = counts.get(label, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _previous_calendar_quarter(calendar_quarter: str) -> Optional[str]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", calendar_quarter)
    if not match:
        return None
    year = int(match.group(1))
    quarter = int(match.group(2))
    if quarter == 1:
        return f"{year - 1}Q4"
    return f"{year}Q{quarter - 1}"


def _calendar_quarter_sort_key(calendar_quarter: Optional[str]) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(calendar_quarter or ""))
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))


def _fallback_calendar_quarter(fallback: dict[str, Any]) -> Optional[str]:
    explicit = str(fallback.get("calendar_quarter") or "")
    if re.fullmatch(r"\d{4}Q[1-4]", explicit):
        return explicit
    return _calendar_quarter_from_fallback(fallback)


def _quarter_is_before(fallback: dict[str, Any], cutoff: str) -> bool:
    calendar_quarter = _fallback_calendar_quarter(fallback)
    if not calendar_quarter:
        return False
    return _calendar_quarter_sort_key(calendar_quarter) < _calendar_quarter_sort_key(cutoff)


def _recent_calendar_quarters(calendar_quarter: str, limit: int = 6) -> list[str]:
    labels: list[str] = []
    current = calendar_quarter
    while current and len(labels) < limit:
        labels.append(current)
        current = _previous_calendar_quarter(current)
    return labels


def _estimate_calendar_period_end(calendar_quarter: str) -> str:
    match = re.fullmatch(r"(\d{4})Q([1-4])", calendar_quarter)
    if not match:
        return ""
    year = int(match.group(1))
    quarter = int(match.group(2))
    end_month = quarter * 3
    end_day = calendar.monthrange(year, end_month)[1]
    return f"{year}-{end_month:02d}-{end_day:02d}"


def _fallback_coverage_months(calendar_quarter: Optional[str], period_end: Optional[str]) -> list[str]:
    if period_end and len(str(period_end)) >= 7:
        try:
            year, month = (int(part) for part in str(period_end)[:7].split("-"))
            months: list[str] = []
            for delta in (-2, -1, 0):
                index = year * 12 + (month - 1) + delta
                shifted_year = index // 12
                shifted_month = index % 12 + 1
                months.append(f"{shifted_year:04d}-{shifted_month:02d}")
            if calendar_quarter and _calendar_quarter_from_fallback({"coverage_months": months}) == calendar_quarter:
                return months
        except Exception:
            pass
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(calendar_quarter or ""))
    if not match:
        return []
    year = int(match.group(1))
    quarter = int(match.group(2))
    start_month = (quarter - 1) * 3 + 1
    return [f"{year:04d}-{start_month + offset:02d}" for offset in range(3)]


def _ensure_parser_context(fallback: dict[str, Any]) -> dict[str, Any]:
    context = dict(fallback or {})
    calendar_quarter = _fallback_calendar_quarter(context)
    if not calendar_quarter:
        return context
    context.setdefault("calendar_quarter", calendar_quarter)
    context.setdefault("fiscal_label", calendar_quarter)
    period_end = str(context.get("period_end") or "") or _estimate_calendar_period_end(calendar_quarter)
    if period_end:
        context["period_end"] = period_end
    if not list(context.get("coverage_months") or []):
        context["coverage_months"] = _fallback_coverage_months(calendar_quarter, period_end)
    return context


def _report_year_from_fallback(fallback: dict[str, Any]) -> Optional[int]:
    calendar_quarter = _fallback_calendar_quarter(fallback)
    if calendar_quarter and re.fullmatch(r"\d{4}Q[1-4]", calendar_quarter):
        return int(calendar_quarter[:4])
    fiscal_label = str(fallback.get("fiscal_label") or "")
    match = re.search(r"\b(20\d{2})\b", fiscal_label)
    if match:
        return int(match.group(1))
    return None


def _text_has_implausible_future_year(text: Optional[str], report_year: Optional[int], *, max_gap: int = 3) -> bool:
    if report_year is None or not text:
        return False
    current_year = time.localtime().tm_year
    if report_year >= current_year - 1:
        return False
    years = [int(token) for token in re.findall(r"\b(20\d{2})\b", str(text))]
    return any(year > report_year + max_gap for year in years)


def _sanitize_temporal_narrative_facts(facts: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    report_year = _report_year_from_fallback(fallback)
    if report_year is None:
        return facts

    sanitized = dict(facts)

    def keep_text(text: Optional[str]) -> bool:
        return not _text_has_implausible_future_year(text, report_year)

    def sanitize_theme_list(items: Any) -> list[dict[str, Any]]:
        cleaned: list[dict[str, Any]] = []
        for item in list(items or []):
            if not isinstance(item, dict):
                continue
            if not keep_text(str(item.get("label") or "")) or not keep_text(str(item.get("note") or "")):
                continue
            cleaned.append(item)
        return cleaned

    def sanitize_takeaway_list(items: Any) -> list[str]:
        cleaned: list[str] = []
        for item in list(items or []):
            text = str(item or "").strip()
            if not text or not keep_text(text):
                continue
            cleaned.append(text)
        return cleaned

    def sanitize_quote_list(items: Any) -> list[dict[str, Any]]:
        cleaned: list[dict[str, Any]] = []
        for item in list(items or []):
            if not isinstance(item, dict):
                continue
            if not keep_text(str(item.get("quote") or "")) or not keep_text(str(item.get("analysis") or "")):
                continue
            cleaned.append(item)
        return cleaned

    def sanitize_text_card_list(items: Any, *, title_key: str = "title", text_key: str = "text") -> list[dict[str, Any]]:
        cleaned: list[dict[str, Any]] = []
        for item in list(items or []):
            if not isinstance(item, dict):
                continue
            if not keep_text(str(item.get(title_key) or "")) or not keep_text(str(item.get(text_key) or "")):
                continue
            cleaned.append(item)
        return cleaned

    for key in (
        "management_theme_items",
        "qna_theme_items",
        "risk_items",
        "catalyst_items",
        "management_themes",
        "qna_themes",
        "risks",
        "catalysts",
    ):
        if key in sanitized:
            sanitized[key] = sanitize_theme_list(sanitized.get(key))
    if "quotes" in sanitized:
        sanitized["quotes"] = sanitize_quote_list(sanitized.get("quotes"))
    if "call_quote_cards" in sanitized:
        sanitized["call_quote_cards"] = sanitize_quote_list(sanitized.get("call_quote_cards"))
    if "evidence_cards" in sanitized:
        sanitized["evidence_cards"] = sanitize_text_card_list(sanitized.get("evidence_cards"))
    if "takeaways" in sanitized:
        sanitized["takeaways"] = sanitize_takeaway_list(sanitized.get("takeaways"))
    if not keep_text(str(sanitized.get("driver") or "")):
        sanitized["driver"] = None
    if not keep_text(str(sanitized.get("headline") or "")):
        sanitized["headline"] = None

    guidance = dict(sanitized.get("guidance") or {})
    if guidance and not keep_text(str(guidance.get("commentary") or "")):
        guidance.pop("commentary", None)
    sanitized["guidance"] = _clean_mapping(guidance) if guidance else {}
    return sanitized


def _load_nearby_annual_materials(
    company: dict[str, Any],
    fallback: dict[str, Any],
    existing_materials: list[dict[str, Any]],
    *,
    lookback_quarters: int = 6,
) -> list[dict[str, Any]]:
    calendar_quarter = _calendar_quarter_from_fallback(fallback)
    if not calendar_quarter:
        return []

    source_config = dict(company.get("official_source") or {})
    annual_forms = [form for form in list(source_config.get("filing_forms") or []) if form in {"10-K", "20-F"}]
    if not annual_forms:
        annual_forms = ["10-K", "20-F"]

    annual_company = copy.deepcopy(company)
    annual_source_config = dict(source_config)
    annual_source_config["release_forms"] = []
    annual_source_config["filing_forms"] = annual_forms
    annual_source_config.pop("filing_document_excludes", None)
    annual_company["official_source"] = annual_source_config

    seen_paths = {str(item.get("text_path") or "") for item in existing_materials}
    for quarter in _recent_calendar_quarters(calendar_quarter, limit=lookback_quarters):
        period_end = _estimate_calendar_period_end(quarter)
        if not period_end:
            continue
        sources = resolve_official_sources(
            annual_company,
            quarter,
            period_end,
            [],
            refresh=False,
            prefer_sec_only=True,
        )
        annual_sources = [
            source
            for source in sources
            if source.get("kind") == "sec_filing"
            and any(token in str(source.get("label") or "").lower() for token in ("10-k", "20-f", "annual report"))
        ]
        if not annual_sources:
            continue
        loaded = _load_materials(hydrate_source_materials(str(company["id"]), quarter, annual_sources, refresh=False))
        loaded = [item for item in loaded if str(item.get("text_path") or "") not in seen_paths]
        if loaded:
            return loaded
    return []


def _microsoft_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Revenue, classified by the major geographic areas in which our customers were located, was as follows:\s*"
        r"\(In millions\)\s*Three Months Ended\s*.*?United States\s*\(a\)\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s*\$?\s*[0-9,]+\s*"
        r"Other countries\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s*[0-9,]+",
        sec_flat,
    )
    if not match:
        return []
    return _geography_list(
        _geography("United States", _bn_from_millions(match.group(1)), _pct_change(_bn_from_millions(match.group(1)), _bn_from_millions(match.group(2)))),
        _geography("Other countries", _bn_from_millions(match.group(3)), _pct_change(_bn_from_millions(match.group(3)), _bn_from_millions(match.group(4)))),
    )


def _nvidia_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Geographic Revenue based upon Customer Headquarters Location \(1\):\s*\(In millions\)\s*"
        r"United States\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s*"
        r"Taiwan \(2\)\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s*"
        r"China \(including Hong Kong\)\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s*"
        r"Other\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+",
        sec_flat,
    )
    if not match:
        return []
    return _geography_list(
        _annual_geography("United States", _bn_from_millions(match.group(1)), _bn_from_millions(match.group(2))),
        _annual_geography("Taiwan", _bn_from_millions(match.group(3)), _bn_from_millions(match.group(4))),
        _annual_geography("China (including Hong Kong)", _bn_from_millions(match.group(5)), _bn_from_millions(match.group(6))),
        _annual_geography("Other", _bn_from_millions(match.group(7)), _bn_from_millions(match.group(8))),
    )


def _avgo_geographies(sec_flat: str) -> list[dict[str, Any]]:
    quarterly_matches = re.findall(
        r"Fiscal Quarter Ended [A-Za-z]+\s+\d{1,2},\s+\d{4}\s+Americas\s+Asia Pacific\s+Europe, the Middle East and Africa\s+Total\s+"
        r"\(In millions\)\s+Products\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+"
        r"Subscriptions and services(?:\s+\(a\))?\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+"
        r"Total\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*[0-9,]+",
        sec_flat,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if quarterly_matches:
        current = quarterly_matches[0]
        prior = quarterly_matches[1] if len(quarterly_matches) >= 2 else None
        return _geography_list(
            _geography(
                "Americas",
                _bn_from_millions(current[0]),
                _pct_change(_bn_from_millions(current[0]), _bn_from_millions(prior[0])) if prior else None,
            ),
            _geography(
                "Asia Pacific",
                _bn_from_millions(current[1]),
                _pct_change(_bn_from_millions(current[1]), _bn_from_millions(prior[1])) if prior else None,
            ),
            _geography(
                "Europe, the Middle East and Africa",
                _bn_from_millions(current[2]),
                _pct_change(_bn_from_millions(current[2]), _bn_from_millions(prior[2])) if prior else None,
            ),
        )

    annual_matches = re.findall(
        r"(?:The following table presents revenue disaggregated by type of revenue and by region:\s*)?"
        r"Fiscal Year(?:\s+Ended\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}|\s+\d{4})\s+Americas\s+Asia Pacific\s+Europe, the Middle East and Africa\s+Total\s+"
        r"\(In millions\)\s+Products\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+"
        r"Subscriptions and services(?:\s+\(a\))?\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+"
        r"Total\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*[0-9,]+",
        sec_flat,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if annual_matches:
        current = annual_matches[0]
        prior = annual_matches[1] if len(annual_matches) >= 2 else None
        return _geography_list(
            _annual_geography("Americas", _bn_from_millions(current[0]), _bn_from_millions(prior[0]) if prior else None),
            _annual_geography("Asia Pacific", _bn_from_millions(current[1]), _bn_from_millions(prior[1]) if prior else None),
            _annual_geography("Europe, the Middle East and Africa", _bn_from_millions(current[2]), _bn_from_millions(prior[2]) if prior else None),
        )
    return []


def _jpm_geographies(sec_flat: str) -> list[dict[str, Any]]:
    anchor_candidates = [
        "Total net revenue (a) Europe/Middle East/Africa",
        "Total net revenue Europe/Middle East/Africa",
        "Europe/Middle East/Africa",
    ]
    anchor_index = next((sec_flat.find(token) for token in anchor_candidates if sec_flat.find(token) >= 0), -1)
    search_text = sec_flat[anchor_index : anchor_index + 6000] if anchor_index >= 0 else sec_flat

    section = ""
    section_match = _search(
        r"Total net revenue\s*\(a\)\s+(.*?)Total net revenue\s+\$?",
        search_text,
    )
    if section_match:
        section = str(section_match.group(1) or "")
    if section:
        def _quarterly_row(label: str) -> tuple[Optional[float], Optional[float]]:
            row_match = _search(
                rf"{re.escape(label)}\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\(?-?[0-9]+\)?\s*%?\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)",
                section,
            )
            if not row_match:
                return (None, None)
            return (_bn_from_millions(row_match.group(3)), _bn_from_millions(row_match.group(4)))

        north_america = _quarterly_row("North America")
        emea = _quarterly_row("Europe/Middle East/Africa")
        apac = _quarterly_row("Asia-Pacific")
        latam = _quarterly_row("Latin America/Caribbean")
        if all(current is not None and prior is not None for current, prior in [north_america, emea, apac, latam]):
            return _geography_list(
                _annual_geography("North America", north_america[0], north_america[1]),
                _annual_geography("Europe / Middle East / Africa", emea[0], emea[1]),
                _annual_geography("Asia-Pacific", apac[0], apac[1]),
                _annual_geography("Latin America / Caribbean", latam[0], latam[1]),
            )

    def _annual_row(pattern: str) -> tuple[Optional[float], Optional[float]]:
        matches = list(re.finditer(pattern, search_text, flags=re.IGNORECASE))
        if len(matches) < 2:
            return (None, None)
        current = _bn_from_millions(matches[0].group(1))
        prior = _bn_from_millions(matches[1].group(1))
        return (current, prior)

    north_america = _annual_row(r"North America(?:\s*\(a\))?\s+\$?\s*([0-9,]+)")
    emea = _annual_row(r"Europe/Middle East/Africa\s+\$?\s*([0-9,]+)")
    apac = _annual_row(r"Asia-Pacific\s+\$?\s*([0-9,]+)")
    latam = _annual_row(r"Latin America/Caribbean\s+\$?\s*([0-9,]+)")
    if not all(current is not None and prior is not None for current, prior in [north_america, emea, apac, latam]):
        return []
    return _geography_list(
        _annual_geography("North America", north_america[0], north_america[1]),
        _annual_geography("Europe / Middle East / Africa", emea[0], emea[1]),
        _annual_geography("Asia-Pacific", apac[0], apac[1]),
        _annual_geography("Latin America / Caribbean", latam[0], latam[1]),
    )


def _micron_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Geographic Information\s+Revenue based on the geographic location of our customers' headquarters was as follows:\s+"
        r"For the year ended\s+\d{4}\s+\d{4}\s+\d{4}\s+"
        r"U\.S\.\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s+"
        r"Taiwan\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Mainland China \(excluding Hong Kong\)\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Other Asia Pacific\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Hong Kong\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Japan\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Europe\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Other\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+",
        sec_flat,
    )
    if not match:
        return []
    return _geography_list(
        _annual_geography("U.S.", _bn_from_millions(match.group(1)), _bn_from_millions(match.group(2))),
        _annual_geography("Taiwan", _bn_from_millions(match.group(3)), _bn_from_millions(match.group(4))),
        _annual_geography("Mainland China (excluding Hong Kong)", _bn_from_millions(match.group(5)), _bn_from_millions(match.group(6))),
        _annual_geography("Other Asia Pacific", _bn_from_millions(match.group(7)), _bn_from_millions(match.group(8))),
        _annual_geography("Hong Kong", _bn_from_millions(match.group(9)), _bn_from_millions(match.group(10))),
        _annual_geography("Japan", _bn_from_millions(match.group(11)), _bn_from_millions(match.group(12))),
        _annual_geography("Europe", _bn_from_millions(match.group(13)), _bn_from_millions(match.group(14))),
        _annual_geography("Other", _bn_from_millions(match.group(15)), _bn_from_millions(match.group(16))),
    )


def _xom_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Year ended December 31,\s*\d{4}\s+Revenues and other income\s+Sales and other operating revenue\s+"
        r"([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+[0-9,()]+\s+"
        r"Income from equity affiliates.*?"
        r"Year ended December 31,\s*\d{4}\s+Revenues and other income\s+Sales and other operating revenue\s+"
        r"([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+([0-9,()]+)\s+[0-9,()]+",
        sec_flat,
    )
    if not match:
        return []
    current_us = sum(_parse_number(match.group(index)) or 0.0 for index in (1, 3, 5, 7))
    current_non_us = sum(_parse_number(match.group(index)) or 0.0 for index in (2, 4, 6, 8))
    prior_us = sum(_parse_number(match.group(index)) or 0.0 for index in (9, 11, 13, 15))
    prior_non_us = sum(_parse_number(match.group(index)) or 0.0 for index in (10, 12, 14, 16))
    return _geography_list(
        _annual_geography("U.S.", round(current_us / 1000, 3), round(prior_us / 1000, 3)),
        _annual_geography("Non-U.S.", round(current_non_us / 1000, 3), round(prior_non_us / 1000, 3)),
    )


def _meta_geographies(sec_flat: str) -> list[dict[str, Any]]:
    billing_party_pattern = r"(?:marketer or developer|advertiser or developer|advertiser or Platform developer)"
    quarterly_match = _search(
        rf"Revenue by geography is based on the billing address of the {billing_party_pattern}\.\s+"
        r"The following table(?:s)? set(?:s)? forth revenue and property and equipment, net by geographic area \(in millions\):\s+"
        r"Three\s+Months\s+Ended\s+[A-Za-z]+\s+\d{1,2},\s+\s*Six Months Ended\s+[A-Za-z]+\s+\d{1,2},\s+\s*"
        r"\d{4}\s*\d{4}\s*\d{4}\s*\d{4}\s+Revenue:\s+"
        r"United States\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s*\$?\s*[0-9,]+\s+"
        r"Rest of the world\s*\(?1\)?\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s*[0-9,]+\s+"
        r"Total revenue",
        sec_flat,
    )
    if quarterly_match:
        return _geography_list(
            _geography("United States", _bn_from_millions(quarterly_match.group(1)), _pct_change(_bn_from_millions(quarterly_match.group(1)), _bn_from_millions(quarterly_match.group(2)))),
            _geography("Rest of the world", _bn_from_millions(quarterly_match.group(3)), _pct_change(_bn_from_millions(quarterly_match.group(3)), _bn_from_millions(quarterly_match.group(4)))),
        )

    match = _search(
        rf"Revenue by geography is based on the billing address of the {billing_party_pattern}\.\s+"
        r"The following table(?:s)? set(?:s)? forth revenue and property and equipment, net by geographic area \(in millions\):\s+"
        r"Year Ended December 31,\s*\d{4}\s*\d{4}\s*\d{4}\s+Revenue:\s+"
        r"United States\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s+"
        r"Rest of the world\s*\(?1\)?\s*([0-9,]+)\s*([0-9,]+)\s*[0-9,]+\s+"
        r"Total revenue",
        sec_flat,
    )
    if not match:
        return []
    return _geography_list(
        _annual_geography("United States", _bn_from_millions(match.group(1)), _bn_from_millions(match.group(2))),
        _annual_geography("Rest of the world", _bn_from_millions(match.group(3)), _bn_from_millions(match.group(4))),
    )


def _berkshire_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Consolidated sales, service and leasing revenues were \$\s*([0-9.]+)\s*billion in \d{4},\s*\$\s*([0-9.]+)\s*billion in \d{4}.*?"
        r"Sales, service and leasing revenues attributable to the United States were\s*([0-9.]+)\s*%\s*in \d{4},\s*([0-9.]+)\s*%\s*in \d{4}",
        sec_flat,
    )
    if not match:
        return []
    current_total = _bn_from_billions(match.group(1))
    prior_total = _bn_from_billions(match.group(2))
    current_us_pct = _pct_value(match.group(3))
    prior_us_pct = _pct_value(match.group(4))
    current_us = None if current_total is None or current_us_pct is None else float(current_total) * float(current_us_pct) / 100
    prior_us = None if prior_total is None or prior_us_pct is None else float(prior_total) * float(prior_us_pct) / 100
    current_other = None if current_total is None or current_us is None else float(current_total) - float(current_us)
    prior_other = None if prior_total is None or prior_us is None else float(prior_total) - float(prior_us)
    return _geography_list(
        _annual_geography("United States", current_us, prior_us),
        _annual_geography("Other", current_other, prior_other),
    )


def _tsmc_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Years Ended December\s*31\s*\d{4}\s*\d{4}\s*\d{4}\s*NT\$\s*NT\$\s*NT\$\s*Geography\s*"
        r"\(In\s*Millions\)\s*\(In\s*Millions\)\s*\(In\s*Millions\)\s*"
        r"Taiwan\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)\s*"
        r"United States\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*"
        r"China\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*"
        r"Japan\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*"
        r"Europe,\s*the Middle East and Africa\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*"
        r"Others\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)\s*([0-9,]+(?:\.[0-9]+)?)",
        sec_flat,
    )
    if match:
        return _geography_list(
            _annual_geography("United States", _bn_from_millions(match.group(6)), _bn_from_millions(match.group(5))),
            _annual_geography("Taiwan", _bn_from_millions(match.group(3)), _bn_from_millions(match.group(2))),
            _annual_geography("China", _bn_from_millions(match.group(9)), _bn_from_millions(match.group(8))),
            _annual_geography("Japan", _bn_from_millions(match.group(12)), _bn_from_millions(match.group(11))),
            _annual_geography("Europe, the Middle East and Africa", _bn_from_millions(match.group(15)), _bn_from_millions(match.group(14))),
            _annual_geography("Others", _bn_from_millions(match.group(18)), _bn_from_millions(match.group(17))),
        )

    legacy_match = _search(
        r"North America\s*NT\$\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*NT\$\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*NT\$\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*"
        r"Asia\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*"
        r"Europe\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9.]+\s*%",
        sec_flat,
    )
    if not legacy_match:
        return []
    return _geography_list(
        _annual_geography("North America", _bn_from_millions(legacy_match.group(3)), _bn_from_millions(legacy_match.group(2))),
        _annual_geography("Asia", _bn_from_millions(legacy_match.group(6)), _bn_from_millions(legacy_match.group(5))),
        _annual_geography("Europe", _bn_from_millions(legacy_match.group(9)), _bn_from_millions(legacy_match.group(8))),
    )


def _asml_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"Total net sales\s+and long-lived assets\s+by geographic region\s+were as follows:\s*"
        r"Year ended December 31\s*\(€,\s*in millions\)\s*\d{4}\s*\d{4}\s*\d{4}\s*"
        r"Total net sales\s+Long-lived assets\s+Total net sales\s+Long-lived assets\s+Total net sales\s+Long-lived assets\s*"
        r"Japan\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"South Korea\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Singapore\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Taiwan\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"China\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Rest of Asia\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Netherlands\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"EMEA\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"United States\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*([0-9,]+(?:\.[0-9]+)?)",
        sec_flat,
    )
    if match:
        return _geography_list(
            _annual_geography("China", _bn_from_millions(match.group(15)), _bn_from_millions(match.group(14))),
            _annual_geography("Taiwan", _bn_from_millions(match.group(12)), _bn_from_millions(match.group(11))),
            _annual_geography("South Korea", _bn_from_millions(match.group(6)), _bn_from_millions(match.group(5))),
            _annual_geography("United States", _bn_from_millions(match.group(27)), _bn_from_millions(match.group(26))),
            _annual_geography("Japan", _bn_from_millions(match.group(3)), _bn_from_millions(match.group(2))),
            _annual_geography("Singapore", _bn_from_millions(match.group(9)), _bn_from_millions(match.group(8))),
            _annual_geography("Rest of Asia", _bn_from_millions(match.group(18)), _bn_from_millions(match.group(17))),
            _annual_geography("EMEA", _bn_from_millions(match.group(24)), _bn_from_millions(match.group(23))),
            _annual_geography("Netherlands", _bn_from_millions(match.group(21)), _bn_from_millions(match.group(20))),
        )

    legacy_match = _search(
        r"Total net sales and long-lived assets \(consisting of property, plant and equipment\) by geographic region were as follows:\s*"
        r"Year ended December\s*31\s*Total net sales\s*Long-lived assets\s*\(in millions\)\s*EUR\s*EUR\s*"
        r"(\d{4})\s*"
        r"Japan\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Korea\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Singapore\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Taiwan\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"China\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Rest of Asia\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Netherlands\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"EMEA\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"United States\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Total\s*[0-9,]+(?:\.[0-9]+)?\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"(\d{4})\s*1?\s*"
        r"Japan\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Korea\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Singapore\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Taiwan\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"China\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Rest of Asia\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"Netherlands\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"EMEA\s*([0-9,]+(?:\.[0-9]+)?)\s*[0-9,]+(?:\.[0-9]+)?\s*"
        r"United States\s*([0-9,]+(?:\.[0-9]+)?)",
        sec_flat,
    )
    if not legacy_match:
        return []
    return _geography_list(
        _annual_geography("China", _bn_from_millions(legacy_match.group(6)), _bn_from_millions(legacy_match.group(15))),
        _annual_geography("Taiwan", _bn_from_millions(legacy_match.group(5)), _bn_from_millions(legacy_match.group(14))),
        _annual_geography("South Korea", _bn_from_millions(legacy_match.group(3)), _bn_from_millions(legacy_match.group(12))),
        _annual_geography("United States", _bn_from_millions(legacy_match.group(10)), _bn_from_millions(legacy_match.group(19))),
        _annual_geography("Japan", _bn_from_millions(legacy_match.group(2)), _bn_from_millions(legacy_match.group(11))),
        _annual_geography("Singapore", _bn_from_millions(legacy_match.group(4)), _bn_from_millions(legacy_match.group(13))),
        _annual_geography("Rest of Asia", _bn_from_millions(legacy_match.group(7)), _bn_from_millions(legacy_match.group(16))),
        _annual_geography("EMEA", _bn_from_millions(legacy_match.group(9)), _bn_from_millions(legacy_match.group(18))),
        _annual_geography("Netherlands", _bn_from_millions(legacy_match.group(8)), _bn_from_millions(legacy_match.group(17))),
    )


def _tsla_geographies(sec_flat: str) -> list[dict[str, Any]]:
    match = _search(
        r"The following table presents revenues by geographic area based on the sales location of our products \(in millions\):\s*"
        r"Year Ended December 31,\s*\d{4}\s*\d{4}\s*\d{4}\s*"
        r"United States\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s*"
        r"China\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+\s*"
        r"Other international\s*\$?\s*([0-9,]+)\s*\$?\s*([0-9,]+)\s*\$?\s*[0-9,]+",
        sec_flat,
    )
    if not match:
        return []
    return _geography_list(
        _annual_geography("United States", _bn_from_millions(match.group(1)), _bn_from_millions(match.group(2))),
        _annual_geography("China", _bn_from_millions(match.group(3)), _bn_from_millions(match.group(4))),
        _annual_geography("Other international", _bn_from_millions(match.group(5)), _bn_from_millions(match.group(6))),
    )


def _percent_tokens(line: str) -> list[float]:
    values: list[float] = []
    for token in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*%", line):
        value = _pct_value(token)
        if value is not None:
            values.append(value)
    return values


def _tsmc_platform_segments(
    company: dict[str, Any],
    quarter_label: str,
    materials: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    if revenue_bn in (None, 0):
        return []
    section_candidates: list[str] = []
    presentation = _pick_material(materials, kind="presentation")
    if presentation is not None:
        for content in (str(presentation.get("raw_text") or ""), str(presentation.get("flat_text") or "")):
            if "Platform" not in content and "Revenue by Platform" not in content:
                continue
            normalized = _clean_text(content)
            start = normalized.find("Platform")
            if start == -1:
                start = normalized.find("Revenue by Platform")
            if start == -1:
                continue
            end_candidates = [index for token in ("Resolution", "Wafer revenue", "Starting the first quarter", "Starting the second quarter") for index in [normalized.find(token, start + 20)] if index != -1]
            end = min(end_candidates) if end_candidates else min(len(normalized), start + 4000)
            section_candidates.append(normalized[start:end])
    if not section_candidates and presentation is None:
        return []

    value_aliases = {
        "Smartphone": ["Smartphone"],
        "HPC": ["High Performance Computing", "HPC"],
        "Internet of Things": ["Internet of Things", "IoT"],
        "Automotive": ["Automotive"],
        "DCE": ["Digital Consumer Electronics", "DCE"],
        "Others": ["Others"],
    }
    best_value_map: dict[str, tuple[float, Optional[float]]] = {}
    for section_text in section_candidates:
        value_map: dict[str, tuple[float, Optional[float]]] = {}
        for name, aliases in value_aliases.items():
            current_bn, prior_bn, yoy_pct = _extract_table_metric(section_text, aliases)
            if current_bn is None or current_bn <= 0:
                continue
            value_map[name] = (float(current_bn), yoy_pct if yoy_pct is None else float(yoy_pct))
        total = sum(value for value, _ in value_map.values())
        if len(value_map) >= 4 and total > 0:
            best_value_map = value_map
            break

    if best_value_map:
        total = sum(value for value, _ in best_value_map.values()) or 1.0
        segments = [
            _segment(
                name,
                float(revenue_bn) * value / total,
                yoy_pct,
            )
            for name in list(company.get("segment_order") or best_value_map.keys())
            for value, yoy_pct in [best_value_map.get(name, (None, None))]
            if value is not None and value > 0
        ]
        normalized_segments = [item for item in segments if item]
        if len(normalized_segments) >= 4:
            return normalized_segments

    application_crosswalk_weights = {
        "Internet of Things": 0.636,
        "Automotive": 0.182,
        "Others": 0.182,
    }
    for material in materials:
        if material.get("kind") != "presentation":
            continue
        content = _clean_text(str(material.get("raw_text") or ""))
        if "Revenue by Application" not in content and "Growth rate by application" not in content:
            continue
        share_map = {
            "Communication": _pct_value(_search(r"Communication\s+(\d+(?:\.\d+)?)%", content).group(1)) if _search(r"Communication\s+(\d+(?:\.\d+)?)%", content) else None,
            "Computer": _pct_value(_search(r"Computer\s+(\d+(?:\.\d+)?)%", content).group(1)) if _search(r"Computer\s+(\d+(?:\.\d+)?)%", content) else None,
            "Consumer": _pct_value(_search(r"Consumer\s+(\d+(?:\.\d+)?)%", content).group(1)) if _search(r"Consumer\s+(\d+(?:\.\d+)?)%", content) else None,
            "Industrial/Standard": _pct_value(_search(r"Industrial(?:/|\s)Standard\s+(\d+(?:\.\d+)?)%", content).group(1)) if _search(r"Industrial(?:/|\s)Standard\s+(\d+(?:\.\d+)?)%", content) else None,
        }
        if not share_map["Communication"] and not share_map["Computer"]:
            continue
        mapped_share_map = {
            "Smartphone": float(share_map["Communication"] or 0.0),
            "HPC": float(share_map["Computer"] or 0.0),
            "DCE": float(share_map["Consumer"] or 0.0),
        }
        industrial_share = float(share_map["Industrial/Standard"] or 0.0)
        for name, weight in application_crosswalk_weights.items():
            mapped_share_map[name] = industrial_share * weight
        total_share = sum(mapped_share_map.values())
        if total_share < 85:
            continue
        segments = [
            _segment(name, float(revenue_bn) * float(share_pct) / 100, None)
            for name in list(company.get("segment_order") or mapped_share_map.keys())
            for share_pct in [mapped_share_map.get(name)]
            if share_pct is not None and share_pct > 0
        ]
        normalized_segments = [item for item in segments if item]
        if len(normalized_segments) >= 4:
            return normalized_segments

    if presentation is None:
        return []

    lines = [_flatten_text(line) for line in _clean_text(str(presentation.get("raw_text") or "")).splitlines()]
    quarter_markers = ["Revenue by Platform"]
    quarter_match = re.search(r"(\d{4})Q([1-4])", quarter_label)
    if quarter_match:
        year_suffix = quarter_match.group(1)[-2:]
        quarter_markers.insert(0, f"{quarter_match.group(2)}Q{year_suffix} Revenue by Platform")
        quarter_markers.insert(1, f"{quarter_match.group(2)}Q{year_suffix}")

    start_indexes = [
        index
        for index, line in enumerate(lines)
        if any(marker in line for marker in quarter_markers)
    ]
    if not start_indexes:
        return []

    def _unsigned_percent_tokens(text: str) -> list[float]:
        values: list[float] = []
        for token in re.finditer(r"(?<![+\-\d])(\d+(?:\.\d+)?)%", text):
            value = _pct_value(token.group(1))
            if value is not None:
                values.append(value)
        return values

    def _find_share(text: str, pattern: str) -> Optional[float]:
        match = _search(pattern, text)
        if not match:
            return None
        return _pct_value(match.group(1))

    best_share_map: dict[str, float] = {}
    for start_index in start_indexes:
        block = [
            line
            for line in lines[start_index : min(len(lines), start_index + 14)]
            if line and not any(marker in line for marker in ("TSMC Property", "Unleash Innovation", "© 2026", "© 2025", "tsme", "=== "))
        ]
        block_text = " ".join(block)
        share_map: dict[str, float] = {}
        if "Smartphone" not in block_text:
            continue
        before_smartphone, after_smartphone = block_text.split("Smartphone", 1)
        small_segment_values = [value for value in _unsigned_percent_tokens(before_smartphone) if 0 < value <= 10]
        if len(small_segment_values) >= 4:
            share_map["Internet of Things"] = small_segment_values[0]
            share_map["Automotive"] = small_segment_values[1]
            share_map["DCE"] = small_segment_values[2]
            share_map["Others"] = small_segment_values[3]

        post_smartphone_values = [value for value in _unsigned_percent_tokens(after_smartphone) if value > 0]
        if post_smartphone_values:
            smartphone_share = post_smartphone_values[0]
            if 10 <= smartphone_share <= 45:
                share_map["Smartphone"] = smartphone_share
            hpc_candidates = [value for value in post_smartphone_values[1:] if value >= 20]
            if hpc_candidates:
                share_map["HPC"] = max(hpc_candidates)

        if not share_map:
            smartphone_share = _find_share(
                block_text,
                r"Smartphone(?:\s*[+\-]\d+(?:\.\d+)?%\s*){0,4}\s*(\d+(?:\.\d+)?)%",
            )
            if smartphone_share is not None:
                share_map["Smartphone"] = smartphone_share
            share_map["Internet of Things"] = _find_share(block_text, r"(?:IoT|loT)\s*(\d+(?:\.\d+)?)%") or 0.0
            share_map["Automotive"] = _find_share(block_text, r"Automotive\s*(\d+(?:\.\d+)?)%") or 0.0
            share_map["DCE"] = _find_share(block_text, r"DCE\s*(\d+(?:\.\d+)?)%") or 0.0
            share_map["Others"] = _find_share(block_text, r"Others\s*(\d+(?:\.\d+)?)%") or 0.0
            assigned_values = {
                round(value, 2)
                for key, value in share_map.items()
                if key != "HPC" and value is not None and value > 0
            }
            candidate_hpc_values = [
                value
                for value in _unsigned_percent_tokens(block_text)
                if value >= 12 and round(value, 2) not in assigned_values
            ]
            if candidate_hpc_values:
                share_map["HPC"] = max(candidate_hpc_values)

        share_map = {name: value for name, value in share_map.items() if value and value > 0}

        if sum(share_map.values()) > sum(best_share_map.values()):
            best_share_map = share_map

    if sum(best_share_map.values()) < 80:
        return []

    segments = [
        _segment(name, float(revenue_bn) * float(share_pct) / 100, None)
        for name in list(company.get("segment_order") or best_share_map.keys())
        for share_pct in [best_share_map.get(name)]
        if share_pct is not None
    ]
    return [item for item in segments if item]


def _tsmc_quarterly_geographies(
    materials: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    if revenue_bn in (None, 0):
        return []
    material_candidates = [
        item
        for item in materials
        if item.get("kind") in {"presentation", "official_release"}
        or (item.get("kind") == "sec_filing" and not _is_annual_material(item))
    ]
    geo_aliases = {
        "United States": ["United States"],
        "China": ["China"],
        "Japan": ["Japan"],
        "EMEA": ["Europe, the Middle East and Africa", "EMEA"],
        "Taiwan": ["Taiwan"],
        "Others": ["Others"],
    }
    best_value_map: dict[str, tuple[float, Optional[float]]] = {}
    for material in material_candidates:
        for content in (str(material.get("raw_text") or ""), str(material.get("flat_text") or "")):
            if "Geography" not in content:
                continue
            normalized = _clean_text(content)
            start = normalized.find("Geography")
            if start == -1:
                continue
            end_candidates = [index for token in ("The Company categorized", "Platform", "Resolution") for index in [normalized.find(token, start + 20)] if index != -1]
            end = min(end_candidates) if end_candidates else min(len(normalized), start + 3500)
            section_text = normalized[start:end]
            value_map: dict[str, tuple[float, Optional[float]]] = {}
            for name, aliases in geo_aliases.items():
                current_bn, prior_bn, yoy_pct = _extract_table_metric(section_text, aliases)
                if current_bn is None or current_bn <= 0:
                    continue
                value_map[name] = (float(current_bn), yoy_pct if yoy_pct is None else float(yoy_pct))
            total = sum(value for value, _ in value_map.values())
            if len(value_map) >= 4 and total > 0:
                best_value_map = value_map
                break
        if best_value_map:
            break

    if not best_value_map:
        return []
    total = sum(value for value, _ in best_value_map.values()) or 1.0
    geographies = [
        _geography(
            name,
            float(revenue_bn) * value / total,
            yoy_pct,
        )
        for name, (value, yoy_pct) in best_value_map.items()
        if value > 0
    ]
    return [item for item in geographies if item]


def _tsmc_statement_metrics(materials: list[dict[str, Any]]) -> dict[str, Optional[float]]:
    presentation = _pick_material(materials, kind="presentation")
    if presentation is None:
        return {}
    text = str(presentation.get("raw_text") or "")
    if "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME" not in text and "NET REVENUE" not in text:
        return {}

    def _find_row(label: str) -> tuple[Optional[float], Optional[float]]:
        pattern = (
            rf"{_table_label_pattern(label)}"
            r"\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+[0-9]+"
            r"\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+[0-9]+"
        )
        match = _search(pattern, text)
        if not match:
            return (None, None)
        return (_parse_number(match.group(1)), _parse_number(match.group(2)))

    revenue_current, revenue_prior = _find_row("NET REVENUE")
    gross_profit_current, _ = _find_row("GROSS PROFIT")
    operating_income_current, _ = _find_row("INCOME FROM OPERATIONS")
    net_income_current, net_income_prior = _find_row("NET INCOME")

    diluted_eps_current, diluted_eps_prior = _per_share_row(text, "Diluted earnings per share")
    basic_eps_current, basic_eps_prior = _per_share_row(text, "Basic earnings per share")

    gross_margin_pct = _safe_ratio_pct(gross_profit_current, revenue_current)
    operating_margin_pct = _safe_ratio_pct(operating_income_current, revenue_current)
    net_margin_pct = _safe_ratio_pct(net_income_current, revenue_current)

    return _clean_mapping(
        {
            "revenue_yoy_pct": _pct_change(revenue_current, revenue_prior),
            "gross_margin_pct": gross_margin_pct,
            "operating_margin_pct": operating_margin_pct,
            "net_margin_pct": net_margin_pct,
            "net_income_yoy_pct": _pct_change(net_income_current, net_income_prior),
            "gaap_eps": diluted_eps_current or basic_eps_current,
            "gaap_eps_yoy_pct": _pct_change(
                diluted_eps_current or basic_eps_current,
                diluted_eps_prior or basic_eps_prior,
            ),
        }
    )


def _extract_company_geographies(
    company_id: str,
    materials: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    narrative_materials = [item for item in materials if item.get("kind") in {"official_release", "presentation"}]
    quarterly_table_materials = [
        item
        for item in materials
        if item.get("kind") == "sec_filing" and not _is_annual_material(item)
    ]
    ordered_materials: list[dict[str, Any]] = []
    for material in narrative_materials + quarterly_table_materials + list(materials):
        if material in ordered_materials:
            continue
        ordered_materials.append(material)

    company_specific_parser = {
        "microsoft": _microsoft_geographies,
        "nvidia": _nvidia_geographies,
        "avgo": _avgo_geographies,
        "jpm": _jpm_geographies,
        "micron": _micron_geographies,
        "xom": _xom_geographies,
        "meta": _meta_geographies,
        "berkshire": _berkshire_geographies,
        "tsmc": _tsmc_geographies,
        "asml": _asml_geographies,
        "tsla": _tsla_geographies,
    }.get(company_id)
    if company_specific_parser is not None:
        best_company_specific: list[dict[str, Any]] = []
        for material in ordered_materials:
            if material.get("kind") != "sec_filing":
                continue
            extracted = company_specific_parser(str(material.get("flat_text") or ""))
            if not extracted:
                continue
            if _is_annual_material(material):
                extracted = [{**item, "scope": "annual_filing"} for item in extracted]
            if _segments_reasonable_for_revenue(extracted, revenue_bn) or _is_annual_material(material):
                if len(extracted) > len(best_company_specific):
                    best_company_specific = extracted
        if best_company_specific:
            return sorted(best_company_specific, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)

    best_match: list[dict[str, Any]] = []
    best_score = -1
    annual_fallback: list[dict[str, Any]] = []
    annual_fallback_score = -1
    generic_table_match: list[dict[str, Any]] = []
    generic_table_annual_fallback: list[dict[str, Any]] = []

    for material in ordered_materials:
        if material.get("kind") != "sec_filing":
            extracted = _extract_generic_geographies_from_text_tables(material, revenue_bn)
            if len(extracted) >= 2:
                if _is_annual_material(material):
                    generic_table_annual_fallback = _prefer_richer_geographies(generic_table_annual_fallback, extracted, revenue_bn)
                else:
                    generic_table_match = _prefer_richer_geographies(generic_table_match, extracted, revenue_bn)
            continue
        extracted = _extract_generic_geographies_from_html_tables(material, revenue_bn)
        if len(extracted) < 2:
            continue
        if _is_annual_material(material):
            generic_table_annual_fallback = _prefer_richer_geographies(generic_table_annual_fallback, extracted, revenue_bn)
            continue
        generic_table_match = _prefer_richer_geographies(generic_table_match, extracted, revenue_bn)

    for profile in GEOGRAPHY_PROFILES:
        for material in ordered_materials:
            if _ambiguous_geography_profile(profile) and not _material_has_geography_context(str(material.get("flat_text") or "")):
                continue
            extracted: list[dict[str, Any]] = []
            for region in profile:
                value_bn, yoy_pct = _extract_segment_metric(material, list(region["labels"]), prefer_table=True)
                if value_bn is None:
                    continue
                found_region = _geography(str(region["name"]), value_bn, yoy_pct)
                if found_region:
                    extracted.append(found_region)
            if len(extracted) < 2:
                continue
            score = len(extracted) * 10 + int(sum(float(item.get("value_bn") or 0.0) for item in extracted))
            if _segments_reasonable_for_revenue(extracted, revenue_bn) and score > best_score:
                best_match = sorted(extracted, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
                best_score = score
                continue
            if _is_annual_material(material) and score > annual_fallback_score:
                annual_fallback = [
                    {
                        **item,
                        "scope": "annual_filing",
                    }
                    for item in sorted(extracted, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
                ]
                annual_fallback_score = score
    best_match = _prefer_richer_geographies(best_match, generic_table_match, revenue_bn)
    annual_fallback = _prefer_richer_geographies(annual_fallback, generic_table_annual_fallback, revenue_bn)
    return best_match or annual_fallback


def _apple_legacy_geographies(text: str) -> list[dict[str, Any]]:
    geography_specs = [
        ("Americas", ["Americas net sales", "Americas revenue"]),
        ("Europe", ["Europe net sales", "Europe revenue"]),
        ("Japan", ["Japan net sales", "Japan revenue"]),
        ("Asia-Pacific", ["Asia-Pacific net sales", "Asia Pacific net sales", "Asia-Pacific revenue"]),
        ("Retail", ["Retail net sales", "Retail revenue"]),
    ]
    extracted: list[dict[str, Any]] = []
    for canonical_name, labels in geography_specs:
        current_bn, _, yoy_pct = _extract_table_metric(text, labels)
        item = _geography(canonical_name, current_bn, yoy_pct)
        if item is not None:
            extracted.append(item)
    return _geography_list(*extracted)


def _apple_legacy_geographies_from_materials(
    materials: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for material in materials:
        for content in (str(material.get("raw_text") or ""), str(material.get("flat_text") or "")):
            if not content:
                continue
            extracted = _apple_legacy_geographies(content)
            if not extracted:
                continue
            if not _segments_reasonable_for_revenue(extracted, revenue_bn):
                continue
            if len(extracted) > len(best):
                best = extracted
    return sorted(best, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)


def _extract_segment_metric(
    material: dict[str, Any],
    labels: list[str],
    *,
    prefer_table: bool = False,
) -> tuple[Optional[float], Optional[float]]:
    raw_text = str(material.get("raw_text") or "")
    flat_text = material["flat_text"]

    def _table_metric() -> tuple[Optional[float], Optional[float], Optional[float]]:
        html_current, html_prior, html_yoy = _extract_html_table_metric_from_material(material, labels)
        if html_current is not None:
            return (html_current, html_prior, html_yoy)
        if raw_text:
            current, prior, yoy = _extract_labeled_text_table_metric(raw_text, labels)
            if current is not None:
                return (current, prior, yoy)
        if raw_text:
            current, prior, yoy = _extract_table_metric(raw_text, labels)
            if current is not None:
                return (current, prior, yoy)
        return _extract_table_metric(flat_text, labels)

    if prefer_table:
        current, prior, yoy = _table_metric()
        if current is not None:
            return (current, yoy if yoy is not None else _pct_change(current, prior))
    for label in labels:
        narrative_value, narrative_yoy = _extract_segment_narrative_metric(flat_text, label)
        if narrative_value is not None and (narrative_yoy is None or abs(float(narrative_yoy)) <= 500):
            return (narrative_value, narrative_yoy)
        narrative_value, narrative_yoy = _extract_narrative_metric(flat_text, re.escape(label))
        if narrative_value is not None:
            return (narrative_value, narrative_yoy)
    if not prefer_table:
        current, prior, yoy = _table_metric()
        if current is not None:
            return (current, yoy if yoy is not None else _pct_change(current, prior))
    return (None, None)


SEGMENT_DISCOVERY_CONTEXT_TOKENS = (
    "segment",
    "business",
    "product",
    "platform",
    "application",
    "end-market",
    "end market",
    "sales by",
    "revenue by",
    "results by",
)

SEGMENT_DISCOVERY_ROW_BLACKLIST = {
    "total",
    "sales",
    "operating profit",
    "operating income",
    "gross profit",
    "net profit",
    "net income",
    "profit before income tax",
    "income tax",
}


def _generic_segment_name_from_line(line: str) -> Optional[str]:
    match = re.match(r"^(.*?)(?=\s+\(?-?[0-9])", _flatten_text(line), flags=re.IGNORECASE)
    if not match:
        return None
    name = re.sub(r"\s+", " ", match.group(1)).strip(" :-")
    normalized = name.casefold()
    if (
        not name
        or len(name) > 80
        or normalized in SEGMENT_DISCOVERY_ROW_BLACKLIST
        or name.startswith(("※", "•", "-", "*"))
        or any(token in normalized for token in ("results", "outlook", "financial data", "growth", "quarterly"))
    ):
        return None
    return name


def _extract_generic_segments_from_material(
    company_id: str,
    material: dict[str, Any],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    raw_text = str(material.get("raw_text") or "")
    lines = [_flatten_text(line) for line in raw_text.splitlines() if _flatten_text(line)]
    if not lines:
        return []
    best: list[dict[str, Any]] = []
    best_score = float("-inf")
    for header_index, line in enumerate(lines):
        headers = _parse_text_metric_headers(line)
        if len(headers) < 2:
            continue
        context = " ".join(lines[max(0, header_index - 6) : header_index + 1]).lower()
        if not any(token in context for token in SEGMENT_DISCOVERY_CONTEXT_TOKENS):
            continue
        current_index, prior_index = _pick_text_table_current_prior_indexes(headers)
        if current_index is None:
            continue
        scale_to_bn = _unit_scale_to_bn_from_text(" ".join(lines[max(0, header_index - 3) : header_index + 1]))
        rows: list[dict[str, Any]] = []
        for row_line in lines[header_index + 1 : min(len(lines), header_index + 18)]:
            segment_name = _generic_segment_name_from_line(row_line)
            if segment_name is None:
                if rows and re.search(r"\b(?:outlook|results|appendix|financial data)\b", row_line, flags=re.IGNORECASE):
                    break
                continue
            values = _line_numeric_values_without_pct(re.sub(r"^(.*?)(?=\s+\(?-?[0-9])", "", row_line, count=1).strip())
            if len(values) < len(headers):
                if rows:
                    break
                continue
            local_scale = scale_to_bn
            if local_scale is None:
                local_scale = 0.001 if max(abs(value) for value in values[: min(4, len(values))]) >= 1_000 else 1.0
            current = float(values[current_index]) * local_scale
            prior = float(values[prior_index]) * local_scale if prior_index is not None and prior_index < len(values) else None
            item = _segment(segment_name, current, _pct_change(current, prior))
            if item is not None:
                rows.append(item)
        if len(rows) < 2:
            continue
        pruned = _prune_overlapping_segments(company_id, rows, revenue_bn)
        if len(pruned) < 2:
            continue
        total = sum(float(item.get("value_bn") or 0.0) for item in pruned)
        closeness = 0.0
        if revenue_bn not in (None, 0):
            closeness = 1 - abs(float(revenue_bn) - total) / max(float(revenue_bn), 1.0)
        reasonable_bonus = 1.0 if _segments_reasonable_for_revenue(pruned, revenue_bn) else 0.0
        score = reasonable_bonus * 10 + len(pruned) + closeness
        if score > best_score:
            best = pruned
            best_score = score
    return best


def _extract_company_segments(
    company_id: str,
    materials: list[dict[str, Any]],
    revenue_bn: Optional[float] = None,
) -> list[dict[str, Any]]:
    profiles = _segment_profiles_for_company(company_id)
    narrative_materials = [item for item in materials if item.get("kind") in {"official_release", "presentation"}]
    quarterly_table_materials = [item for item in materials if item.get("kind") == "sec_filing" and not _is_annual_material(item)]
    ordered_materials: list[dict[str, Any]] = []
    for material in narrative_materials + quarterly_table_materials + list(materials):
        if material in ordered_materials:
            continue
        ordered_materials.append(material)
    extracted: list[dict[str, Any]] = []
    for profile in profiles:
        segment_name = str(profile["name"])
        labels = list(profile["labels"])
        found_segment: Optional[dict[str, Any]] = None
        for material in ordered_materials:
            value_bn, yoy_pct = _extract_segment_metric(
                material,
                labels,
                prefer_table=material.get("kind") == "sec_filing",
            )
            if value_bn is None:
                continue
            found_segment = _segment(segment_name, value_bn, yoy_pct)
            break
        if found_segment:
            extracted.append(found_segment)
    ranked = _prune_overlapping_segments(company_id, [item for item in extracted if item], revenue_bn)
    generic_segments: list[dict[str, Any]] = []
    for material in ordered_materials:
        generic_segments = _extract_generic_segments_from_material(company_id, material, revenue_bn)
        if generic_segments:
            break
    if generic_segments and (
        not ranked
        or (len(generic_segments) > len(ranked) and _segments_reasonable_for_revenue(generic_segments, revenue_bn))
        or not _segments_reasonable_for_revenue(ranked, revenue_bn)
    ):
        return generic_segments
    ranked.sort(key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
    return ranked


def _extract_segment_narrative_metric(flat_text: str, label: str) -> tuple[Optional[float], Optional[float]]:
    escaped = re.escape(label)
    patterns = [
        rf"{escaped}(?:\s+revenue[s]?)?.{{0,120}}?(?:up|increased|grew|rose)\s+([0-9]+(?:\.[0-9]+)?)\s*%(?:.{{0,120}}?)to\s+\$?([0-9,]+(?:\.[0-9]+)?)\s*(trillion|billion|million|thousand|tn|bn|mn|k)",
        rf"{escaped}(?:\s+revenue[s]?)?.{{0,120}}?(?:down|decreased|declined|fell)\s+([0-9]+(?:\.[0-9]+)?)\s*%(?:.{{0,120}}?)to\s+\$?([0-9,]+(?:\.[0-9]+)?)\s*(trillion|billion|million|thousand|tn|bn|mn|k)",
        rf"{escaped}(?:\s+revenue[s]?)?.{{0,80}}?\$?([0-9,]+(?:\.[0-9]+)?)\s*(trillion|billion|million|thousand|tn|bn|mn|k)(?:.{{0,120}}?)(?:up|increased|grew|rose)\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        rf"{escaped}(?:\s+revenue[s]?)?.{{0,80}}?\$?([0-9,]+(?:\.[0-9]+)?)\s*(trillion|billion|million|thousand|tn|bn|mn|k)(?:.{{0,120}}?)(?:down|decreased|declined|fell)\s+([0-9]+(?:\.[0-9]+)?)\s*%",
    ]
    for index, pattern in enumerate(patterns):
        match = _search(pattern, flat_text)
        if not match:
            continue
        if index == 0:
            return (_bn_from_unit(match.group(2), match.group(3)), _pct_value(match.group(1)))
        if index == 1:
            value = _pct_value(match.group(1))
            return (_bn_from_unit(match.group(2), match.group(3)), None if value is None else -value)
        if index == 2:
            return (_bn_from_unit(match.group(1), match.group(2)), _pct_value(match.group(3)))
        value = _pct_value(match.group(3))
        return (_bn_from_unit(match.group(1), match.group(2)), None if value is None else -value)
    return (None, None)


def _excerpt_around(flat_text: str, match: re.Match[str], width: int = 180) -> str:
    start = max(0, match.start() - 20)
    end = min(len(flat_text), match.end() + width)
    return flat_text[start:end].strip(" .,;")


def _guidance_excerpt_has_signal(text: str) -> bool:
    lowered = _flatten_text(text).lower()
    if not lowered:
        return False
    noise_tokens = (
        "income tax",
        "tax positions",
        "financial instruments",
        "balance sheet",
        "cash equivalents",
        "adoption",
        "disclosures",
        "note 2",
        "note 3",
        "note 4",
        "accounting",
        "fair value",
    )
    if sum(token in lowered for token in noise_tokens) >= 2:
        return False
    signal_tokens = (
        "revenue",
        "sales",
        "demand",
        "orders",
        "backlog",
        "margin",
        "inventory",
        "pricing",
        "shipments",
        "capex",
        "utilization",
        "growth",
        "expect",
        "outlook",
        "guidance",
    )
    return any(token in lowered for token in signal_tokens)


def _extract_generic_guidance(material: Optional[dict[str, Any]]) -> dict[str, Any]:
    if material is None:
        return {}
    flat_text = material["flat_text"]
    currency_prefix = r"(?:us\$|u\.s\.\$|usd\s+|eur\s+|€|\$)?\s*"
    range_patterns = [
        rf"(?:expects?|expected|guidance|outlook).{{0,120}}?(?:revenue|revenues|sales).{{0,80}}?between\s+{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)\s+and\s+{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)",
        rf"(?:revenue|revenues|sales).{{0,40}}?(?:is|are)\s+expected.{{0,40}}?between\s+{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)\s+and\s+{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)",
    ]
    for pattern in range_patterns:
        range_match = _search(pattern, flat_text)
        if not range_match:
            continue
        low_bn = _bn_from_unit(range_match.group(1), range_match.group(2))
        high_bn = _bn_from_unit(range_match.group(3), range_match.group(4))
        return {
            "mode": "official",
            "revenue_bn": _midpoint(low_bn, high_bn),
            "revenue_low_bn": low_bn,
            "revenue_high_bn": high_bn,
            "comparison_label": "下一季收入指引",
            "commentary": _guidance_midpoint_commentary(low_bn, high_bn, extra="官方原文已给出明确收入区间。"),
        }
    tolerance_match = _search(
        rf"(?:revenue|revenues|sales).{{0,40}}?(?:is|are)\s+expected.{{0,40}}?{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)\s*,?\s*(?:plus\s+or\s+minus|±)\s*([0-9]+(?:\.[0-9]+)?|zero|one|two|three|four|five|six|seven|eight|nine|ten)\s*%",
        flat_text,
    )
    if tolerance_match:
        revenue_bn = _bn_from_unit(tolerance_match.group(1), tolerance_match.group(2))
        tolerance_pct = _percent_token_value(tolerance_match.group(3))
        commentary = f"官方原文给出的收入展望约为 {format_money_bn(revenue_bn)}。"
        if tolerance_pct is not None:
            commentary = f"下一季收入指引约为 {format_money_bn(revenue_bn)}，容差约 ±{tolerance_pct:g}%。"
        return {
            "mode": "official",
            "revenue_bn": revenue_bn,
            "comparison_label": "下一季收入指引",
            "commentary": commentary,
        }
    point_patterns = [
        rf"(?:expects?|expected|guidance|outlook).{{0,120}}?(?:revenue|revenues|sales).{{0,40}}?{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)",
        rf"(?:revenue|revenues|sales).{{0,40}}?(?:is|are)\s+expected.{{0,40}}?{currency_prefix}([0-9,]+(?:\.[0-9]+)?)\s*(billion|million)",
    ]
    for pattern in point_patterns:
        point_match = _search(pattern, flat_text)
        if not point_match:
            continue
        revenue_bn = _bn_from_unit(point_match.group(1), point_match.group(2))
        return {
            "mode": "official",
            "revenue_bn": revenue_bn,
            "comparison_label": "下一季收入指引",
            "commentary": f"官方原文给出的收入展望约为 {format_money_bn(revenue_bn)}。",
        }
    context_match = _search(r"(?:expects?|expected|guidance|outlook).{0,220}", flat_text)
    if context_match:
        excerpt = _excerpt_around(flat_text, context_match)
        if not _guidance_excerpt_has_signal(excerpt):
            return {}
        return {
            "mode": "official_context",
            "commentary": f"官方展望语境摘录：{excerpt}。",
        }
    return {}


def _extract_quote_cards(material: Optional[dict[str, Any]]) -> list[dict[str, str]]:
    if material is None:
        return []
    raw_text = _clean_text(str(material.get("raw_text") or ""))
    source_label = str(material.get("label") or "Official materials")
    quote_verbs = r"(?:said|stated|noted|added|commented|remarked|explained)"
    patterns = [
        rf"[“\"]([^\"”]{{40,320}})[”\"],?\s+{quote_verbs}\s+([^.,\n]+)",
        rf"([^.,\n]+?)\s+{quote_verbs}[:,]?\s+[“\"]([^\"”]{{40,320}})[”\"]",
        rf"([^:\n]{{3,120}}):\s*[“\"]([^\"”]{{40,320}})[”\"]",
    ]
    cards: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pattern in patterns:
        for match in re.finditer(pattern, raw_text, flags=re.IGNORECASE | re.DOTALL):
            if pattern.startswith("[“"):
                quote = _flatten_text(match.group(1))
                speaker = _flatten_text(match.group(2))
            else:
                speaker = _flatten_text(match.group(1))
                quote = _flatten_text(match.group(2))
            if speaker.lower() in {"operator", "question", "analyst"}:
                continue
            key = (speaker, quote)
            if len(quote) < 40 or key in seen:
                continue
            cards.append(
                _quote_card(
                    speaker,
                    quote,
                    "管理层原文已被动态抓取，本页优先保留最能概括当季经营重心的官方表述。",
                    source_label,
                )
            )
            seen.add(key)
            if len(cards) >= 2:
                return cards
    return cards


def _generic_headline(
    company: dict[str, Any],
    fallback: dict[str, Any],
    revenue_bn: Optional[float],
    revenue_yoy_pct: Optional[float],
    net_income_bn: Optional[float],
    net_income_yoy_pct: Optional[float],
    driver: Optional[str],
) -> Optional[str]:
    if revenue_bn is None:
        return None
    parts = [f"{company['english_name']} {fallback['fiscal_label']} 收入 {format_money_bn(revenue_bn, company['money_symbol'])}"]
    if revenue_yoy_pct is not None:
        parts.append(f"同比 {format_pct(revenue_yoy_pct, signed=True)}")
    headline = "，".join(parts)
    if net_income_bn is not None:
        headline += f"；净利润 {format_money_bn(net_income_bn, company['money_symbol'])}"
        if net_income_yoy_pct is not None:
            headline += f"，同比 {format_pct(net_income_yoy_pct, signed=True)}"
    if driver:
        headline += f"；{driver}"
    headline += "。"
    return headline


def _generic_takeaways(
    company: dict[str, Any],
    facts: dict[str, Any],
    fallback: dict[str, Any],
) -> list[str]:
    revenue_bn = facts.get("revenue_bn")
    revenue_yoy_pct = facts.get("revenue_yoy_pct")
    segments = facts.get("segments") or []
    money_symbol = company["money_symbol"]
    lines: list[str] = []
    if revenue_bn is not None:
        top_segments = sorted(segments, key=lambda item: float(item["value_bn"]), reverse=True)[:2]
        if top_segments:
            segment_text = "、".join(
                f"{item['name']} {format_money_bn(float(item['value_bn']), money_symbol)}"
                + (
                    f"（同比 {format_pct(float(item['yoy_pct']), signed=True)}）"
                    if item.get("yoy_pct") is not None
                    else ""
                )
                for item in top_segments
            )
            lines.append(
                f"收入 {format_money_bn(revenue_bn, money_symbol)}，同比 {format_pct(revenue_yoy_pct, signed=True)}；核心结构由 {segment_text} 支撑。"
            )
        else:
            lines.append(
                f"收入 {format_money_bn(revenue_bn, money_symbol)}，同比 {format_pct(revenue_yoy_pct, signed=True)}。"
            )

    quality_bits: list[str] = []
    if facts.get("gross_margin_pct") is not None:
        quality_bits.append(f"毛利率 {format_pct(facts['gross_margin_pct'])}")
    if facts.get("gaap_eps") is not None:
        eps_text = f"EPS {facts['gaap_eps']:.2f}"
        if facts.get("gaap_eps_yoy_pct") is not None:
            eps_text += f"，同比 {format_pct(facts['gaap_eps_yoy_pct'], signed=True)}"
        quality_bits.append(eps_text)
    if facts.get("operating_cash_flow_bn") is not None:
        quality_bits.append(f"经营现金流 {format_money_bn(facts['operating_cash_flow_bn'], money_symbol)}")
    elif facts.get("free_cash_flow_bn") is not None:
        quality_bits.append(f"自由现金流 {format_money_bn(facts['free_cash_flow_bn'], money_symbol)}")
    elif facts.get("operating_income_bn") is not None:
        quality_bits.append(f"经营利润 {format_money_bn(facts['operating_income_bn'], money_symbol)}")
    if quality_bits:
        lines.append("；".join(quality_bits) + "。")

    guidance = facts.get("guidance") or {}
    guidance_commentary = str(guidance.get("commentary") or "").strip()
    if guidance_commentary:
        lines.append(guidance_commentary)
    elif facts.get("driver"):
        lines.append(str(facts["driver"]))
    return lines[:3]


def _generic_evidence_cards(company: dict[str, Any], facts: dict[str, Any]) -> list[dict[str, Any]]:
    money_symbol = company["money_symbol"]
    primary_source = facts.get("primary_source_label", "Official materials")
    structure_source = facts.get("structure_source_label", primary_source)
    guidance = facts.get("guidance") or {}
    guidance_commentary = str(guidance.get("commentary") or "").strip()
    cards: list[dict[str, Any]] = []
    revenue_bn = facts.get("revenue_bn")
    operating_income_bn = facts.get("operating_income_bn")
    net_income_bn = facts.get("net_income_bn")
    if revenue_bn is not None:
        quarter_text = f"当季收入 {format_money_bn(revenue_bn, money_symbol)}"
        if operating_income_bn is not None:
            quarter_text += f"，经营利润 {format_money_bn(operating_income_bn, money_symbol)}"
        if net_income_bn is not None:
            quarter_text += f"，净利润 {format_money_bn(net_income_bn, money_symbol)}"
        quarter_text += "。"
        cards.append(
            {
                "title": "季度结果",
                "text": quarter_text,
                "source_label": primary_source,
            }
        )

    segments = facts.get("segments") or []
    if segments:
        top_segments = sorted(segments, key=lambda item: float(item["value_bn"]), reverse=True)[:3]
        structure_text = "；".join(
            f"{item['name']} {format_money_bn(float(item['value_bn']), money_symbol)}"
            + (
                f"，同比 {format_pct(float(item['yoy_pct']), signed=True)}"
                if item.get("yoy_pct") is not None
                else ""
            )
            for item in top_segments
        )
        cards.append(
            {
                "title": "结构与驱动",
                "text": structure_text + "。",
                "source_label": structure_source,
            }
        )

    if guidance_commentary:
        cards.append(
            {
                "title": "展望与管理层语境",
                "text": guidance_commentary,
                "source_label": facts.get("guidance_source_label", primary_source),
            }
        )
    if len(cards) < 3:
        quality_bits: list[str] = []
        if facts.get("gross_margin_pct") is not None:
            quality_bits.append(f"毛利率 {format_pct(float(facts['gross_margin_pct']))}")
        if facts.get("gaap_eps") is not None:
            quality_bits.append(f"GAAP EPS {float(facts['gaap_eps']):.2f}")
        if facts.get("operating_cash_flow_bn") is not None:
            quality_bits.append(f"经营现金流 {format_money_bn(float(facts['operating_cash_flow_bn']), money_symbol)}")
        elif facts.get("free_cash_flow_bn") is not None:
            quality_bits.append(f"自由现金流 {format_money_bn(float(facts['free_cash_flow_bn']), money_symbol)}")
        if quality_bits:
            cards.append(
                {
                    "title": "盈利与现金质量",
                    "text": "；".join(quality_bits) + "。",
                    "source_label": primary_source,
                }
            )
    return cards[:3]


def _generic_management_themes(company: dict[str, Any], facts: dict[str, Any]) -> list[dict[str, Any]]:
    money_symbol = company["money_symbol"]
    revenue_bn = facts.get("revenue_bn")
    revenue_yoy_pct = facts.get("revenue_yoy_pct")
    segments = facts.get("segments") or []
    geographies = facts.get("geographies") or []
    items = list(facts.get("management_theme_items") or [])
    fastest = _fastest_segment(segments)
    if fastest:
        items.append(
            _theme(
                f"{fastest['name']} 增长弹性",
                84 + min(max(float(fastest.get("yoy_pct") or 0.0) * 0.12, 0), 10),
                f"{fastest['name']} 收入 {format_money_bn(float(fastest['value_bn']), money_symbol)}，同比 {format_pct(float(fastest['yoy_pct']), signed=True)}。",
            )
        )
    top = _top_segment(segments)
    share_pct = _segment_share(top, revenue_bn)
    if top and share_pct is not None:
        items.append(
            _theme(
                "结构重心",
                76,
                f"{top['name']} 占当季收入 {format_pct(share_pct)}，仍是阅读本季财报的第一锚点。",
            )
        )
    if revenue_bn is not None and revenue_yoy_pct is not None:
        items.append(
            _theme(
                "总量成长节奏",
                72 + min(max(abs(float(revenue_yoy_pct)) * 0.2, 0), 12),
                f"当季收入 {format_money_bn(revenue_bn, money_symbol)}，同比 {format_pct(revenue_yoy_pct, signed=True)}。",
            )
        )
    if facts.get("gross_margin_pct") is not None:
        items.append(
            _theme(
                "利润质量",
                74 + min(max(float(facts["gross_margin_pct"]) * 0.18, 0), 12),
                f"官方材料对应毛利率 {format_pct(facts['gross_margin_pct'])}。"
                if facts.get("operating_cash_flow_bn") is None
                else f"毛利率 {format_pct(facts['gross_margin_pct'])}，经营现金流 {format_money_bn(facts['operating_cash_flow_bn'], money_symbol)}。",
            )
        )
    if facts.get("net_income_bn") is not None and facts.get("net_income_yoy_pct") is not None:
        items.append(
            _theme(
                "利润兑现",
                74 + min(max(abs(float(facts["net_income_yoy_pct"])) * 0.12, 0), 10),
                f"净利润 {format_money_bn(facts['net_income_bn'], money_symbol)}，同比 {format_pct(facts['net_income_yoy_pct'], signed=True)}。",
            )
        )
    if geographies:
        top_geo = max(geographies, key=lambda item: float(item.get("value_bn") or 0.0))
        items.append(
            _theme(
                "地区需求重心",
                68,
                f"已披露地区结构中，{top_geo['name']} 是当前最大区域收入池，反映公司本季主要需求落点。",
            )
        )
    guidance = facts.get("guidance") or {}
    guidance_commentary = str(guidance.get("commentary") or "").strip()
    if guidance_commentary:
        items.append(_theme("管理层展望", 72, guidance_commentary))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        label = str(item.get("label") or "")
        if not label or label in seen:
            continue
        deduped.append(item)
        seen.add(label)
    return deduped[:4]


def _generic_qna_themes(company: dict[str, Any], facts: dict[str, Any]) -> list[dict[str, Any]]:
    money_symbol = company["money_symbol"]
    items = list(facts.get("qna_theme_items") or [])
    segments = facts.get("segments") or []
    geographies = facts.get("geographies") or []
    top = _top_segment(segments)
    fastest = _fastest_segment(segments)
    if top:
        items.append(
            _theme(
                f"{top['name']} 可持续性",
                78,
                f"头部业务 {top['name']} 体量最大，市场会追问其需求和收入贡献能否继续延续。",
            )
        )
    if fastest and fastest != top:
        items.append(
            _theme(
                f"{fastest['name']} 加速来源",
                74,
                f"{fastest['name']} 同比 {format_pct(float(fastest['yoy_pct']), signed=True)}，需要继续拆分增长来自 volume、pricing 还是新产品/AI 供给。",
            )
        )
    if facts.get("revenue_yoy_pct") is not None:
        items.append(
            _theme(
                "增长可持续性",
                70,
                f"本季收入同比 {format_pct(facts['revenue_yoy_pct'], signed=True)}，市场会继续追问增长是阶段性抬升还是新中枢形成。",
            )
        )
    if facts.get("operating_cash_flow_bn") is not None or facts.get("free_cash_flow_bn") is not None:
        cash_value = facts.get("operating_cash_flow_bn") or facts.get("free_cash_flow_bn")
        cash_label = "经营现金流" if facts.get("operating_cash_flow_bn") is not None else "自由现金流"
        items.append(
            _theme(
                "现金兑现强度",
                70,
                f"{cash_label}达到 {format_money_bn(cash_value, money_symbol)}，后续问答会继续围绕资本开支、现金回流和利润兑现展开。",
            )
        )
    if geographies:
        top_geo = max(geographies, key=lambda item: float(item.get("value_bn") or 0.0))
        items.append(
            _theme(
                "区域需求分化",
                66,
                f"地区披露显示 {top_geo['name']} 贡献最高，后续问答会继续追踪不同区域的景气差异。",
            )
        )
    guidance = facts.get("guidance") or {}
    guidance_commentary = str(guidance.get("commentary") or "").strip()
    if guidance.get("mode") == "official" and guidance_commentary:
        items.append(_theme("指引兑现度", 82, guidance_commentary))
    elif guidance_commentary:
        items.append(_theme("官方展望与经营基线", 72, guidance_commentary))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        label = str(item.get("label") or "")
        if not label or label in seen:
            continue
        deduped.append(item)
        seen.add(label)
    return deduped[:4]


def _generic_risks(company: dict[str, Any], facts: dict[str, Any]) -> list[dict[str, Any]]:
    items = list(facts.get("risk_items") or [])
    segments = facts.get("segments") or []
    top = _top_segment(segments)
    share_pct = _segment_share(top, facts.get("revenue_bn"))
    if top and share_pct is not None and share_pct >= 40:
        items.append(
            _theme(
                f"{top['name']} 集中度风险",
                70,
                f"{top['name']} 占收入约 {format_pct(share_pct)}，一旦需求或供给节奏变化，会直接放大对整体收入与利润的影响。",
            )
        )
    if facts.get("revenue_yoy_pct") is not None and float(facts["revenue_yoy_pct"]) < 0:
        items.append(
            _theme(
                "收入增速回落",
                68,
                f"当季收入同比 {format_pct(facts['revenue_yoy_pct'], signed=True)}，若回落延续，会先体现在结构页和经营杠杆页。",
            )
        )
    if facts.get("gross_margin_pct") is not None and facts["gross_margin_pct"] < 25:
        items.append(
            _theme(
                "利润率波动",
                68,
                f"当前毛利率 {format_pct(facts['gross_margin_pct'])}，若价格、成本或产品结构承压，利润弹性会先受到影响。",
            )
        )
    if facts.get("net_income_yoy_pct") is not None and float(facts["net_income_yoy_pct"]) < 0:
        items.append(
            _theme(
                "利润兑现承压",
                66,
                f"净利润同比 {format_pct(facts['net_income_yoy_pct'], signed=True)}，意味着经营杠杆或费用控制仍需继续观察。",
            )
        )
    guidance = facts.get("guidance") or {}
    guidance_commentary = str(guidance.get("commentary") or "").strip()
    if guidance.get("mode") == "official" and guidance_commentary:
        items.append(_theme("指引下沿压力", 66, guidance_commentary))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        label = str(item.get("label") or "")
        if not label or label in seen:
            continue
        deduped.append(item)
        seen.add(label)
    return deduped[:3]


def _generic_catalysts(company: dict[str, Any], facts: dict[str, Any]) -> list[dict[str, Any]]:
    money_symbol = company["money_symbol"]
    items = list(facts.get("catalyst_items") or [])
    fastest = _fastest_segment(facts.get("segments") or [])
    if fastest and fastest.get("yoy_pct") is not None and float(fastest["yoy_pct"]) > 0:
        items.append(
            _theme(
                f"{fastest['name']} 延续高增",
                84,
                f"{fastest['name']} 本季达到 {format_money_bn(float(fastest['value_bn']), money_symbol)}，若高增延续，会继续抬高整体结构质量。",
            )
        )
    if facts.get("revenue_yoy_pct") is not None and float(facts["revenue_yoy_pct"]) > 0:
        items.append(
            _theme(
                "总量增长延续",
                76,
                f"收入同比 {format_pct(facts['revenue_yoy_pct'], signed=True)}，若需求与订单节奏继续维持，TTM 增速有望进一步抬升。",
            )
        )
    if facts.get("operating_cash_flow_bn") is not None:
        items.append(
            _theme(
                "现金流兑现",
                76,
                f"经营现金流 {format_money_bn(facts['operating_cash_flow_bn'], money_symbol)}，意味着业绩改善更容易转化为估值韧性。",
            )
        )
    guidance = facts.get("guidance") or {}
    guidance_commentary = str(guidance.get("commentary") or "").strip()
    if guidance.get("mode") == "official" and guidance_commentary:
        items.append(_theme("官方指引支撑", 78, guidance_commentary))
    elif guidance_commentary:
        items.append(_theme("管理层积极语境", 70, guidance_commentary))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        label = str(item.get("label") or "")
        if not label or label in seen:
            continue
        deduped.append(item)
        seen.add(label)
    return deduped[:3]


def _quarterize_annual_geographies(
    geographies: list[dict[str, Any]],
    revenue_bn: Optional[float],
) -> list[dict[str, Any]]:
    if not geographies:
        return []
    if revenue_bn in (None, 0):
        return list(geographies)
    annual_items = [
        item
        for item in geographies
        if str(item.get("scope") or "").casefold() == "annual_filing" and float(item.get("value_bn") or 0.0) > 0
    ]
    if len(annual_items) < 2:
        return list(geographies)
    annual_total = sum(float(item.get("value_bn") or 0.0) for item in annual_items)
    if annual_total <= 0:
        return list(geographies)

    quarter_revenue = float(revenue_bn)
    converted: list[dict[str, Any]] = []
    for item in geographies:
        normalized = dict(item)
        if str(item.get("scope") or "").casefold() == "annual_filing":
            annual_value = max(float(item.get("value_bn") or 0.0), 0.0)
            share = annual_value / annual_total
            mapped_value = round(quarter_revenue * share, 3)
            if mapped_value <= 0 and annual_value > 0 and quarter_revenue > 0:
                mapped_value = 0.001
            normalized["value_bn"] = mapped_value
            normalized["share_pct"] = round(share * 100, 2)
            normalized["scope"] = "quarterly_mapped_from_official_geography"
        converted.append(normalized)
    return converted


def _ensure_minimum_management_themes(
    management_themes: list[dict[str, Any]],
    qna_themes: list[dict[str, Any]],
    *,
    minimum: int = 3,
) -> list[dict[str, Any]]:
    enriched = [dict(item) for item in list(management_themes or []) if isinstance(item, dict)]
    seen = {str(item.get("label") or "") for item in enriched if str(item.get("label") or "")}
    for item in list(qna_themes or []):
        if len(enriched) >= minimum:
            break
        label = str(item.get("label") or "").strip()
        note = str(item.get("note") or "").strip()
        if not label or not note:
            continue
        candidate_label = f"经营跟踪：{label}"
        if candidate_label in seen:
            continue
        enriched.append(_theme(candidate_label, float(item.get("score") or 68), note))
        seen.add(candidate_label)
    return enriched


def _ensure_minimum_qna_themes(
    qna_themes: list[dict[str, Any]],
    management_themes: list[dict[str, Any]],
    risks: list[dict[str, Any]],
    catalysts: list[dict[str, Any]],
    *,
    minimum: int = 3,
) -> list[dict[str, Any]]:
    enriched = [dict(item) for item in list(qna_themes or []) if isinstance(item, dict)]
    seen = {str(item.get("label") or "") for item in enriched if str(item.get("label") or "")}
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
            enriched.append(_theme(candidate_label, float(item.get("score") or 70), note))
            seen.add(candidate_label)
        if len(enriched) >= minimum:
            break
    return enriched


def _ensure_minimum_evidence_cards(
    evidence_cards: list[dict[str, Any]],
    *,
    source_label: str,
    management_themes: list[dict[str, Any]],
    qna_themes: list[dict[str, Any]],
    guidance_commentary: str,
    minimum: int = 3,
) -> list[dict[str, Any]]:
    cards = [dict(item) for item in list(evidence_cards or []) if isinstance(item, dict)]
    if len(cards) >= minimum:
        return cards
    for item in list(management_themes or []) + list(qna_themes or []):
        if len(cards) >= minimum:
            break
        label = str(item.get("label") or "").strip()
        note = str(item.get("note") or "").strip()
        if not label or not note:
            continue
        cards.append(
            {
                "title": f"要点核验：{label}",
                "text": note if note.endswith("。") else f"{note}。",
                "source_label": source_label,
            }
        )
    if len(cards) < minimum and guidance_commentary:
        cards.append(
            {
                "title": "展望核验",
                "text": guidance_commentary if guidance_commentary.endswith("。") else f"{guidance_commentary}。",
                "source_label": source_label,
            }
        )
    return cards[: max(minimum, len(cards))]


def _ensure_minimum_quote_cards(
    quote_cards: list[dict[str, Any]],
    *,
    source_label: str,
    management_themes: list[dict[str, Any]],
    qna_themes: list[dict[str, Any]],
    guidance_commentary: str,
    minimum: int = 2,
) -> list[dict[str, Any]]:
    quotes = [dict(item) for item in list(quote_cards or []) if isinstance(item, dict)]
    if len(quotes) >= minimum:
        return quotes
    for item in list(management_themes or []) + list(qna_themes or []):
        if len(quotes) >= minimum:
            break
        note = str(item.get("note") or "").strip()
        label = str(item.get("label") or "").strip()
        if not note:
            continue
        quotes.append(
            _quote_card(
                "Management context",
                note[:220],
                f"该观点围绕“{label or '经营主线'}”展开，可直接用于电话会追问框架。",
                source_label,
            )
        )
    if len(quotes) < minimum and guidance_commentary:
        quotes.append(
            _quote_card(
                "Guidance context",
                guidance_commentary[:220],
                "官方展望可作为电话会验证点，用于核对下一季兑现节奏。",
                source_label,
            )
        )
    return quotes[: max(minimum, len(quotes))]


def _finalize(
    company: dict[str, Any],
    fallback: dict[str, Any],
    facts: dict[str, Any],
    materials: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    facts = _sanitize_temporal_narrative_facts(facts, fallback)
    revenue_bn = facts.get("revenue_bn")
    revenue_yoy_pct = facts.get("revenue_yoy_pct")
    net_income_bn = facts.get("net_income_bn")
    net_income_yoy_pct = facts.get("net_income_yoy_pct")

    latest_kpis = _clean_mapping(
        {
            "revenue_bn": revenue_bn,
            "revenue_yoy_pct": revenue_yoy_pct,
            "revenue_qoq_pct": facts.get("revenue_qoq_pct"),
            "gaap_gross_margin_pct": facts.get("gross_margin_pct"),
            "non_gaap_gross_margin_pct": facts.get("non_gaap_gross_margin_pct", facts.get("gross_margin_pct")),
            "operating_income_bn": facts.get("operating_income_bn"),
            "net_income_bn": net_income_bn,
            "net_income_yoy_pct": net_income_yoy_pct,
            "operating_cash_flow_bn": facts.get("operating_cash_flow_bn"),
            "free_cash_flow_bn": facts.get("free_cash_flow_bn"),
            "gaap_eps": facts.get("gaap_eps"),
            "non_gaap_eps": facts.get("non_gaap_eps", facts.get("gaap_eps")),
            "ending_equity_bn": facts.get("ending_equity_bn"),
        }
    )

    headline = facts.get("headline") or _generic_headline(
        company,
        fallback,
        revenue_bn,
        revenue_yoy_pct,
        net_income_bn,
        net_income_yoy_pct,
        facts.get("driver"),
    )

    takeaways = facts.get("takeaways") or _generic_takeaways(company, facts, fallback)
    management_themes = facts.get("management_themes") or _generic_management_themes(company, facts)
    qna_themes = facts.get("qna_themes") or _generic_qna_themes(company, facts)
    risks = facts.get("risks") or _generic_risks(company, facts)
    catalysts = facts.get("catalysts") or _generic_catalysts(company, facts)
    management_themes = _ensure_minimum_management_themes(management_themes, qna_themes)
    qna_themes = _ensure_minimum_qna_themes(qna_themes, management_themes, risks, catalysts)
    management_themes = _ensure_minimum_management_themes(management_themes, qna_themes)
    guidance_commentary = str((facts.get("guidance") or {}).get("commentary") or "").strip()
    evidence_cards = _ensure_minimum_evidence_cards(
        facts.get("evidence_cards") or _generic_evidence_cards(company, facts),
        source_label=str(facts.get("primary_source_label") or "Official materials"),
        management_themes=management_themes,
        qna_themes=qna_themes,
        guidance_commentary=guidance_commentary,
    )
    quote_cards = _ensure_minimum_quote_cards(
        facts.get("quotes") or [],
        source_label=str(facts.get("primary_source_label") or "Official materials"),
        management_themes=management_themes,
        qna_themes=qna_themes,
        guidance_commentary=guidance_commentary,
    )
    geographies = facts.get("geographies")
    segments = facts.get("segments")
    if not segments and materials is not None:
        segments = _extract_company_segments(str(company["id"]), materials, revenue_bn)
    segments = _prune_overlapping_segments(str(company["id"]), list(segments or []), revenue_bn)
    if geographies is None and materials is not None:
        geographies = _extract_company_geographies(str(company["id"]), materials, revenue_bn)
    geographies = _quarterize_annual_geographies(list(geographies or []), revenue_bn)
    if geographies and _geographies_look_suspicious(list(geographies), revenue_bn):
        geographies = []
    if geographies and segments and not any(str(item.get("scope") or "") == "annual_filing" for item in geographies):
        segment_map = {str(item.get("name") or "").casefold(): float(item.get("value_bn") or 0.0) for item in segments}
        segment_names = set(segment_map.keys())
        overlap_names = [
            str(item.get("name") or "")
            for item in geographies
            if str(item.get("name") or "").casefold() in segment_map
            and abs(float(item.get("value_bn") or 0.0) - segment_map[str(item.get("name") or "").casefold()]) <= max(0.15, float(revenue_bn or 0.0) * 0.012)
        ]
        geo_names = {str(item.get("name") or "").casefold() for item in geographies}
        if geo_names.issubset(segment_names) and len(segment_names - geo_names) >= 1 and len(geo_names) >= 2:
            geographies = []
        elif len(overlap_names) >= 2 and geo_names.issubset(segment_names):
            geographies = []
    if not geographies and segments:
        regional_segment_names = {
            "north america",
            "international",
            "united states",
            "united states and canada",
            "americas",
            "europe",
            "asia-pacific",
            "apac",
            "apj",
            "greater china",
            "japan",
            "rest of world",
        }
        regional_segments = [
            {**item, "scope": "regional_segment"}
            for item in segments
            if str(item.get("name") or "").casefold() in regional_segment_names
        ]
        if len(regional_segments) >= 2:
            geographies = regional_segments

    coverage_notes = [
        "当前季度 KPI、摘要、证据卡与管理层锚点已优先根据官方原文动态解析，未披露字段才回退到内置口径。"
    ]
    if facts.get("coverage_notes"):
        coverage_notes.extend(list(facts["coverage_notes"]))
    if geographies:
        coverage_notes.append("当前季度已动态补入地区营收结构，优先采用官方披露原文中的地理口径。")
        if any(str(item.get("scope") or "") == "annual_filing" for item in geographies):
            coverage_notes.append("公司当季未单列地区收入表时，地区结构会保留官方年报地区披露口径并显式标记来源。")
        elif any(str(item.get("scope") or "") == "quarterly_mapped_from_official_geography" for item in geographies):
            coverage_notes.append("地区结构已按官方地理披露占比完成季度化映射，确保与当季收入口径一致。")
        elif any(str(item.get("scope") or "") == "regional_segment" for item in geographies):
            coverage_notes.append("若公司按区域经营分部披露而未单列终端地理收入，地区结构页会采用区域经营分部口径并显式标注。")

    payload = _clean_mapping(
        {
            "headline": headline,
            "takeaways": takeaways,
            "latest_kpis": latest_kpis,
            "guidance": facts.get("guidance"),
            "current_segments": segments,
            "current_geographies": geographies,
            "income_statement": facts.get("income_statement"),
            "management_themes": management_themes,
            "qna_themes": qna_themes,
            "risks": risks,
            "catalysts": catalysts,
            "evidence_cards": evidence_cards,
            "call_quote_cards": quote_cards,
            "coverage_notes": coverage_notes,
        }
    )
    return payload


def _parse_apple(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing")
    if sec is None:
        return {}
    sec_flat = str(sec.get("flat_text") or "")
    legacy_segment_markers = (
        "iphone and related products and services",
        "other music related products and services",
        "software, service and other sales",
        "software, service, and other sales",
        "total mac net sales",
    )
    modern_segment_markers = (
        "wearables, home and accessories",
        "wearables, home & accessories",
        "services",
        "products and services performance",
    )
    if any(marker in sec_flat.lower() for marker in legacy_segment_markers):
        return _parse_apple_legacy(company, fallback, materials)
    if release is None:
        if any(marker in sec_flat.lower() for marker in modern_segment_markers):
            return _parse_apple_dynamic(company, fallback, materials)
        return _parse_apple_legacy(company, fallback, materials)

    release_flat = release["flat_text"]
    if "apple intelligence" not in release_flat.lower():
        return _parse_apple_dynamic(company, fallback, materials)
    sec_raw = sec["raw_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, prior_revenue_bn, revenue_yoy_pct = _extract_table_metric(sec_raw, ["Total net sales"])
    iphone_bn, _, iphone_yoy = _extract_table_metric(sec_raw, ["iPhone"])
    mac_bn, _, mac_yoy = _extract_table_metric(sec_raw, ["Mac"])
    ipad_bn, _, ipad_yoy = _extract_table_metric(sec_raw, ["iPad"])
    wearables_bn, _, wearables_yoy = _extract_table_metric(sec_raw, ["Wearables, Home and Accessories"])
    services_bn, _, services_yoy = _extract_table_metric(sec_raw, ["Services"])
    operating_income_bn, prior_operating_income_bn, _ = _extract_table_metric(sec_raw, ["Operating income"])
    if operating_income_bn is None:
        operating_income_bn, prior_operating_income_bn = _millions_row_no_pct(sec_flat, "Operating income")
    net_income_bn, prior_net_income_bn, _ = _extract_table_metric(sec_raw, ["Net income"])
    if net_income_bn is None:
        net_income_bn, prior_net_income_bn = _millions_row_no_pct(sec_flat, "Net income")
    gross_margin_pct = _extract_pct_metric(sec_raw, ["Total gross margin percentage"])
    products_margin_pct = _extract_pct_metric(sec_raw, ["Products"])
    services_margin_pct = _extract_pct_metric(sec_raw, ["Services"])
    r_and_d_bn, _, _ = _extract_table_metric(sec_raw, ["Research and development"])
    if r_and_d_bn is None:
        r_and_d_bn, _ = _millions_row_no_pct(sec_flat, "Research and development")
    sga_bn, _, _ = _extract_table_metric(sec_raw, ["Selling, general and administrative"])
    if sga_bn is None:
        sga_bn, _ = _millions_row_no_pct(sec_flat, "Selling, general and administrative")

    eps_match = _search(r"Diluted earnings per share was \$([0-9.]+), up ([0-9]+) percent year over year", release_flat)
    ocf_match = _search(r"generated nearly \$([0-9.]+) billion in operating cash flow", release_flat)
    installed_base_match = _search(r"more than ([0-9.]+) billion active devices", release_flat)
    products_revenue_bn = None
    if revenue_bn is not None and services_bn is not None:
        products_revenue_bn = revenue_bn - services_bn
    products_prior_bn = None
    if prior_revenue_bn is not None and services_bn is not None:
        services_prior = _extract_table_metric(sec_raw, ["Services"])[1]
        if services_prior is not None:
            products_prior_bn = prior_revenue_bn - services_prior
    products_yoy_pct = _pct_change(products_revenue_bn, products_prior_bn)

    segments = _segment_list(
        _segment("iPhone", iphone_bn, iphone_yoy),
        _segment("Services", services_bn, services_yoy),
        _segment("Wearables, Home and Accessories", wearables_bn, wearables_yoy),
        _segment("iPad", ipad_bn, ipad_yoy),
        _segment("Mac", mac_bn, mac_yoy),
    )
    driver = "iPhone 与 Services 同时创下历史新高"
    installed_base_text = ""
    if installed_base_match:
        installed_base_text = f"活跃设备安装基数已超过 {installed_base_match.group(1)}B。"

    facts = {
        "primary_source_label": release["label"],
        "structure_source_label": sec["label"],
        "guidance_source_label": release["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": _pct_change(net_income_bn, prior_net_income_bn),
        "operating_cash_flow_bn": _bn_from_billions(ocf_match.group(1)) if ocf_match else None,
        "gaap_eps": _parse_number(eps_match.group(1)) if eps_match else None,
        "gaap_eps_yoy_pct": _pct_value(eps_match.group(2)) if eps_match else None,
        "segments": segments,
        "driver": driver,
        "guidance": {
            "mode": "official_context",
            "commentary": (
                "公司未给出数值收入指引；管理层强调 iPhone 与 Services 继续刷新纪录，installed base 继续扩张，"
                "Apple Intelligence rollout 仍是后续季度最值得跟踪的中期催化。"
            ),
        },
        "quotes": [
            _quote_card(
                "Tim Cook",
                "Today, Apple is proud to report a remarkable, record-breaking quarter, with revenue of $143.8 billion, up 16 percent from a year ago and well above our expectations.",
                "这句原话直接对应当季收入强于去年同期，且把 iPhone 与 Services 双高景气定义成了本季主叙事。",
                release["label"],
            ),
            _quote_card(
                "Kevan Parekh",
                "These exceptionally strong results generated nearly $54 billion in operating cash flow, allowing us to return almost $32 billion to shareholders.",
                "Apple 不只是交出更高收入与 EPS，还强调了现金创造和股东回报，说明利润兑现质量同样强。",
                release["label"],
            ),
        ],
        "management_theme_items": [
            _theme("Services 高毛利抬升结构", 90, f"Services 收入 {format_money_bn(services_bn, money_symbol)}，毛利率约 {format_pct(services_margin_pct)}。"),
            _theme("安装基数继续扩张", 80, installed_base_text or "管理层继续把 installed base 扩张作为中期生态变现与 AI 分发的基础。"),
        ],
        "qna_theme_items": [
            _theme("iPhone 高端机型需求", 78, f"iPhone 收入 {format_money_bn(iphone_bn, money_symbol)}，同比 {format_pct(iphone_yoy, signed=True)}。"),
            _theme("Apple Intelligence 对换机的兑现时点", 72, "AI 功能更像中期需求催化，市场会继续追问它转化为收入的节奏。"),
        ],
        "risk_items": [
            _theme("硬件周期回落", 66, "若 iPhone 高景气在后续季度回落，收入与利润率弹性都可能同步回落。"),
            _theme("产品与服务 mix 波动", 60, f"Products 毛利率约 {format_pct(products_margin_pct)}，若硬件 mix 转弱，整体利润质量会承压。"),
        ],
        "catalyst_items": [
            _theme("Services 延续高毛利扩张", 86, f"Services 毛利率约 {format_pct(services_margin_pct)}，继续提升会直接强化整体盈利质量。"),
            _theme("installed base 继续增长", 74, installed_base_text or "更大的活跃设备基础能持续支持 Services、支付与 AI 功能的后续变现。"),
        ],
        "income_statement": {
            "subtitle": "利润表页改用 Apple 10-Q 的 Products / Services 与官方费用项。",
            "sources": [
                {"name": "Products", "value_bn": round(products_revenue_bn or 0.0, 3), "yoy_pct": round(products_yoy_pct or 0.0, 1), "margin_pct": products_margin_pct},
                {"name": "Services", "value_bn": round(services_bn or 0.0, 3), "yoy_pct": round(services_yoy or 0.0, 1), "margin_pct": services_margin_pct},
            ],
            "opex_breakdown": [
                {"name": "Research and development", "value_bn": round(r_and_d_bn or 0.0, 3), "pct_of_revenue": round((r_and_d_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#E11D48"},
                {"name": "Selling, general and administrative", "value_bn": round(sga_bn or 0.0, 3), "pct_of_revenue": round((sga_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#F43F5E"},
            ],
            "annotations": [
                {"title": "iPhone 仍是主引擎", "value": f"{format_money_bn(iphone_bn, money_symbol)} | {format_pct(iphone_yoy, signed=True)} YoY", "note": "单品类仍是最大收入来源。", "color": "#111827"},
                {"title": "Services 高毛利", "value": f"{format_money_bn(services_bn, money_symbol)} | {format_pct(services_margin_pct)} margin", "note": "高毛利业务继续提升整体利润质量。", "color": "#2563EB"},
                {"title": "现金流同步兑现", "value": f"{format_money_bn(_bn_from_billions(ocf_match.group(1)) if ocf_match else None, money_symbol)} OCF", "note": "利润增长同时转换成了强现金流。", "color": "#0EA5E9"},
            ],
        },
    }
    return _finalize(company, fallback, facts, materials)


def _parse_apple_dynamic(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing")
    if sec is None:
        return {}

    release_flat = str(release.get("flat_text") or "") if release else ""
    sec_flat = sec["flat_text"]
    release_raw = str(release.get("raw_text") or "") if release else ""
    sec_raw = sec["raw_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, prior_revenue_bn, revenue_yoy_pct = _extract_table_metric(release_raw, ["Total net sales"])
    if revenue_bn is None:
        revenue_bn, prior_revenue_bn, revenue_yoy_pct = _extract_table_metric(sec_raw, ["Total net sales"])
    iphone_bn, _, iphone_yoy = _extract_table_metric(sec_raw, ["iPhone"])
    mac_bn, _, mac_yoy = _extract_table_metric(sec_raw, ["Mac"])
    ipad_bn, _, ipad_yoy = _extract_table_metric(sec_raw, ["iPad"])
    wearables_bn, _, wearables_yoy = _extract_table_metric(sec_raw, ["Wearables, Home and Accessories", "Wearables, Home & Accessories"])
    services_bn, prior_services_bn, services_yoy = _extract_table_metric(sec_raw, ["Services"])
    if iphone_bn is None and services_bn is None and wearables_bn is None:
        return _parse_apple_legacy(company, fallback, materials)

    operating_income_bn, prior_operating_income_bn, _ = _extract_table_metric(release_raw, ["Operating income"])
    if operating_income_bn is None:
        operating_income_bn, prior_operating_income_bn, _ = _extract_table_metric(sec_raw, ["Operating income"])
    net_income_bn, prior_net_income_bn, net_income_yoy_pct = _extract_table_metric(release_raw, ["Net income"])
    if net_income_bn is None:
        net_income_bn, prior_net_income_bn, net_income_yoy_pct = _extract_table_metric(sec_raw, ["Net income"])
    gross_margin_pct = _extract_pct_metric(sec_raw, ["Total gross margin percentage"])
    products_margin_pct = _extract_pct_metric(sec_raw, ["Products"])
    services_margin_pct = _extract_pct_metric(sec_raw, ["Services"])
    r_and_d_bn, _, _ = _extract_table_metric(release_raw, ["Research and development"])
    if r_and_d_bn is None:
        r_and_d_bn, _, _ = _extract_table_metric(sec_raw, ["Research and development"])
    sga_bn, _, _ = _extract_table_metric(release_raw, ["Selling, general and administrative"])
    if sga_bn is None:
        sga_bn, _, _ = _extract_table_metric(sec_raw, ["Selling, general and administrative"])
    total_opex_bn, _, _ = _extract_table_metric(release_raw, ["Total operating expenses"])
    if total_opex_bn is None:
        total_opex_bn, _, _ = _extract_table_metric(sec_raw, ["Total operating expenses"])
    operating_cash_flow_bn, _ = _extract_narrative_metric(
        release_flat,
        r"(?:operating cash flow|cash flow from operations|net cash provided by operating activities)",
    )
    gaap_eps, gaap_eps_yoy_pct = _extract_narrative_eps(release_flat)
    if gaap_eps is None:
        gaap_eps, prior_eps = _per_share_row(sec_flat, "Diluted")
        gaap_eps_yoy_pct = _pct_change(gaap_eps, prior_eps)

    products_revenue_bn = None
    products_prior_bn = None
    products_yoy_pct = None
    if revenue_bn is not None and services_bn is not None:
        products_revenue_bn = revenue_bn - services_bn
    if prior_revenue_bn is not None and prior_services_bn is not None:
        products_prior_bn = prior_revenue_bn - prior_services_bn
    if products_revenue_bn is not None and products_prior_bn not in (None, 0):
        products_yoy_pct = _pct_change(products_revenue_bn, products_prior_bn)

    segments = _segment_list(
        _segment("iPhone", iphone_bn, iphone_yoy),
        _segment("Services", services_bn, services_yoy),
        _segment("Mac", mac_bn, mac_yoy),
        _segment("Wearables, Home and Accessories", wearables_bn, wearables_yoy),
        _segment("iPad", ipad_bn, ipad_yoy),
    )
    geographies = _extract_company_geographies(str(company["id"]), materials, revenue_bn)
    guidance = _extract_generic_guidance(release) if release is not None else {}
    quotes = _extract_quote_cards(release) if release is not None else []
    top_segment = _top_segment(segments)
    fastest_segment = _fastest_segment(segments)
    driver_parts: list[str] = []
    if top_segment:
        driver_parts.append(f"{top_segment['name']} 仍是当季最大收入来源")
    if fastest_segment and fastest_segment.get("yoy_pct") is not None:
        driver_parts.append(f"{fastest_segment['name']} 同比 {format_pct(float(fastest_segment['yoy_pct']), signed=True)}")
    if services_bn is not None and services_margin_pct is not None:
        driver_parts.append(f"Services 毛利率约 {format_pct(services_margin_pct)}")
    driver = "；".join(driver_parts) if driver_parts else "Apple 当季收入与利润表现已切到官方 release / SEC 表格口径。"

    income_sources = []
    if products_revenue_bn is not None:
        income_sources.append(
            {
                "name": "Products",
                "value_bn": round(products_revenue_bn, 3),
                "yoy_pct": round(products_yoy_pct or 0.0, 1) if products_yoy_pct is not None else None,
                "margin_pct": products_margin_pct,
            }
        )
    if services_bn is not None:
        income_sources.append(
            {
                "name": "Services",
                "value_bn": round(services_bn, 3),
                "yoy_pct": round(services_yoy or 0.0, 1) if services_yoy is not None else None,
                "margin_pct": services_margin_pct,
            }
        )

    annotations = []
    if top_segment is not None:
        annotations.append(
            {
                "title": f"{top_segment['name']} 仍是主引擎",
                "value": f"{format_money_bn(float(top_segment['value_bn']), money_symbol)}"
                + (
                    f" | {format_pct(float(top_segment['yoy_pct']), signed=True)} YoY"
                    if top_segment.get("yoy_pct") is not None
                    else ""
                ),
                "note": "头部业务仍是当前季度最直接的阅读锚点。",
                "color": "#111827",
            }
        )
    if services_bn is not None and services_margin_pct is not None:
        annotations.append(
            {
                "title": "Services 高毛利",
                "value": f"{format_money_bn(services_bn, money_symbol)} | {format_pct(services_margin_pct)} margin",
                "note": "高毛利服务收入继续抬升整体利润质量。",
                "color": "#2563EB",
            }
        )
    if operating_cash_flow_bn is not None:
        annotations.append(
            {
                "title": "现金流同步兑现",
                "value": f"{format_money_bn(operating_cash_flow_bn, money_symbol)} OCF",
                "note": "利润改善同步转化为了经营现金流。",
                "color": "#0EA5E9",
            }
        )

    facts = {
        "primary_source_label": release["label"] if release is not None else sec["label"],
        "structure_source_label": sec["label"],
        "guidance_source_label": release["label"] if release is not None else sec["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct if net_income_yoy_pct is not None else _pct_change(net_income_bn, prior_net_income_bn),
        "operating_cash_flow_bn": operating_cash_flow_bn,
        "gaap_eps": gaap_eps,
        "gaap_eps_yoy_pct": gaap_eps_yoy_pct,
        "segments": segments,
        "geographies": geographies,
        "guidance": guidance,
        "quotes": quotes,
        "driver": driver,
        "income_statement": {
            "subtitle": "利润表页按 Apple 官方发布的季度收入、成本与经营费用科目动态生成。",
            "sources": income_sources,
            "opex_breakdown": [
                {
                    "name": "Research and development",
                    "value_bn": round(r_and_d_bn or 0.0, 3),
                    "pct_of_revenue": round((r_and_d_bn or 0.0) / (revenue_bn or 1) * 100, 1),
                    "color": "#E11D48",
                },
                {
                    "name": "Selling, general and administrative",
                    "value_bn": round(sga_bn or 0.0, 3),
                    "pct_of_revenue": round((sga_bn or 0.0) / (revenue_bn or 1) * 100, 1),
                    "color": "#F43F5E",
                },
            ],
            "annotations": annotations[:3],
            "operating_expenses_bn": total_opex_bn,
        },
        "coverage_notes": [
            "Apple 历史季度会优先从官方 earnings release 和 10-Q / 10-K 表格中动态抽取当季收入结构、费用科目与地区结构。"
        ],
    }
    return _finalize(company, fallback, facts, materials)


def _parse_apple_legacy(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    sec = _pick_material(materials, kind="sec_filing")
    if sec is None:
        return {}

    sec_flat = sec["flat_text"]
    sec_raw = sec["raw_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, prior_revenue_bn, revenue_yoy_pct = _extract_table_metric(sec_raw, ["Total net sales"])
    mac_bn, _, mac_yoy = _extract_table_metric(sec_raw, ["Total Macintosh net sales", "Total Mac net sales"])
    ipod_bn, _, ipod_yoy = _extract_table_metric(sec_raw, ["iPod"])
    other_music_bn, _, other_music_yoy = _extract_table_metric(sec_raw, ["Other music related products and services", "Other music related products and services (e)"])
    iphone_bn, _, iphone_yoy = _extract_table_metric(sec_raw, ["iPhone and related products and services", "iPhone and related products and services (f)"])
    ipad_bn, _, ipad_yoy = _extract_table_metric(sec_raw, ["iPad and related products and services", "iPad and related products and services (e)"])
    peripherals_bn, _, peripherals_yoy = _extract_table_metric(sec_raw, ["Peripherals and other hardware", "Peripherals and other hardware (g)"])
    software_services_bn, _, software_services_yoy = _extract_table_metric(
        sec_raw,
        ["Software, service, and other sales", "Software, service and other sales", "Software, service, and other sales (h)"],
    )
    if iphone_bn is None:
        iphone_match = _search(
            r"iPhone and related products and services(?:\s*\(f\))?\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+(?:—|--|-)\s+(?:NM|nm)",
            sec_flat,
        )
        if iphone_match:
            iphone_bn = _bn_from_millions(iphone_match.group(1))
    operating_income_bn, prior_operating_income_bn, _ = _extract_table_metric(sec_raw, ["Operating income"])
    net_income_bn, prior_net_income_bn, net_income_yoy_pct = _extract_table_metric(sec_raw, ["Net income"])
    gross_profit_bn, _, _ = _extract_table_metric(sec_raw, ["Gross margin", "Gross profit"])
    gross_margin_pct = _safe_ratio_pct(gross_profit_bn, revenue_bn)
    gaap_eps, prior_eps = _per_share_row(sec_flat, "Diluted")
    geographies = (
        _apple_legacy_geographies(sec_raw)
        or _apple_legacy_geographies(sec_flat)
        or _apple_legacy_geographies_from_materials(materials, revenue_bn)
        or _extract_company_geographies(str(company["id"]), materials, revenue_bn)
    )

    segments = _segment_list(
        _segment("iPhone and related products and services", iphone_bn, iphone_yoy),
        _segment("Mac", mac_bn, mac_yoy),
        _segment("iPad and related products and services", ipad_bn, ipad_yoy),
        _segment("iPod", ipod_bn, ipod_yoy),
        _segment("Other music related products and services", other_music_bn, other_music_yoy),
        _segment("Peripherals and other hardware", peripherals_bn, peripherals_yoy),
        _segment("Software, service, and other sales", software_services_bn, software_services_yoy),
    )
    top_segment = _top_segment(segments)
    fastest_segment = _fastest_segment(segments)
    driver_parts: list[str] = []
    if top_segment:
        driver_parts.append(f"{top_segment['name']} 是当季最大收入科目")
    if fastest_segment and fastest_segment.get("yoy_pct") is not None:
        driver_parts.append(f"{fastest_segment['name']} 同比 {format_pct(float(fastest_segment['yoy_pct']), signed=True)}")
    if iphone_bn is not None and float(iphone_bn) > 0:
        driver_parts.append("iPhone 仍处早期爬坡阶段")
    driver = "；".join(driver_parts) if driver_parts else "早期 Apple 财报仍以 Mac、iPod 与软件服务收入共同驱动。"

    management_themes = []
    if top_segment is not None:
        management_themes.append(
            _theme(
                "产品收入重心",
                80,
                f"{top_segment['name']} 收入 {format_money_bn(float(top_segment['value_bn']), money_symbol)}，是该季度最主要的业务抓手。",
            )
        )
    if fastest_segment is not None and fastest_segment.get("yoy_pct") is not None:
        management_themes.append(
            _theme(
                "增长弹性来源",
                76,
                f"{fastest_segment['name']} 同比 {format_pct(float(fastest_segment['yoy_pct']), signed=True)}，反映当时产品组合正在发生迁移。",
            )
        )
    if gross_margin_pct is not None:
        management_themes.append(_theme("利润质量", 73, f"按季度 SEC 表口径估算毛利率约 {format_pct(gross_margin_pct)}。"))

    qna_themes = []
    if iphone_bn is not None and float(iphone_bn) > 0:
        qna_themes.append(_theme("iPhone 商业化节奏", 78, f"iPhone 相关收入 {format_money_bn(iphone_bn, money_symbol)}，市场更会关注其渗透与渠道扩张节奏。"))
    if top_segment is not None:
        qna_themes.append(_theme("主力品类持续性", 72, f"{top_segment['name']} 仍占据最大收入体量，后续问答会围绕需求可持续性展开。"))
    if geographies:
        top_geo = max(geographies, key=lambda item: float(item.get("value_bn") or 0.0))
        qna_themes.append(_theme("地区扩张路径", 68, f"当前披露中 {top_geo['name']} 是最大区域收入池，国际扩张节奏值得跟踪。"))

    facts = {
        "primary_source_label": sec["label"],
        "structure_source_label": sec["label"],
        "guidance_source_label": sec["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct if net_income_yoy_pct is not None else _pct_change(net_income_bn, prior_net_income_bn),
        "gaap_eps": gaap_eps,
        "gaap_eps_yoy_pct": _pct_change(gaap_eps, prior_eps),
        "segments": segments,
        "geographies": geographies,
        "driver": driver,
        "guidance": {},
        "management_theme_items": management_themes,
        "qna_theme_items": qna_themes,
        "risk_items": [
            _theme("主力产品集中度", 66, "早期产品线集中度较高，单一主力品类需求波动会更直接传导到季度表现。"),
            _theme("新品爬坡不确定性", 63, "新产品仍处放量初期时，渠道、定价和供给节奏都可能让单季波动放大。"),
        ],
        "catalyst_items": [
            _theme("新品类放量", 74, "若新产品渗透继续提升，结构迁移会先体现在业务 mix，再传导到利润表现。"),
            _theme("国际市场扩张", 70, "地区扩张若顺利，会带来更广泛的收入基础与更高的经营杠杆。"),
        ],
        "coverage_notes": [
            "Apple 早期季度缺少稳定 earnings release 时，系统会改用官方 10-Q 表格原文动态抽取收入、利润和旧版产品结构。"
        ],
    }
    if operating_income_bn is not None and prior_operating_income_bn is not None:
        facts["management_theme_items"].append(
            _theme(
                "经营利润兑现",
                72,
                f"经营利润 {format_money_bn(operating_income_bn, money_symbol)}，同比 {format_pct(_pct_change(operating_income_bn, prior_operating_income_bn), signed=True)}。",
            )
        )
    return _finalize(company, fallback, facts, materials)


def _parse_microsoft(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing")
    if release is None and sec is None:
        return {}

    release_flat = str(release.get("flat_text") or "") if release else ""
    sec_flat = str(sec.get("flat_text") or "") if sec else ""
    sec_raw = str(sec.get("raw_text") or "") if sec else ""
    money_symbol = company["money_symbol"]

    product_revenue_match = _search(r"Revenue:\s+Product\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", sec_flat)
    service_revenue_match = _search(
        r"Revenue:\s+Product\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+Service and other\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)",
        sec_flat,
    )
    total_revenue_bn, prior_total_revenue_bn = _millions_row_no_pct(sec_flat, "Total revenue")
    product_cost_match = _search(r"Cost of revenue:\s+Product\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", sec_flat)
    service_cost_match = _search(
        r"Cost of revenue:\s+Product\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+\$?\s*[0-9,]+\s+Service and other\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)",
        sec_flat,
    )
    total_cost_of_revenue_bn, _ = _millions_row_no_pct(sec_flat, "Total cost of revenue")
    gross_margin_bn, _ = _millions_row_no_pct(sec_flat, "Gross margin")
    r_and_d_bn, _ = _millions_row_no_pct(sec_flat, "Research and development")
    sales_marketing_bn, _ = _millions_row_no_pct(sec_flat, "Sales and marketing")
    ga_bn, _ = _millions_row_no_pct(sec_flat, "General and administrative")
    operating_income_bn, prior_operating_income_bn = _millions_row_no_pct(sec_flat, "Operating income")

    revenue_match = _search(r"Revenue was \$([0-9.]+) billion and increased ([0-9]+)%", release_flat)
    net_income_match = _search(
        r"Net income on a GAAP basis was \$([0-9.]+)\s*billion and increased ([0-9]+)%, and on a non-GAAP basis was \$([0-9.]+)\s*billion and increased ([0-9]+)%",
        release_flat,
    )
    eps_match = _search(
        r"Diluted earnings per share on a GAAP basis was \$([0-9.]+) and increased ([0-9]+)%, and on a non-GAAP basis was \$([0-9.]+) and increased ([0-9]+)%",
        release_flat,
    )
    cloud_match = _search(r"Microsoft Cloud revenue was \$([0-9.]+) billion and increased ([0-9]+)%", release_flat)
    pbp_match = _search(
        r"Revenue in Productivity and Business Processes was \$([0-9.]+) billion and (?:(increased|decreased) ([0-9]+)%|(was relatively unchanged))",
        release_flat,
    )
    ic_match = _search(
        r"Revenue in Intelligent Cloud was \$([0-9.]+) billion and (?:(increased|decreased) ([0-9]+)%|(was relatively unchanged))",
        release_flat,
    )
    mpc_match = _search(
        r"Revenue in More Personal Computing was \$([0-9.]+) billion and (?:(increased|decreased) ([0-9]+)%|(was relatively unchanged))",
        release_flat,
    )
    azure_match = _search(r"Azure and other cloud services revenue increased ([0-9]+)%", release_flat)
    gross_margin_pct = None
    product_revenue_bn = _bn_from_millions(product_revenue_match.group(1)) if product_revenue_match else None
    prior_product_revenue_bn = _bn_from_millions(product_revenue_match.group(2)) if product_revenue_match else None
    service_revenue_bn = _bn_from_millions(service_revenue_match.group(1)) if service_revenue_match else None
    prior_service_revenue_bn = _bn_from_millions(service_revenue_match.group(2)) if service_revenue_match else None
    product_cost_bn = _bn_from_millions(product_cost_match.group(1)) if product_cost_match else None
    service_cost_bn = _bn_from_millions(service_cost_match.group(1)) if service_cost_match else None
    geographies = _microsoft_geographies(sec_flat) if sec_flat else []
    if total_revenue_bn is not None and total_cost_of_revenue_bn is not None:
        gross_margin_pct = (float(total_revenue_bn) - float(total_cost_of_revenue_bn)) / float(total_revenue_bn) * 100

    segments = _segment_list(
        _segment(
            "Productivity and Business Processes",
            _bn_from_billions(pbp_match.group(1)) if pbp_match else None,
            _directional_pct(pbp_match.group(2) or pbp_match.group(4), pbp_match.group(3)) if pbp_match else None,
        ),
        _segment(
            "Intelligent Cloud",
            _bn_from_billions(ic_match.group(1)) if ic_match else None,
            _directional_pct(ic_match.group(2) or ic_match.group(4), ic_match.group(3)) if ic_match else None,
        ),
        _segment(
            "More Personal Computing",
            _bn_from_billions(mpc_match.group(1)) if mpc_match else None,
            _directional_pct(mpc_match.group(2) or mpc_match.group(4), mpc_match.group(3)) if mpc_match else None,
        ),
    )
    if not segments:
        segments = _extract_company_segments(str(company["id"]), materials, revenue_bn)
    segment_margin_matches = {
        "Productivity and Business Processes": _search(
            r"Productivity and Business Processes\s+Revenue\s+\$?\s*([0-9,]+).*?Operating income\s+\$?\s*([0-9,]+)",
            sec_raw,
        ),
        "Intelligent Cloud": _search(
            r"Intelligent Cloud\s+Revenue\s+\$?\s*([0-9,]+).*?Operating income\s+\$?\s*([0-9,]+)",
            sec_raw,
        ),
        "More Personal Computing": _search(
            r"More Personal Computing\s+Revenue\s+\$?\s*([0-9,]+).*?Operating income\s+\$?\s*([0-9,]+)",
            sec_raw,
        ),
    }
    for segment in segments:
        match = segment_margin_matches.get(str(segment.get("name") or ""))
        if match is None:
            continue
        segment_revenue_bn = _bn_from_millions(match.group(1))
        segment_operating_income_bn = _bn_from_millions(match.group(2))
        if segment_revenue_bn not in (None, 0) and segment_operating_income_bn is not None:
            segment["operating_income_bn"] = round(float(segment_operating_income_bn), 3)
            segment["margin_pct"] = round(float(segment_operating_income_bn) / float(segment_revenue_bn) * 100, 1)

    service_margin_pct = None
    product_margin_pct = None
    if product_revenue_bn not in (None, 0) and product_cost_bn is not None:
        product_margin_pct = (float(product_revenue_bn) - float(product_cost_bn)) / float(product_revenue_bn) * 100
    if service_revenue_bn not in (None, 0) and service_cost_bn is not None:
        service_margin_pct = (float(service_revenue_bn) - float(service_cost_bn)) / float(service_revenue_bn) * 100

    primary_label = release["label"] if release is not None else sec["label"]
    structure_label = release["label"] if release is not None else sec["label"]
    guidance = _extract_generic_guidance(release or sec) if (release or sec) is not None else {}
    quotes = _extract_quote_cards(release) or _extract_quote_cards(sec)
    top_segment = _top_segment(segments)
    fastest_segment = _fastest_segment(segments)
    driver_parts: list[str] = []
    if top_segment is not None:
        driver_parts.append(f"{top_segment['name']} 仍是核心收入支柱")
    if cloud_match is not None:
        driver_parts.append(f"Microsoft Cloud 达到 {format_money_bn(_bn_from_billions(cloud_match.group(1)), money_symbol)}")
    if fastest_segment is not None and fastest_segment.get("yoy_pct") is not None:
        driver_parts.append(f"{fastest_segment['name']} 同比 {format_pct(float(fastest_segment['yoy_pct']), signed=True)}")
    driver = "；".join(driver_parts) if driver_parts else "Azure、Microsoft Cloud 与企业软件仍在同向放大 AI 变现"

    income_sources: list[dict[str, Any]] = []
    if product_revenue_bn is not None:
        income_sources.append(
            {
                "name": "Product revenue",
                "value_bn": round(product_revenue_bn, 3),
                "yoy_pct": round(_pct_change(product_revenue_bn, prior_product_revenue_bn) or 0.0, 1) if prior_product_revenue_bn is not None else None,
                "margin_pct": product_margin_pct,
            }
        )
    if service_revenue_bn is not None:
        income_sources.append(
            {
                "name": "Service and other revenue",
                "value_bn": round(service_revenue_bn, 3),
                "yoy_pct": round(_pct_change(service_revenue_bn, prior_service_revenue_bn) or 0.0, 1) if prior_service_revenue_bn is not None else None,
                "margin_pct": service_margin_pct,
            }
        )
    if not income_sources:
        income_sources = list(segments)

    facts = {
        "primary_source_label": primary_label,
        "structure_source_label": structure_label,
        "guidance_source_label": primary_label,
        "revenue_bn": total_revenue_bn or (_bn_from_billions(revenue_match.group(1)) if revenue_match else None),
        "revenue_yoy_pct": _pct_value(revenue_match.group(2)) if revenue_match else _pct_change(total_revenue_bn, prior_total_revenue_bn),
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn or (_bn_from_billions(_search(r"Operating income was \$([0-9.]+) billion", release_flat).group(1)) if _search(r"Operating income was \$([0-9.]+) billion", release_flat) else None),
        "net_income_bn": _bn_from_billions(net_income_match.group(1)) if net_income_match else None,
        "net_income_yoy_pct": _pct_value(net_income_match.group(2)) if net_income_match else None,
        "gaap_eps": _parse_number(eps_match.group(1)) if eps_match else None,
        "gaap_eps_yoy_pct": _pct_value(eps_match.group(2)) if eps_match else None,
        "non_gaap_eps": _parse_number(eps_match.group(3)) if eps_match else None,
        "segments": segments,
        "geographies": geographies,
        "driver": driver,
        "guidance": guidance or {
            "mode": "official_context",
            "commentary": (
                "公司本次未给出 consolidated revenue 数值指引；但管理层持续强调云与 AI 仍处扩散初期，"
                "Microsoft Cloud 与 Azure 仍是最重要的兑现锚点。"
            ),
        },
        "quotes": quotes or (
            [
                _quote_card(
                    "Satya Nadella",
                    "We are innovating across every layer of our differentiated technology stack and leading in key secular areas that are critical to our customers' success.",
                    "即使旧年份口径下，管理层也把平台层创新和企业客户价值作为季度主线。",
                    primary_label,
                )
            ]
            if release is not None
            else []
        ),
        "management_theme_items": [
            _theme("Azure 与 AI 供需共振", 92, f"Azure 及其他云服务收入同比增长 {format_pct(_pct_value(azure_match.group(1)) if azure_match else None, signed=True)}。"),
            _theme("Microsoft Cloud 体量上台阶", 86, f"Microsoft Cloud 单季收入 {format_money_bn(_bn_from_billions(cloud_match.group(1)) if cloud_match else None, money_symbol)}。"),
        ],
        "qna_theme_items": [
            _theme("AI 收入兑现节奏", 80, "市场会继续追问 AI 业务扩张是更多来自推理、平台还是 Copilot 货币化。"),
            _theme("云业务利润率", 74, f"总毛利率约 {format_pct(gross_margin_pct)}，同时云产品 mix 继续向服务收入倾斜。"),
        ],
        "risk_items": [
            _theme("AI 基建投入压力", 64, "云与 AI 需求强，但更高的算力和基础设施投入也会持续影响利润率节奏。"),
            _theme(
                "More Personal Computing 承压",
                58,
                f"More Personal Computing 收入同比 {format_pct(_directional_pct(mpc_match.group(2) or mpc_match.group(4), mpc_match.group(3)) if mpc_match else None, signed=True)}。",
            ),
        ],
        "catalyst_items": [
            _theme("Azure 保持高增", 88, f"Azure 及其他云服务继续保持接近 40% 的增长，是最直接的上修催化。"),
            _theme("服务收入占比继续提升", 74, f"Service and other revenue 已达 {format_money_bn(service_revenue_bn, money_symbol)}。"),
        ],
        "income_statement": {
            "subtitle": "利润表页改用业务集团桥接收入，并向下串联 Microsoft 官方成本与经营费用科目。",
            "sources": income_sources,
            "opex_breakdown": [
                {"name": "Research and development", "value_bn": round(r_and_d_bn or 0.0, 3), "pct_of_revenue": round((r_and_d_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#E11D48"},
                {"name": "Sales and marketing", "value_bn": round(sales_marketing_bn or 0.0, 3), "pct_of_revenue": round((sales_marketing_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#F43F5E"},
                {"name": "General and administrative", "value_bn": round(ga_bn or 0.0, 3), "pct_of_revenue": round((ga_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#FB7185"},
            ],
            "annotations": [
                {"title": "Microsoft Cloud 突破 50B", "value": f"{format_money_bn(_bn_from_billions(cloud_match.group(1)) if cloud_match else None, money_symbol)}", "note": "云业务体量再上台阶，AI 需求仍在放大。", "color": "#0078D4"},
                {"title": "Azure 高增", "value": f"{format_pct(_pct_value(azure_match.group(1)) if azure_match else None, signed=True)} YoY", "note": "Azure 仍是最核心的增长驱动。", "color": "#2563EB"},
                {"title": "服务收入主导", "value": f"{format_money_bn(service_revenue_bn, money_symbol)}", "note": "更高占比的服务收入继续提升收入质量。", "color": "#0EA5E9"},
            ],
        },
    }
    return _finalize(company, fallback, facts, materials)


def _alphabet_historical_segment(
    flat_text: str,
    label: str,
    canonical_name: str,
) -> Optional[dict[str, Any]]:
    pattern = rf"{re.escape(label)}(?:\s*\(\d+\))?\s+\$?\s*\(?([0-9,]+(?:\.[0-9]+)?)\)?\s+\$?\s*\(?([0-9,]+(?:\.[0-9]+)?)\)?"
    match = _search(pattern, flat_text)
    if not match:
        return None
    prior_bn = _bn_from_millions(match.group(1))
    current_bn = _bn_from_millions(match.group(2))
    if current_bn is None:
        return None
    return _segment(canonical_name, current_bn, _pct_change(current_bn, prior_bn))


def _parse_alphabet_historical(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    release = _pick_material(materials, kind="official_release") or _pick_material(materials, kind="call_summary")
    sec = _pick_material(materials, kind="sec_filing")
    primary = release or sec
    if primary is None:
        return parsed

    flat = primary["flat_text"]
    money_symbol = company["money_symbol"]
    latest_kpis = dict(parsed.get("latest_kpis") or {})
    segment_revenue_bn = _coalesce_number(
        dict(fallback.get("latest_kpis") or {}).get("revenue_bn"),
        latest_kpis.get("revenue_bn"),
    )
    detailed_segments = _segment_list(
        _alphabet_historical_segment(flat, "Google Search & other", "Google Search & other"),
        _alphabet_historical_segment(flat, "YouTube ads", "YouTube ads"),
        _alphabet_historical_segment(flat, "Google Network Members' properties", "Google Network"),
        _alphabet_historical_segment(flat, "Google Cloud", "Google Cloud"),
        _alphabet_historical_segment(flat, "Google other", "Google subscriptions, platforms, and devices"),
        _alphabet_historical_segment(flat, "Other Bets revenues", "Other Bets"),
    )
    legacy_segments = _segment_list(
        _alphabet_historical_segment(flat, "Google properties revenues", "Google properties"),
        _alphabet_historical_segment(flat, "Google Network Members' properties revenues", "Google Network"),
        _alphabet_historical_segment(flat, "Google other revenues", "Google other"),
        _alphabet_historical_segment(flat, "Other Bets revenues", "Other Bets"),
    )
    segments = detailed_segments
    if not _segments_reasonable_for_revenue(segments, segment_revenue_bn):
        if _segments_reasonable_for_revenue(legacy_segments, segment_revenue_bn):
            segments = legacy_segments
        elif len(legacy_segments) > len(segments):
            segments = legacy_segments
    if not segments:
        segments = legacy_segments
    segments = segments or list(parsed.get("current_segments") or []) or _extract_company_segments(str(company["id"]), materials, revenue_bn)
    if not segments:
        return parsed

    segment_revenue_bn = sum(float(item.get("value_bn") or 0.0) for item in segments) or None
    revenue_bn = _coalesce_number(
        dict(fallback.get("latest_kpis") or {}).get("revenue_bn"),
        segment_revenue_bn,
        latest_kpis.get("revenue_bn"),
    )
    def _historical_geo_row(label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
        # Newer expanded disclosure: prior-year, current quarter, YoY growth.
        modern_match = _search(
            rf"{re.escape(label)} revenues(?:\s+\(GAAP\))?\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+([0-9]+(?:\.[0-9]+)?)\s*%",
            flat,
        )
        if modern_match:
            return (
                _bn_from_millions(modern_match.group(2)),
                _bn_from_millions(modern_match.group(1)),
                _pct_value(modern_match.group(3)),
            )

        # Older disclosure splits YoY and QoQ side by side; use the first GAAP / prior-period / YoY lane.
        legacy_match = _search(
            rf"{re.escape(label)} revenues \(GAAP\)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\b.*?"
            rf"Prior period {re.escape(label)} revenues \(GAAP\)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\b.*?"
            rf"{re.escape(label)} revenue (?:growth|percentage change) \(GAAP\)\s+([0-9]+(?:\.[0-9]+)?)\s*%",
            flat,
        )
        if legacy_match:
            return (
                _bn_from_millions(legacy_match.group(1)),
                _bn_from_millions(legacy_match.group(2)),
                _pct_value(legacy_match.group(3)),
            )
        if label == "United States":
            us_match = _search(
                r"United States revenues(?:\s+\(GAAP\))?\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)"
                r"(?:\s+\$?\s*([0-9,]+(?:\.[0-9]+)?))?.*?"
                r"United States revenue(?: growth| percentage change) \(GAAP\)\s+([0-9]+(?:\.[0-9]+)?)\s*%",
                flat,
            )
            if us_match:
                current_bn = _bn_from_millions(us_match.group(1))
                yoy_pct = _pct_value(us_match.group(3))
                prior_bn = (
                    None
                    if current_bn is None or yoy_pct is None or yoy_pct <= -100
                    else float(current_bn) / (1 + float(yoy_pct) / 100)
                )
                return (current_bn, prior_bn, yoy_pct)
        return (None, None, None)

    current_us_match = _search(r"United States revenues \(GAAP\)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)", flat)
    current_geographies: list[dict[str, Any]] = list(parsed.get("current_geographies") or [])
    detailed_geo_rows = [
        ("United States",) + _historical_geo_row("United States"),
        ("EMEA",) + _historical_geo_row("EMEA"),
        ("Asia Pacific",) + _historical_geo_row("APAC"),
        ("Americas Excluding U.S.",) + _historical_geo_row("Other Americas"),
    ]
    detailed_geographies = _geography_list(
        *[
            _geography(name, current_bn, yoy_pct if yoy_pct is not None else _pct_change(current_bn, prior_bn))
            for name, current_bn, prior_bn, yoy_pct in detailed_geo_rows
            if current_bn is not None
        ]
    )
    if len(detailed_geographies) >= 3 and _segments_reasonable_for_revenue(detailed_geographies, revenue_bn):
        current_geographies = detailed_geographies
    elif current_us_match and revenue_bn not in (None, 0):
        prior_us_bn = _bn_from_millions(current_us_match.group(1))
        current_us_bn = _bn_from_millions(current_us_match.group(2))
        if current_us_bn is not None:
            international_bn = max(float(revenue_bn) - float(current_us_bn), 0.0)
            prior_total_match = _search(r"Total revenues\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)", flat)
            prior_total_bn = _bn_from_millions(prior_total_match.group(1)) if prior_total_match else None
            prior_international_bn = (
                max(float(prior_total_bn) - float(prior_us_bn), 0.0)
                if prior_total_bn is not None and prior_us_bn is not None
                else None
            )
            current_geographies = _geography_list(
                _geography("United States", current_us_bn, _pct_change(current_us_bn, prior_us_bn)),
                _geography("International", international_bn, _pct_change(international_bn, prior_international_bn)),
            )

    top_segment = max(segments, key=lambda item: float(item.get("value_bn") or 0.0))
    cloud = next((item for item in segments if str(item.get("name") or "") == "Google Cloud"), None)
    youtube = next((item for item in segments if str(item.get("name") or "") == "YouTube ads"), None)
    qna_items = [
        _theme(
            "Cloud 持续放量",
            82,
            (
                f"Google Cloud 收入 {format_money_bn(float(cloud['value_bn']), money_symbol)}，"
                f"同比 {format_pct(float(cloud['yoy_pct']), signed=True)}。"
            )
            if cloud and cloud.get("yoy_pct") is not None
            else "Google Cloud 仍处在较早的放量阶段，后续问答会持续围绕订单兑现与利润率路径展开。",
        ),
        _theme(
            "广告主业务韧性",
            76,
            f"{top_segment['name']} 仍是最大收入池，问答重点会围绕广告 ROI 与需求韧性展开。",
        ),
    ]
    if youtube and youtube.get("yoy_pct") is not None:
        qna_items.append(
            _theme(
                "YouTube 商业化进度",
                72,
                f"YouTube ads 收入 {format_money_bn(float(youtube['value_bn']), money_symbol)}，同比 {format_pct(float(youtube['yoy_pct']), signed=True)}。",
            )
        )

    updates = _clean_mapping(
        {
            "current_segments": segments,
            "current_geographies": current_geographies,
            "call_quote_cards": _extract_quote_cards(release or sec),
            "management_themes": [
                _theme(
                    "搜索仍是主引擎",
                    84,
                    f"{top_segment['name']} 收入 {format_money_bn(float(top_segment['value_bn']), money_symbol)}，仍是本季最大收入锚点。",
                ),
                _theme(
                    "Cloud 进入加速通道",
                    82,
                    (
                        f"Google Cloud 收入 {format_money_bn(float(cloud['value_bn']), money_symbol)}，"
                        f"同比 {format_pct(float(cloud['yoy_pct']), signed=True)}。"
                    )
                    if cloud and cloud.get("yoy_pct") is not None
                    else "Google Cloud 继续保持高于公司整体的增长弹性。 ",
                ),
                _theme(
                    "粒度披露开始提升",
                    74,
                    "公司在这一时期开始提供更细的 Search、YouTube ads 与 Cloud 收入披露，结构可读性明显提升。",
                ),
            ],
            "qna_themes": qna_items,
            "risks": [
                _theme("广告周期波动", 66, "广告业务仍占据最大收入比重，宏观投放与流量变现效率波动会更直接传导到季度结果。"),
                _theme("Cloud 规模尚早", 60, "Cloud 仍在扩张期，市场会持续关注增长能否稳定兑现为利润贡献。"),
            ],
            "catalysts": [
                _theme("Cloud 保持高增", 82, "若 Cloud 延续高增，将持续改善 Alphabet 的第二增长曲线质量。"),
                _theme("YouTube 广告扩容", 74, "YouTube ads 已具备独立披露口径，后续商业化提升会更容易被资本市场单独定价。"),
            ],
            "income_statement": {
                **dict(parsed.get("income_statement") or {}),
                "sources": segments,
                "annotations": [
                    {
                        "title": "Search 仍是最大引擎",
                        "value": format_money_bn(float(top_segment["value_bn"]), money_symbol),
                        "note": "搜索广告仍然构成最主要的收入底盘。",
                        "color": "#1A73E8",
                    },
                    {
                        "title": "Cloud 进入加速区",
                        "value": format_money_bn(float(cloud["value_bn"]), money_symbol) if cloud else "-",
                        "note": "Cloud 开始成为更具辨识度的第二增长曲线。",
                        "color": "#0F9D58",
                    },
                    {
                        "title": "YouTube 广告独立披露",
                        "value": format_money_bn(float(youtube["value_bn"]), money_symbol) if youtube else "-",
                        "note": "管理层开始单独展示 YouTube 广告规模，提升结构透明度。",
                        "color": "#EA4335",
                    },
                ],
            },
            "coverage_notes": [
                "Alphabet 较早季度会优先从官方 earnings release 的 Q4 / full-year expanded disclosure 表中动态抽取 Search、YouTube、Cloud 等收入行。"
                + (
                    " 地区结构若已披露 EMEA / APAC / Other Americas / United States，也会优先沿该官方细分口径展示。"
                    if current_geographies and len(current_geographies) >= 3
                    else ""
                )
            ]
            + (
                ["若当季仍处于旧披露阶段，业务结构会自动切换到 Google properties / Google Network / Google other / Other Bets 官方口径。"] 
                if any(str(item.get("name") or "") == "Google properties" for item in segments)
                else []
            )
            + (
                ["若 release 页面暂未稳定发现，系统会退回到 SEC filing 与通用分部抽取逻辑，优先维持业务口径连续性。"]
                if release is None
                else []
            ),
        }
    )
    return _merge_parsed_payload(updates, parsed)


def _parse_alphabet(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    if _quarter_is_before(fallback, "2024Q1"):
        return _parse_alphabet_historical(company, fallback, materials)
    transcript = _pick_material(materials, kind="call_summary") or _pick_material(materials, kind="official_release")
    if transcript is None:
        return {}

    flat = transcript["flat_text"]
    money_symbol = company["money_symbol"]

    revenue_match = _search(r"Consolidated revenues reached \$([0-9.]+) billion, up ([0-9]+)%", flat)
    cost_revenue_match = _search(r"Total Cost of Revenue was \$([0-9.]+) billion, up ([0-9]+)%", flat)
    operating_income_match = _search(r"Operating income increased ([0-9]+)% to \$([0-9.]+) billion", flat)
    net_income_match = _search(r"Net income increased ([0-9]+)% to \$([0-9.]+) billion, and earnings per share increased ([0-9]+)% to \$([0-9.]+)", flat)
    ocf_match = _search(r"record operating cash flow of \$([0-9.]+) billion in the fourth quarter", flat)
    fcf_match = _search(r"\$([0-9.]+) billion of free cashflow in the fourth quarter", flat)
    cash_match = _search(r"\$([0-9.]+) billion in cash and marketable securities", flat)
    search_match = _search(r"Google Search and Other advertising revenues increased by ([0-9]+)% to \$([0-9.]+) billion", flat)
    youtube_match = _search(r"YouTube advertising revenues increased ([0-9]+)% to \$([0-9.]+) billion", flat)
    network_match = _search(r"Network advertising revenues of \$([0-9.]+) billion were down ([0-9]+)%", flat)
    subs_match = _search(r"Subscriptions, Platforms and Devices revenues increased ([0-9]+)% this quarter to \$([0-9.]+) billion", flat)
    cloud_match = _search(r"Cloud revenue accelerated meaningfully and was up ([0-9]+)% to \$([0-9.]+) billion", flat)
    other_bets_match = _search(r"In Other Bets, revenues were \$([0-9.]+) million", flat)
    capex_match = _search(r"CapEx to be in the range of \$([0-9.]+)\s*billion to \$([0-9.]+)\s*billion", flat)

    revenue_bn = _bn_from_billions(revenue_match.group(1)) if revenue_match else None
    cost_revenue_bn = _bn_from_billions(cost_revenue_match.group(1)) if cost_revenue_match else None
    gross_margin_pct = None
    if revenue_bn not in (None, 0) and cost_revenue_bn is not None:
        gross_margin_pct = (float(revenue_bn) - float(cost_revenue_bn)) / float(revenue_bn) * 100

    segments = _segment_list(
        _segment("Google Search & other", _bn_from_billions(search_match.group(2)) if search_match else None, _pct_value(search_match.group(1)) if search_match else None),
        _segment("YouTube ads", _bn_from_billions(youtube_match.group(2)) if youtube_match else None, _pct_value(youtube_match.group(1)) if youtube_match else None),
        _segment("Google Network", _bn_from_billions(network_match.group(1)) if network_match else None, -_pct_value(network_match.group(2)) if network_match else None),
        _segment("Google subscriptions, platforms, and devices", _bn_from_billions(subs_match.group(2)) if subs_match else None, _pct_value(subs_match.group(1)) if subs_match else None),
        _segment("Google Cloud", _bn_from_billions(cloud_match.group(2)) if cloud_match else None, _pct_value(cloud_match.group(1)) if cloud_match else None),
        _segment("Other Bets", _bn_from_millions(other_bets_match.group(1)) if other_bets_match else None, None),
    )

    guidance_commentary = (
        "公司未给出下一季收入数值指引，但明确表示 Google Services 会继续受益于 AI 创新与广告 ROI 改善，"
        "Google Cloud 仍处在强需求与紧供给并存阶段；2026 年 CapEx 预计达到 "
        f"{format_money_bn(_bn_from_billions(capex_match.group(1)) if capex_match else None)} 到 {format_money_bn(_bn_from_billions(capex_match.group(2)) if capex_match else None)}。"
    )

    facts = {
        "primary_source_label": transcript["label"],
        "structure_source_label": transcript["label"],
        "guidance_source_label": transcript["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": _pct_value(revenue_match.group(2)) if revenue_match else None,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": _bn_from_billions(operating_income_match.group(2)) if operating_income_match else None,
        "net_income_bn": _bn_from_billions(net_income_match.group(2)) if net_income_match else None,
        "net_income_yoy_pct": _pct_value(net_income_match.group(1)) if net_income_match else None,
        "operating_cash_flow_bn": _bn_from_billions(ocf_match.group(1)) if ocf_match else None,
        "free_cash_flow_bn": _bn_from_billions(fcf_match.group(1)) if fcf_match else None,
        "gaap_eps": _parse_number(net_income_match.group(4)) if net_income_match else None,
        "gaap_eps_yoy_pct": _pct_value(net_income_match.group(3)) if net_income_match else None,
        "segments": segments,
        "driver": "Search 与 Cloud 加速增长，AI 投入正开始系统性转化为收入与 backlog",
        "guidance": {
            "mode": "official_context",
            "commentary": guidance_commentary,
        },
        "quotes": [
            _quote_card(
                "Sundar Pichai",
                "It was a tremendous quarter for Alphabet. The launch of Gemini 3 was a major milestone and we have great momentum.",
                "管理层把季度亮点直接落在 Gemini 3 与整体动能上，意味着 AI 投入已经是公司级增长主线。",
                transcript["label"],
            ),
            _quote_card(
                "Anat Ashkenazi",
                "Cloud revenue accelerated meaningfully and was up 48% to $17.7 billion.",
                "CFO 在电话会里直接强调 Cloud 加速与 enterprise AI 需求，是当前季度结构升级最清楚的证据之一。",
                transcript["label"],
            ),
        ],
        "management_theme_items": [
            _theme("Google Cloud 明显加速", 92, f"Google Cloud 收入 {format_money_bn(_bn_from_billions(cloud_match.group(2)) if cloud_match else None, money_symbol)}，同比 {format_pct(_pct_value(cloud_match.group(1)) if cloud_match else None, signed=True)}。"),
            _theme("Search 广告韧性", 84, f"Search & Other 广告收入 {format_money_bn(_bn_from_billions(search_match.group(2)) if search_match else None, money_symbol)}，同比 {format_pct(_pct_value(search_match.group(1)) if search_match else None, signed=True)}。"),
        ],
        "qna_theme_items": [
            _theme("Cloud 供给约束何时缓解", 80, "管理层明确提到 strong demand 与 tight supply 并存，供给兑现节奏仍是关键问题。"),
            _theme("AI 投入对利润率的影响", 72, guidance_commentary),
        ],
        "risk_items": [
            _theme("AI 投入推高折旧与费用", 68, "更高 CapEx 会继续推升折旧和数据中心运营成本，短期会对利润率形成压力。"),
            _theme("广告季节性与结构波动", 58, "Google Services 仍受广告季节性和宏观投放节奏影响。"),
        ],
        "catalyst_items": [
            _theme("Cloud backlog 扩张", 86, "Google Cloud backlog 已达到 240B 美元，意味着未来几个季度仍有较强的已签需求支撑。"),
            _theme("AI 提升广告 ROI", 74, "管理层明确提到 AI 正在改善广告 ROI，这会帮助 Services 端维持更高质量增长。"),
        ],
        "income_statement": {
            "subtitle": "利润表页改用电话会中的业务收入与成本/资本开支口径，展示 AI 投入与经营结果的对应关系。",
            "sources": segments,
            "annotations": [
                {"title": "Cloud 强劲加速", "value": f"{format_money_bn(_bn_from_billions(cloud_match.group(2)) if cloud_match else None, money_symbol)} | {format_pct(_pct_value(cloud_match.group(1)) if cloud_match else None, signed=True)} YoY", "note": "企业 AI 需求与 backlog 同时上行。", "color": "#1A73E8"},
                {"title": "Search 仍是最大引擎", "value": f"{format_money_bn(_bn_from_billions(search_match.group(2)) if search_match else None, money_symbol)}", "note": "Search 仍然是最大收入池，同时继续加速。", "color": "#2563EB"},
                {"title": "CapEx 继续上台阶", "value": f"{format_money_bn(_bn_from_billions(capex_match.group(1)) if capex_match else None)}-{format_money_bn(_bn_from_billions(capex_match.group(2)) if capex_match else None)}", "note": "2026 年投入强度继续明显提升。", "color": "#0F9D58"},
            ],
        },
    }
    return _finalize(company, fallback, facts, materials)


def _amazon_historical_segment(
    flat_text: str,
    label: str,
) -> Optional[dict[str, Any]]:
    pattern = (
        rf"{re.escape(label)}\s+Net sales\s+\$?\s*\(?([0-9,]+(?:\.[0-9]+)?)\)?\s+\$?\s*\(?([0-9,]+(?:\.[0-9]+)?)\)?"
        rf"\s+\$?\s*\(?([0-9,]+(?:\.[0-9]+)?)\)?\s+\$?\s*\(?([0-9,]+(?:\.[0-9]+)?)\)?"
    )
    match = _search(pattern, flat_text)
    if not match:
        return None
    prior_bn = _bn_from_millions(match.group(1))
    current_bn = _bn_from_millions(match.group(2))
    if current_bn is None:
        return None
    return _segment(label, current_bn, _pct_change(current_bn, prior_bn))


def _parse_amazon_historical(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    release = _pick_material(materials, kind="official_release")
    if release is None:
        return parsed

    flat = release["flat_text"]
    money_symbol = company["money_symbol"]
    segments = _segment_list(
        _amazon_historical_segment(flat, "North America"),
        _amazon_historical_segment(flat, "International"),
        _amazon_historical_segment(flat, "AWS"),
    )
    if not segments:
        return parsed

    aws = next((item for item in segments if str(item.get("name") or "") == "AWS"), None)
    north_america = next((item for item in segments if str(item.get("name") or "") == "North America"), None)
    international = next((item for item in segments if str(item.get("name") or "") == "International"), None)
    guidance = dict(parsed.get("guidance") or {})

    updates = _clean_mapping(
        {
            "current_segments": segments,
            "management_themes": [
                _theme(
                    "AWS 加速扩张",
                    88,
                    (
                        f"AWS 收入 {format_money_bn(float(aws['value_bn']), money_symbol)}，"
                        f"同比 {format_pct(float(aws['yoy_pct']), signed=True)}。"
                    )
                    if aws and aws.get("yoy_pct") is not None
                    else "AWS 仍是 Amazon 最具盈利质量的业务引擎。",
                ),
                _theme(
                    "北美零售底盘稳固",
                    80,
                    (
                        f"North America 收入 {format_money_bn(float(north_america['value_bn']), money_symbol)}，"
                        f"同比 {format_pct(float(north_america['yoy_pct']), signed=True)}。"
                    )
                    if north_america and north_america.get("yoy_pct") is not None
                    else "北美零售仍是当季最大收入基础盘。",
                ),
                _theme(
                    "国际业务仍在修复",
                    72,
                    (
                        f"International 收入 {format_money_bn(float(international['value_bn']), money_symbol)}，"
                        f"同比 {format_pct(float(international['yoy_pct']), signed=True)}。"
                    )
                    if international and international.get("yoy_pct") is not None
                    else "国际业务改善速度仍将影响整体经营杠杆。 ",
                ),
            ],
            "qna_themes": [
                _theme(
                    "AWS 增长持续性",
                    80,
                    (
                        f"AWS 已增长至 {format_money_bn(float(aws['value_bn']), money_symbol)}，"
                        "后续问答会继续围绕云业务需求与利润率路径展开。"
                    )
                    if aws
                    else "AWS 仍是最关键的问答主题。",
                ),
                _theme(
                    "零售利润率兑现",
                    74,
                    "北美和国际零售的收入增速与履约、技术投入之间如何平衡，仍是当季最核心的利润率问题。",
                ),
            ],
            "risks": [
                _theme("高投入压制利润弹性", 66, "物流、内容和技术投入继续偏高时，零售业务利润率修复会慢于收入增长。"),
                _theme("国际业务波动", 60, "国际业务的利润波动仍显著高于北美和 AWS。"),
            ],
            "catalysts": [
                _theme("AWS 继续抬升 mix", 84, "若 AWS 继续快于公司整体增长，Amazon 的结构质量会继续改善。"),
                _theme("官方指引给出区间", 74, str(guidance.get("commentary") or "公司已给出下一季收入与经营利润区间。")),
            ],
            "income_statement": {
                **dict(parsed.get("income_statement") or {}),
                "sources": segments,
                "annotations": [
                    {
                        "title": "AWS 是利润质量核心",
                        "value": format_money_bn(float(aws["value_bn"]), money_symbol) if aws else "-",
                        "note": "高利润率云业务继续改善整体结构。",
                        "color": "#2563EB",
                    },
                    {
                        "title": "北美零售仍是最大盘",
                        "value": format_money_bn(float(north_america["value_bn"]), money_symbol) if north_america else "-",
                        "note": "北美仍是收入规模最大的零售市场。",
                        "color": "#111827",
                    },
                    {
                        "title": "国际业务继续修复",
                        "value": format_money_bn(float(international["value_bn"]), money_symbol) if international else "-",
                        "note": "国际零售恢复节奏将决定下一阶段经营杠杆弹性。",
                        "color": "#F59E0B",
                    },
                ],
            },
            "coverage_notes": [
                "Amazon 较早季度会优先从官方 earnings release 的 segment highlights 表中动态抽取 North America / International / AWS 收入。"
            ],
        }
    )
    return _merge_parsed_payload(updates, parsed)


def _parse_amazon(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    if _quarter_is_before(fallback, "2024Q1"):
        return _parse_amazon_historical(company, fallback, materials)
    release = _pick_material(materials, kind="official_release")
    if release is None:
        return {}

    flat = release["flat_text"]
    money_symbol = company["money_symbol"]

    total_revenue_bn, prior_revenue_bn = _reversed_millions_row_no_pct(flat, "Total net sales")
    cost_of_sales_bn, _ = _reversed_millions_row_no_pct(flat, "Cost of sales")
    fulfillment_bn, _ = _reversed_millions_row_no_pct(flat, "Fulfillment")
    tech_bn, _ = _reversed_millions_row_no_pct(flat, "Technology and infrastructure")
    sales_marketing_bn, _ = _reversed_millions_row_no_pct(flat, "Sales and marketing")
    ga_bn, _ = _reversed_millions_row_no_pct(flat, "General and administrative")
    total_opex_bn, _ = _reversed_millions_row_no_pct(flat, "Total operating expenses")
    operating_income_bn, prior_operating_income_bn = _reversed_millions_row_no_pct(flat, "Operating income")
    guidance_revenue_match = _search(r"Net sales are expected to be between \$([0-9.]+) billion and \$([0-9.]+) billion, or to grow between ([0-9]+)% and ([0-9]+)%", flat)
    guidance_op_income_match = _search(r"Operating income is expected to be between \$([0-9.]+) billion and \$([0-9.]+) billion", flat)
    net_income_match = _search(r"Net income increased to \$([0-9.]+) billion in the fourth quarter, or \$([0-9.]+) per diluted share, compared with \$([0-9.]+) billion", flat)
    aws_match = _search(r"AWS segment sales increased ([0-9]+)% year-over-year to \$([0-9.]+) billion", flat)
    na_match = _search(r"North America segment sales increased ([0-9]+)% year-over-year to \$([0-9.]+) billion", flat)
    intl_match = _search(r"International segment sales increased ([0-9]+)% year-over-year to \$([0-9.]+) billion", flat)
    ocf_match = _search(r"Operating cash flow increased ([0-9]+)% to \$([0-9.]+) billion for the trailing twelve months", flat)
    fcf_match = _search(r"Free cash flow decreased to \$([0-9.]+) billion for the trailing twelve months", flat)
    quote_match = _search(r"“AWS growing 24% \(our fastest growth in 13 quarters\), Advertising growing 22%.*?return on invested capital.”", release["raw_text"])

    gross_margin_pct = None
    if total_revenue_bn not in (None, 0) and cost_of_sales_bn is not None:
        gross_margin_pct = (float(total_revenue_bn) - float(cost_of_sales_bn)) / float(total_revenue_bn) * 100

    segments = _segment_list(
        _segment("North America", _bn_from_billions(na_match.group(2)) if na_match else None, _pct_value(na_match.group(1)) if na_match else None),
        _segment("International", _bn_from_billions(intl_match.group(2)) if intl_match else None, _pct_value(intl_match.group(1)) if intl_match else None),
        _segment("AWS", _bn_from_billions(aws_match.group(2)) if aws_match else None, _pct_value(aws_match.group(1)) if aws_match else None),
    )

    low_guidance_bn = _bn_from_billions(guidance_revenue_match.group(1)) if guidance_revenue_match else None
    high_guidance_bn = _bn_from_billions(guidance_revenue_match.group(2)) if guidance_revenue_match else None
    guidance_commentary = _guidance_midpoint_commentary(
        low_guidance_bn,
        high_guidance_bn,
        _pct_value(guidance_revenue_match.group(3)) if guidance_revenue_match else None,
        _pct_value(guidance_revenue_match.group(4)) if guidance_revenue_match else None,
        (
            f"经营利润指引区间为 {format_money_bn(_bn_from_billions(guidance_op_income_match.group(1)) if guidance_op_income_match else None)}"
            f" 到 {format_money_bn(_bn_from_billions(guidance_op_income_match.group(2)) if guidance_op_income_match else None)}。"
        ),
    )

    facts = {
        "primary_source_label": release["label"],
        "structure_source_label": release["label"],
        "guidance_source_label": release["label"],
        "revenue_bn": total_revenue_bn,
        "revenue_yoy_pct": _pct_change(total_revenue_bn, prior_revenue_bn),
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": _bn_from_billions(net_income_match.group(1)) if net_income_match else None,
        "net_income_yoy_pct": _pct_change(_bn_from_billions(net_income_match.group(1)) if net_income_match else None, _bn_from_billions(net_income_match.group(3)) if net_income_match else None),
        "operating_cash_flow_bn": None,
        "free_cash_flow_bn": None,
        "gaap_eps": _parse_number(net_income_match.group(2)) if net_income_match else None,
        "segments": segments,
        "driver": "AWS 再次加速，零售与广告也在共同抬升整体收入规模",
        "guidance": {
            "mode": "official",
            "revenue_bn": _midpoint(low_guidance_bn, high_guidance_bn),
            "revenue_low_bn": low_guidance_bn,
            "revenue_high_bn": high_guidance_bn,
            "current_label": "本季净销售额",
            "comparison_label": "下一季收入指引中枢",
            "commentary": guidance_commentary,
        },
        "quotes": [
            _quote_card(
                "Andy Jassy",
                (
                    quote_match.group(0).strip("“”")
                    if quote_match
                    else "AWS growing 24% (our fastest growth in 13 quarters), Advertising growing 22%, Stores growing briskly across North America and International."
                ),
                "Amazon 这次把增长主线直接落在 AWS、广告、零售和芯片四条线并行，这使得公司当前并不是单一业务驱动。",
                release["label"],
            ),
            _quote_card(
                "Management",
                "Net sales are expected to be between $173.5 billion and $178.5 billion, or to grow between 11% and 15% compared with first quarter 2025.",
                "公司给出明确的下一季收入区间，同时也把外汇、快速零售和国际价格投入写进了利润指引语境。",
                release["label"],
            ),
        ],
        "management_theme_items": [
            _theme("AWS 加速回升", 90, f"AWS 收入 {format_money_bn(_bn_from_billions(aws_match.group(2)) if aws_match else None, money_symbol)}，同比 {format_pct(_pct_value(aws_match.group(1)) if aws_match else None, signed=True)}。"),
            _theme("AI 与资本开支上行", 80, "管理层预计 2026 年资本开支约 200B 美元，说明 AI、芯片与物流投入都将继续加速。"),
        ],
        "qna_theme_items": [
            _theme("AWS 增长持续性", 80, "AWS 本季是 13 个季度以来最快增速，市场会持续追问这一加速能否延续。"),
            _theme("投入与利润区间", 72, guidance_commentary or "下一季给出收入与经营利润指引，投资强度是核心问答方向。"),
        ],
        "risk_items": [
            _theme("高投入压缩自由现金流", 66, f"TTM 自由现金流仅 {format_money_bn(_bn_from_billions(fcf_match.group(1)) if fcf_match else None, money_symbol)}，主要受 AI 资本开支拖累。"),
            _theme("国际业务利润波动", 58, "国际业务收入恢复较快，但利润率仍明显低于北美和 AWS。"),
        ],
        "catalyst_items": [
            _theme("AWS 与广告共振", 86, "AWS 加速、广告持续增长、零售稳健扩张，使 Amazon 具备多引擎共同上修的条件。"),
            _theme("官方指引仍具韧性", 74, guidance_commentary or "管理层给出的下一季区间仍然维持双位数增长。"),
        ],
        "income_statement": {
            "subtitle": "利润表页改用 Amazon 官方披露的 segment 销售与 operating expense 科目。",
            "sources": segments,
            "opex_breakdown": [
                {"name": "Fulfillment", "value_bn": round(fulfillment_bn or 0.0, 3), "pct_of_revenue": round((fulfillment_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#E11D48"},
                {"name": "Technology and infrastructure", "value_bn": round(tech_bn or 0.0, 3), "pct_of_revenue": round((tech_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#F43F5E"},
                {"name": "Sales and marketing", "value_bn": round(sales_marketing_bn or 0.0, 3), "pct_of_revenue": round((sales_marketing_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#FB7185"},
                {"name": "General and administrative", "value_bn": round(ga_bn or 0.0, 3), "pct_of_revenue": round((ga_bn or 0.0) / (total_revenue_bn or 1) * 100, 1), "color": "#FDA4AF"},
            ],
            "annotations": [
                {"title": "AWS 再次加速", "value": f"{format_money_bn(_bn_from_billions(aws_match.group(2)) if aws_match else None, money_symbol)} | {format_pct(_pct_value(aws_match.group(1)) if aws_match else None, signed=True)} YoY", "note": "这是 13 个季度以来最快增速。", "color": "#2563EB"},
                {"title": "北美仍是最大体量", "value": f"{format_money_bn(_bn_from_billions(na_match.group(2)) if na_match else None, money_symbol)}", "note": "北美零售仍是最大的收入底盘。", "color": "#111827"},
                {"title": "AI 投入继续上台阶", "value": "~$200B capex", "note": "资本开支会继续影响自由现金流与利润弹性。", "color": "#0EA5E9"},
            ],
        },
        "coverage_notes": [
            f"TTM 经营现金流 {format_money_bn(_bn_from_billions(ocf_match.group(2)) if ocf_match else None, money_symbol)}、TTM 自由现金流 {format_money_bn(_bn_from_billions(fcf_match.group(1)) if fcf_match else None, money_symbol)} 来自公司新闻稿的 trailing-twelve-month 口径，因此未写入单季 KPI 卡。"
        ],
    }
    return _finalize(company, fallback, facts, materials)


def _parse_meta(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release", label_contains="results") or _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing")
    call_summary = _pick_material(materials, kind="call_summary") or _pick_material(materials, role="earnings_call")
    if release is None:
        return {}

    flat = release["flat_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, prior_revenue_bn, revenue_yoy_pct = _millions_row(flat, "Revenue")
    advertising_bn, advertising_prior_bn, advertising_yoy_pct = _millions_row(flat, "Advertising")
    expenses_bn, expenses_prior_bn, expenses_yoy_pct = _millions_row(flat, "Total costs and expenses")
    cost_of_revenue_bn, _ = _millions_row_no_pct(flat, "Cost of revenue")
    operating_income_bn, prior_operating_income_bn, operating_income_yoy_pct = _millions_row(flat, "Income from operations")
    net_income_bn, prior_net_income_bn, net_income_yoy_pct = _millions_row(flat, "Net income")
    eps_bn = _search(r"Diluted earnings per share \(EPS\)\s+\$?\s*([0-9.]+)\s+\$?\s*([0-9.]+)\s+([0-9]+)\s*%", flat)
    ocf_match = _search(r"Cash flow from operating activities was \$([0-9.]+) billion and \$([0-9.]+) billion, and free cash flow was \$([0-9.]+) billion and \$([0-9.]+) billion", flat)
    guidance_match = _search(r"We expect first quarter 2026 total revenue to be in the range of \$([0-9.]+)-([0-9.]+) billion", flat)
    expense_match = _search(r"We expect full year 2026 total expenses to be in the range of \$([0-9.]+)-([0-9.]+) billion", flat)
    capex_match = _search(r"We anticipate 2026 capital expenditures.*?to be in the range of \$([0-9.]+)-([0-9.]+) billion", flat)
    foa_current_bn, foa_prior_bn = _millions_row_no_pct(flat, "Family of Apps")
    rl_current_bn, rl_prior_bn = _millions_row_no_pct(flat, "Reality Labs")
    r_and_d_bn, _ = _millions_row_no_pct(flat, "Research and development")
    marketing_sales_bn, _ = _millions_row_no_pct(flat, "Marketing and sales")
    ga_bn, _ = _millions_row_no_pct(flat, "General and administrative")
    dau_match = _search(r"DAUs were\s*([0-9.]+)\s*billion.*?increase of\s*([0-9]+)%", flat)
    mau_match = _search(r"MAUs were\s*([0-9.]+)\s*billion.*?increase of\s*([0-9]+)%", flat)
    mobile_ad_share_match = _search(r"Mobile advertising revenue .*?approximately\s*([0-9]+)%\s*of advertising revenue", flat)
    capex_full_year_match = _search(r"Capital expenditures .*?were\s*\$([0-9.]+)\s*billion", flat)
    cash_balance_match = _search(r"Cash and cash equivalents and marketable securities were\s*\$([0-9.]+)\s*billion", flat)
    commentary_flat = str((call_summary or release).get("flat_text") or "")
    ad_price_match = _search(r"average price per ad increased\s*([0-9]+)%", commentary_flat)
    ad_impression_match = _search(r"(?:total number of )?ad impressions served increased\s*([0-9]+)%", commentary_flat)
    video_focus_match = _search(r"video as a mega trend.*?keep putting video first", commentary_flat)
    instagram_match = _search(r"Instagram now has over\s*([0-9.]+)\s*million monthly actives.*?passed\s*([0-9.]+)\s*million daily actives", commentary_flat)

    gross_margin_pct = None
    if revenue_bn not in (None, 0) and cost_of_revenue_bn is not None:
        gross_margin_pct = (float(revenue_bn) - float(cost_of_revenue_bn)) / float(revenue_bn) * 100

    segments = _segment_list(
        _segment("Family of Apps", foa_current_bn, _pct_change(foa_current_bn, foa_prior_bn)),
        _segment("Reality Labs", rl_current_bn, _pct_change(rl_current_bn, rl_prior_bn)),
    )
    geographies: list[dict[str, Any]] = []
    if sec is not None:
        sec_flat = sec["flat_text"]
        us_canada_match = _search(r"United States and Canada\s+\(1\)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*[0-9,]+", sec_flat)
        europe_match = _search(r"Europe\s+\(2\)\s+([0-9,]+)\s+([0-9,]+)\s+[0-9,]+", sec_flat)
        apac_match = _search(r"Asia-Pacific\s+([0-9,]+)\s+([0-9,]+)\s+[0-9,]+", sec_flat)
        rest_match = _search(r"Rest of World\s+\(2\)\s+([0-9,]+)\s+([0-9,]+)\s+[0-9,]+", sec_flat)
        geographies = [
            item
            for item in [
                _annual_geography("United States and Canada", _bn_from_millions(us_canada_match.group(1)) if us_canada_match else None, _bn_from_millions(us_canada_match.group(2)) if us_canada_match else None),
                _annual_geography("Europe", _bn_from_millions(europe_match.group(1)) if europe_match else None, _bn_from_millions(europe_match.group(2)) if europe_match else None),
                _annual_geography("Asia-Pacific", _bn_from_millions(apac_match.group(1)) if apac_match else None, _bn_from_millions(apac_match.group(2)) if apac_match else None),
                _annual_geography("Rest of World", _bn_from_millions(rest_match.group(1)) if rest_match else None, _bn_from_millions(rest_match.group(2)) if rest_match else None),
            ]
            if item
        ]
    if not geographies:
        annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        annual_geographies = _extract_company_geographies(str(company["id"]), annual_materials, revenue_bn) if annual_materials else []
        if len(annual_geographies) >= 2:
            geographies = annual_geographies

    low_guidance_bn = _bn_from_billions(guidance_match.group(1)) if guidance_match else None
    high_guidance_bn = _bn_from_billions(guidance_match.group(2)) if guidance_match else None
    guidance: dict[str, Any] = {}
    guidance_commentary = ""
    if guidance_match:
        guidance_commentary = _guidance_midpoint_commentary(low_guidance_bn, high_guidance_bn) or "公司给出了下一季收入区间。"
        if expense_match or capex_match:
            guidance_commentary += " "
            guidance_commentary += (
                f"2026 年总费用区间 {format_money_bn(_bn_from_billions(expense_match.group(1)) if expense_match else None)}"
                f" 到 {format_money_bn(_bn_from_billions(expense_match.group(2)) if expense_match else None)}，"
                f"资本开支区间 {format_money_bn(_bn_from_billions(capex_match.group(1)) if capex_match else None)}"
                f" 到 {format_money_bn(_bn_from_billions(capex_match.group(2)) if capex_match else None)}。"
            )
        guidance = {
            "mode": "official",
            "revenue_bn": _midpoint(low_guidance_bn, high_guidance_bn),
            "revenue_low_bn": low_guidance_bn,
            "revenue_high_bn": high_guidance_bn,
            "comparison_label": "下一季收入指引中枢",
            "commentary": guidance_commentary,
        }
    else:
        context_bits: list[str] = []
        if mobile_ad_share_match:
            context_bits.append(f"官方披露移动广告收入约占广告收入的 {mobile_ad_share_match.group(1)}%，移动端商业化仍在提升整体变现质量")
        if capex_full_year_match:
            context_bits.append(f"全年资本开支约 {format_money_bn(_bn_from_billions(capex_full_year_match.group(1)))}")
        if cash_balance_match:
            context_bits.append(f"期末现金及有价证券约 {format_money_bn(_bn_from_billions(cash_balance_match.group(1)))}")
        if context_bits:
            guidance_commentary = "；".join(context_bits) + "。"
            guidance = {
                "mode": "official_context",
                "commentary": guidance_commentary,
            }

    driver_bits: list[str] = []
    if advertising_bn is not None and advertising_yoy_pct is not None:
        driver_bits.append(f"广告收入 {format_money_bn(advertising_bn, money_symbol)}，同比 {format_pct(advertising_yoy_pct, signed=True)}")
    if dau_match:
        driver_bits.append(f"DAU 达到 {dau_match.group(1)}B，同比 {format_pct(_pct_value(dau_match.group(2)), signed=True)}")
    elif mau_match:
        driver_bits.append(f"MAU 达到 {mau_match.group(1)}B，同比 {format_pct(_pct_value(mau_match.group(2)), signed=True)}")
    if not driver_bits and foa_current_bn is not None:
        driver_bits.append("广告主引擎仍然强劲，AI 与基础设施投入进入更高强度阶段")

    quotes = _extract_quote_cards(release) or _extract_quote_cards(call_summary)

    management_theme_items: list[dict[str, Any]] = []
    if foa_current_bn is not None:
        management_theme_items.append(
            _theme("Family of Apps 现金牛仍强", 90, f"Family of Apps 收入 {format_money_bn(foa_current_bn, money_symbol)}，同比 {format_pct(_pct_change(foa_current_bn, foa_prior_bn), signed=True)}。")
        )
    elif advertising_bn is not None:
        management_theme_items.append(
            _theme("广告收入保持高增", 88, f"广告收入 {format_money_bn(advertising_bn, money_symbol)}，同比 {format_pct(advertising_yoy_pct, signed=True)}。")
        )
    if mobile_ad_share_match:
        management_theme_items.append(
            _theme("移动端商业化继续提升", 80, f"移动广告收入约占广告收入 {mobile_ad_share_match.group(1)}%，说明流量变现仍在向移动端集中。")
        )
    elif dau_match or mau_match:
        user_label = "DAU" if dau_match else "MAU"
        user_match = dau_match or mau_match
        management_theme_items.append(
            _theme("用户规模继续扩张", 78, f"{user_label} 达到 {user_match.group(1)}B，同比 {format_pct(_pct_value(user_match.group(2)), signed=True)}。")
        )
    if guidance_commentary and (guidance_match or expense_match or capex_match):
        management_theme_items.append(_theme("投入强度继续抬升", 82, guidance_commentary))

    qna_theme_items: list[dict[str, Any]] = []
    if ad_impression_match or ad_price_match:
        qna_bits: list[str] = []
        if ad_impression_match:
            qna_bits.append(f"广告展示量同比 +{ad_impression_match.group(1)}%")
        if ad_price_match:
            qna_bits.append(f"单广告平均价格同比 +{ad_price_match.group(1)}%")
        qna_theme_items.append(_theme("广告 load 与定价弹性", 80, "，".join(qna_bits) + "，市场会继续追问增长来自流量、load 还是定价。"))
    if rl_current_bn is not None or "reality labs" in commentary_flat.lower():
        qna_theme_items.append(_theme("Reality Labs 亏损路径", 70, "Reality Labs 的投入节奏与亏损收敛速度，仍会是电话会追问重点。"))
    elif video_focus_match or instagram_match:
        note = "视频与社交流量分发仍是未来几年的产品重点。"
        if instagram_match:
            note = (
                f"Instagram 月活已超过 {instagram_match.group(1)}M、日活超过 {instagram_match.group(2)}M，"
                "视频与商业化提效会继续成为电话会重点。"
            )
        qna_theme_items.append(_theme("视频与 Instagram 商业化", 74, note))

    risk_items: list[dict[str, Any]] = []
    if expenses_yoy_pct is not None and revenue_yoy_pct is not None and float(expenses_yoy_pct) > float(revenue_yoy_pct):
        risk_items.append(_theme("费用增速高于收入", 68, f"本季总成本与费用同比增长 {format_pct(expenses_yoy_pct, signed=True)}，快于收入增长。"))
    if "legal and regulatory" in flat.lower() or "regulation" in flat.lower():
        risk_items.append(_theme("监管与法律风险", 60, "官方材料继续强调监管与法律环境变化可能影响结果。"))

    catalyst_items: list[dict[str, Any]] = []
    if advertising_bn is not None and advertising_yoy_pct is not None:
        catalyst_items.append(_theme("广告引擎延续高增长", 86, f"广告收入同比 {format_pct(advertising_yoy_pct, signed=True)}，广告投放和价格仍在同步改善。"))
    if instagram_match:
        catalyst_items.append(_theme("Instagram 与视频延续放量", 76, f"Instagram MAU 超过 {instagram_match.group(1)}M、DAU 超过 {instagram_match.group(2)}M，后续商业化空间仍大。"))
    elif guidance_commentary:
        catalyst_items.append(_theme("官方经营语境偏积极", 72, guidance_commentary))

    annotations = [
        {
            "title": "收入增长仍强",
            "value": f"{format_money_bn(revenue_bn, money_symbol)} | {format_pct(revenue_yoy_pct, signed=True)} YoY",
            "note": "广告业务仍是绝对主驱动。",
            "color": "#0866FF",
        }
    ]
    if advertising_bn is not None:
        annotations.append(
            {
                "title": "广告收入高增",
                "value": format_money_bn(advertising_bn, money_symbol),
                "note": "广告仍是最核心的商业化引擎。",
                "color": "#16A34A",
            }
        )
    elif foa_current_bn is not None:
        annotations.append(
            {
                "title": "FoA 仍是核心引擎",
                "value": format_money_bn(foa_current_bn, money_symbol),
                "note": "广告与社交产品仍是绝对主收入来源。",
                "color": "#0866FF",
            }
        )
    if capex_match or capex_full_year_match:
        capex_value = (
            f"{format_money_bn(_bn_from_billions(capex_match.group(1)) if capex_match else None)}-{format_money_bn(_bn_from_billions(capex_match.group(2)) if capex_match else None)}"
            if capex_match
            else format_money_bn(_bn_from_billions(capex_full_year_match.group(1)) if capex_full_year_match else None)
        )
        annotations.append(
            {
                "title": "CapEx 继续提升",
                "value": capex_value,
                "note": "基础设施投入继续加大，会影响利润弹性与长期增长。",
                "color": "#0EA5E9",
            }
        )
    elif mobile_ad_share_match:
        annotations.append(
            {
                "title": "移动广告占比提升",
                "value": f"{mobile_ad_share_match.group(1)}%",
                "note": "移动端变现效率继续提升。",
                "color": "#0EA5E9",
            }
        )

    facts = {
        "primary_source_label": release["label"],
        "structure_source_label": release["label"],
        "guidance_source_label": release["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct,
        "operating_cash_flow_bn": _bn_from_billions(ocf_match.group(1)) if ocf_match else None,
        "free_cash_flow_bn": _bn_from_billions(ocf_match.group(3)) if ocf_match else None,
        "gaap_eps": _parse_number(eps_bn.group(1)) if eps_bn else None,
        "gaap_eps_yoy_pct": _pct_value(eps_bn.group(3)) if eps_bn else None,
        "segments": segments,
        "geographies": geographies,
        "driver": "，".join(driver_bits[:2]) if driver_bits else "广告主引擎仍然强劲，用户活跃度与商业化效率继续支撑收入增长",
        "guidance": guidance,
        "quotes": quotes,
        "coverage_notes": [
            "Meta 历史季度在缺少可直接复用的地区拆分时，会优先连接官方 10-K 年报中的 billing-address geography 表并完成季度化映射。"
        ] if geographies and any(str(item.get("scope") or "") == "annual_filing" for item in geographies) else [],
        "management_theme_items": management_theme_items,
        "qna_theme_items": qna_theme_items,
        "risk_items": risk_items,
        "catalyst_items": catalyst_items,
        "income_statement": {
            "subtitle": "利润表页改用 Meta 新闻稿中的 segment 收入与费用科目。",
            "sources": segments,
            "opex_breakdown": [
                {"name": "Research and development", "value_bn": round(r_and_d_bn or 0.0, 3), "pct_of_revenue": round((r_and_d_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#E11D48"},
                {"name": "Marketing and sales", "value_bn": round(marketing_sales_bn or 0.0, 3), "pct_of_revenue": round((marketing_sales_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#F43F5E"},
                {"name": "General and administrative", "value_bn": round(ga_bn or 0.0, 3), "pct_of_revenue": round((ga_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#FB7185"},
            ],
            "annotations": annotations,
        },
    }
    return _finalize(company, fallback, facts, materials)


def _walmart_segment_row(
    flat_text: str,
    label: str,
    canonical_name: str,
) -> Optional[dict[str, Any]]:
    pattern = (
        rf"{_table_label_pattern(label)}\s*\$?\s*([0-9]+(?:\.[0-9]+)?)\s*\$?\s*([0-9]+(?:\.[0-9]+)?)\s+"
        rf"{_table_pct_pattern()}\s*%\s+"
    )
    match = _search(pattern, flat_text)
    if not match:
        return None
    current_bn = _bn_from_billions(match.group(1))
    prior_bn = _bn_from_billions(match.group(2))
    yoy_pct = _directional_pct("up", match.group(3))
    if current_bn is None:
        return None
    return _segment(canonical_name, current_bn, yoy_pct if yoy_pct is not None else _pct_change(current_bn, prior_bn))


def _parse_walmart(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    release = _pick_material(materials, kind="official_release") or _pick_material(materials, kind="presentation")
    if release is None:
        return parsed

    flat = release["flat_text"]
    revenue_bn = _coalesce_number(
        dict(parsed.get("latest_kpis") or {}).get("revenue_bn"),
        dict(fallback.get("latest_kpis") or {}).get("revenue_bn"),
    )
    segments = _segment_list(
        _walmart_segment_row(flat, "Walmart U.S.", "Walmart U.S."),
        _walmart_segment_row(flat, "Walmart International", "Walmart International"),
        _walmart_segment_row(flat, "Sam's Club", "Sam's Club U.S."),
        _walmart_segment_row(flat, "Sam’s Club", "Sam's Club U.S."),
    )
    if revenue_bn is None and segments:
        revenue_bn = sum(float(item.get("value_bn") or 0.0) for item in segments) or None
    if not _segments_reasonable_for_revenue(segments, revenue_bn):
        return parsed

    geographies = [
        {
            "name": str(item.get("name") or ""),
            "value_bn": float(item.get("value_bn") or 0.0),
            "yoy_pct": item.get("yoy_pct"),
        }
        for item in segments
        if float(item.get("value_bn") or 0.0) > 0
    ]
    updates = _clean_mapping(
        {
            "current_segments": segments,
            "current_geographies": geographies,
            "coverage_notes": [
                "Walmart 历史季度会优先解析官方 earnings release 的 Net Sales table，并将其地区经营单元口径同步映射到地区结构页。"
            ],
        }
    )
    return _merge_parsed_payload(updates, parsed)


def _nvidia_legacy_value_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{re.escape(label)}\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+"
        r"(Up|Down)\s+([0-9]+(?:\.[0-9]+)?)%\s+(Up|Down)\s+([0-9]+(?:\.[0-9]+)?)%"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None)
    qoq = _directional_pct(match.group(4), match.group(5))
    yoy = _directional_pct(match.group(6), match.group(7))
    return (_bn_from_millions(match.group(1)), qoq, yoy)


def _nvidia_legacy_margin_row(flat_text: str, label: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        rf"{re.escape(label)}\s+([0-9]+(?:\.[0-9]+)?)\s*%\s+([0-9]+(?:\.[0-9]+)?)\s*%\s+([0-9]+(?:\.[0-9]+)?)\s*%\s+"
        r"(Up|Down)\s+([0-9]+(?:\.[0-9]+)?)\s*bps\s+(Up|Down)\s+([0-9]+(?:\.[0-9]+)?)\s*bps"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None, None)
    qoq_bps = _directional_pct(match.group(4), match.group(5))
    yoy_bps = _directional_pct(match.group(6), match.group(7))
    return (
        _pct_value(match.group(1)),
        None if qoq_bps is None else qoq_bps / 100,
        None if yoy_bps is None else yoy_bps / 100,
    )


def _nvidia_legacy_eps_row(flat_text: str) -> tuple[Optional[float], Optional[float]]:
    pattern = (
        r"Diluted earnings per share\s+\$([0-9]+(?:\.[0-9]+)?)\s+\$([0-9]+(?:\.[0-9]+)?)\s+\$([0-9]+(?:\.[0-9]+)?)\s+"
        r"(Up|Down)\s+([0-9]+(?:\.[0-9]+)?)%\s+(Up|Down)\s+([0-9]+(?:\.[0-9]+)?)%"
    )
    match = _search(pattern, flat_text)
    if not match:
        return (None, None)
    return (_parse_number(match.group(1)), _directional_pct(match.group(6), match.group(7)))


def _nvidia_legacy_market_platform_segments(flat_text: str) -> list[dict[str, Any]]:
    mappings = [
        ("Gaming", "Gaming"),
        ("Professional Visualization", "Professional Visualization"),
        ("Datacenter", "Data Center"),
        ("Automotive", "Automotive"),
        ("OEM and IP", "OEM and Other"),
    ]
    segments: list[dict[str, Any]] = []
    for label, name in mappings:
        value_bn, _, yoy_pct = _nvidia_legacy_value_row(flat_text, label)
        segment = _segment(name, value_bn, yoy_pct)
        if segment:
            segments.append(segment)
    return segments


def _parse_nvidia_legacy(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    commentary = _pick_material(materials, kind="presentation", label_contains="commentary") or _pick_material(materials, kind="presentation")
    release = _pick_material(materials, kind="official_release") or commentary
    sec = _pick_material(materials, kind="sec_filing", label_contains="10-k") or _pick_material(materials, kind="sec_filing")
    if commentary is None:
        return {}

    flat = commentary["flat_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, revenue_qoq_pct, revenue_yoy_pct = _nvidia_legacy_value_row(flat, "Revenue")
    operating_income_bn, _, operating_income_yoy_pct = _nvidia_legacy_value_row(flat, "Operating income")
    net_income_bn, _, net_income_yoy_pct = _nvidia_legacy_value_row(flat, "Net income")
    gross_margin_pct, _, _ = _nvidia_legacy_margin_row(flat, "Gross margin")
    gaap_eps, gaap_eps_yoy_pct = _nvidia_legacy_eps_row(flat)
    segments = _nvidia_legacy_market_platform_segments(flat)
    geographies = _nvidia_geographies(sec["flat_text"]) if sec is not None else []
    guidance = _extract_generic_guidance(commentary) or _extract_generic_guidance(release)
    quotes = _extract_quote_cards(release) or _extract_quote_cards(commentary)

    top_segment = _top_segment(segments)
    fastest_segment = _fastest_segment(segments)
    driver_bits: list[str] = []
    if top_segment is not None:
        driver_bits.append(f"{top_segment['name']} 仍是最大平台")
    if fastest_segment is not None and fastest_segment.get("yoy_pct") is not None:
        driver_bits.append(f"{fastest_segment['name']} 同比 {format_pct(float(fastest_segment['yoy_pct']), signed=True)}")
    driver = "，".join(driver_bits) + "，共同支撑季度创纪录收入" if driver_bits else "旧季度材料已切换到官方 CFO commentary 与新闻稿动态解析"

    facts = {
        "primary_source_label": release["label"] if release is not None else commentary["label"],
        "structure_source_label": commentary["label"],
        "guidance_source_label": commentary["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "revenue_qoq_pct": revenue_qoq_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct,
        "gaap_eps": gaap_eps,
        "gaap_eps_yoy_pct": gaap_eps_yoy_pct,
        "segments": segments,
        "geographies": geographies,
        "driver": driver,
        "guidance": guidance,
        "quotes": quotes,
        "management_theme_items": [
            _theme(
                "Gaming 仍是最大收入平台",
                84,
                f"Gaming 收入 {format_money_bn(next((float(item['value_bn']) for item in segments if item['name'] == 'Gaming'), None), money_symbol)}，同比 {format_pct(next((float(item['yoy_pct']) for item in segments if item['name'] == 'Gaming' and item.get('yoy_pct') is not None), None), signed=True)}。",
            ),
            _theme(
                "Data Center 高增开始放大",
                88,
                f"Data Center 收入 {format_money_bn(next((float(item['value_bn']) for item in segments if item['name'] == 'Data Center'), None), money_symbol)}，同比 {format_pct(next((float(item['yoy_pct']) for item in segments if item['name'] == 'Data Center' and item.get('yoy_pct') is not None), None), signed=True)}。",
            ),
        ],
        "qna_theme_items": [
            _theme("Data Center 可持续性", 80, "市场会继续追问 AI / HPC 需求是否会把 Data Center 的高增持续带入后续季度。"),
            _theme("Gaming 高位延续", 74, "Gaming 仍是最大平台，问答重点会围绕新产品周期和高端 GPU 需求强度展开。"),
        ],
        "risk_items": [
            _theme("平台集中度仍高", 68, "Gaming 与 Data Center 合计占比较高，单一平台节奏变化会更快传导到收入与利润。"),
            _theme("小平台环比波动", 58, "Automotive 与 OEM and Other 体量较小且季度波动更明显，短期对结构阅读容易形成噪音。"),
        ],
        "catalyst_items": [
            _theme("Data Center 三位数增长", 86, "旧季度官方材料已显示 Data Center 进入更强的增长斜率，是后续结构迁移的重要起点。"),
            _theme("官方指引仍稳", 76, str(guidance.get("commentary") or "官方材料给出了下一季收入与利润率指引。")),
        ],
        "income_statement": {
            "subtitle": "利润表页改用 NVIDIA CFO commentary 中的 GAAP 汇总与 Market Platform 收入口径。",
            "sources": segments,
            "annotations": [
                {"title": "季度收入创历史新高", "value": f"{format_money_bn(revenue_bn, money_symbol)}", "note": "旧季度已改为直接解析官方新闻稿与 CFO commentary。", "color": "#76B900"},
                {"title": "净利润高弹性兑现", "value": f"{format_money_bn(net_income_bn, money_symbol)} | {format_pct(net_income_yoy_pct, signed=True)} YoY", "note": "收入增长已经明显传导到净利润与 EPS。", "color": "#111827"},
                {"title": "Data Center 进入加速期", "value": f"{format_money_bn(next((float(item['value_bn']) for item in segments if item['name'] == 'Data Center'), None), money_symbol)}", "note": "三位数同比增长使其成为后续 12 季结构迁移的核心来源。", "color": "#0EA5E9"},
            ],
        },
        "coverage_notes": [
            "历史季度已接入 NVIDIA 旧版 8-K 附件中的 CFO commentary，并优先采用 Revenue by Market Platform 表解析结构。",
        ],
    }
    if operating_income_yoy_pct is not None and facts["guidance"].get("commentary") and "gross margin" not in str(facts["guidance"]["commentary"]).lower():
        facts["guidance"]["commentary"] = str(facts["guidance"]["commentary"]).rstrip("。") + f"，本季经营利润同比 {format_pct(operating_income_yoy_pct, signed=True)}。"
    return _finalize(company, fallback, facts, materials)


def _parse_nvidia(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    commentary = _pick_material(materials, kind="presentation", label_contains="commentary") or _pick_material(materials, kind="presentation")
    if commentary is not None and any(token in commentary["flat_text"] for token in ("Revenue by Market Platform", "OEM and IP", "Q4 FY18", "Q1 Fiscal 2019")):
        legacy = _parse_nvidia_legacy(company, fallback, materials)
        if legacy:
            return legacy

    release = _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing", label_contains="10-k") or _pick_material(materials, kind="sec_filing")
    if release is None:
        return {}

    flat = release["flat_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, prior_qoq_revenue_bn, prior_revenue_bn, revenue_qoq_pct, revenue_yoy_pct = _nvidia_gaap_row(flat, "Revenue")
    operating_income_bn, _, prior_operating_income_bn, _, operating_income_yoy_pct = _nvidia_gaap_row(flat, "Operating income")
    net_income_bn, _, prior_net_income_bn, _, net_income_yoy_pct = _nvidia_gaap_row(flat, "Net income")
    gross_margin_match = _search(r"Gross margin\s+([0-9.]+)\s*%\s+[0-9.]+\s*%\s+[0-9.]+\s*%\s+[0-9.]+\s*pts\s+[0-9.]+\s*pts", flat)
    gross_margin_pct = _pct_value(gross_margin_match.group(1)) if gross_margin_match else None
    eps_match = _search(r"Diluted earnings per share\s+\$([0-9.]+)\s+\$([0-9.]+)\s+\$([0-9.]+)\s+([0-9]+)\s+%\s+([0-9]+)\s+%", flat)
    ocf_match = _search(r"Net cash provided by operating activities\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)", flat)
    fcf_match = _search(r"Free cash flow\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", flat)
    guidance_revenue_match = _search(r"Revenue is expected to be \$([0-9.]+) billion, plus or minus ([0-9]+)%", flat)
    guidance_margin_match = _search(r"GAAP and non-GAAP gross margins are expected to be ([0-9.]+)% and ([0-9.]+)%", flat)
    data_center_match = _search(r"Fourth-quarter revenue was a record \$([0-9.]+) billion, up ([0-9]+)% from the previous quarter and up ([0-9]+)% from a year ago, driven by the major platform shifts - accelerated computing and AI", flat)
    gaming_match = _search(r"Fourth-quarter Gaming revenue was \$([0-9.]+) billion, up ([0-9]+)% from a year ago, driven by strong Blackwell demand, and down ([0-9]+)% from the previous quarter", flat)
    pro_viz_match = _search(r"Professional Visualization Fourth-quarter revenue was \$([0-9.]+) billion, up ([0-9]+)% from the previous quarter and up ([0-9]+)% from a year ago", flat)
    auto_match = _search(r"Fourth-quarter Automotive revenue was \$([0-9.]+) million, up ([0-9]+)% from the previous quarter and up ([0-9]+)% from a year ago", flat)

    segments = _segment_list(
        _segment("Data Center", _bn_from_billions(data_center_match.group(1)) if data_center_match else None, _pct_value(data_center_match.group(3)) if data_center_match else None),
        _segment("Gaming", _bn_from_billions(gaming_match.group(1)) if gaming_match else None, _pct_value(gaming_match.group(2)) if gaming_match else None),
        _segment("Professional Visualization", _bn_from_billions(pro_viz_match.group(1)) if pro_viz_match else None, _pct_value(pro_viz_match.group(3)) if pro_viz_match else None),
        _segment("Automotive", _bn_from_millions(auto_match.group(1)) if auto_match else None, _pct_value(auto_match.group(3)) if auto_match else None),
    )
    segment_sum = sum(float(item["value_bn"]) for item in segments)
    if revenue_bn is not None and segment_sum < float(revenue_bn):
        segments.append({"name": "OEM and Other", "value_bn": round(float(revenue_bn) - segment_sum, 3)})
    geographies = _nvidia_geographies(sec["flat_text"]) if sec is not None else []

    guidance_revenue_bn = _bn_from_billions(guidance_revenue_match.group(1)) if guidance_revenue_match else None
    guidance_commentary = (
        f"下一季收入指引为 {format_money_bn(guidance_revenue_bn, money_symbol)}，容差约 ±{guidance_revenue_match.group(2)}%；"
        if guidance_revenue_match
        else ""
    )
    if guidance_margin_match:
        guidance_commentary += (
            f"GAAP / non-GAAP 毛利率指引约为 {guidance_margin_match.group(1)}% / {guidance_margin_match.group(2)}%；"
        )
    guidance_commentary += "管理层同时明确指出，当前展望未假设来自中国的数据中心计算收入。"

    facts = {
        "primary_source_label": release["label"],
        "structure_source_label": release["label"],
        "guidance_source_label": release["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "revenue_qoq_pct": revenue_qoq_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct,
        "operating_cash_flow_bn": _bn_from_millions(ocf_match.group(1)) if ocf_match else None,
        "free_cash_flow_bn": _bn_from_millions(fcf_match.group(1)) if fcf_match else None,
        "gaap_eps": _parse_number(eps_match.group(1)) if eps_match else None,
        "gaap_eps_yoy_pct": _pct_value(eps_match.group(5)) if eps_match else None,
        "segments": segments,
        "geographies": geographies,
        "driver": "Data Center 继续绝对主导，Blackwell / Rubin 与 agentic AI 需求正在继续推高收入与利润",
        "guidance": {
            "mode": "official",
            "revenue_bn": guidance_revenue_bn,
            "comparison_label": "下一季收入指引",
            "commentary": guidance_commentary,
            "gaap_gross_margin_pct": _pct_value(guidance_margin_match.group(1)) if guidance_margin_match else None,
        },
        "quotes": [
            _quote_card(
                "Jensen Huang",
                "Computing demand is growing exponentially - the agentic AI inflection point has arrived.",
                "NVIDIA 把本季强劲表现明确归因到 agentic AI 带来的算力需求拐点，这也是当前估值叙事的核心。",
                release["label"],
            ),
            _quote_card(
                "Management",
                "Revenue is expected to be $78.0 billion, plus or minus 2%. NVIDIA is not assuming any Data Center compute revenue from China in its outlook.",
                "在不计入中国数据中心计算收入的前提下，公司仍给出 78B 的下一季收入中枢，官方口径依然非常强。",
                release["label"],
            ),
        ],
        "management_theme_items": [
            _theme("Data Center 继续绝对主导", 96, f"Data Center 收入 {format_money_bn(_bn_from_billions(data_center_match.group(1)) if data_center_match else None, money_symbol)}，同比 {format_pct(_pct_value(data_center_match.group(3)) if data_center_match else None, signed=True)}。"),
            _theme("Blackwell / Rubin 继续扩散", 86, "管理层把 Blackwell、Rubin 与 inference token cost 优势继续作为后续增长的最核心抓手。"),
        ],
        "qna_theme_items": [
            _theme("中国假设与需求外溢", 82, "公司明确表示下一季展望未计入中国数据中心计算收入，市场会持续追问这一假设的影响。"),
            _theme("Data Center 集中度", 78, "超高集中度意味着任何供给、客户或区域波动都会被快速放大。"),
        ],
        "risk_items": [
            _theme("区域与监管限制", 72, "中国相关限制仍然是管理层主动写进展望的重要不确定性。"),
            _theme("结构集中度过高", 64, "高集中度让季度波动更多取决于 Data Center 的供给和大客户节奏。"),
        ],
        "catalyst_items": [
            _theme("官方指引继续上修", 90, guidance_commentary),
            _theme("Gaming 与新平台协同", 70, "除 Data Center 外，Gaming 和 Professional Visualization 也在继续提供边际增量。"),
        ],
        "income_statement": {
            "subtitle": "利润表页改用 NVIDIA 财报新闻稿中的业务收入结构与 GAAP 利润表口径。",
            "sources": segments,
            "annotations": [
                {"title": "Data Center 仍是一切核心", "value": f"{format_money_bn(_bn_from_billions(data_center_match.group(1)) if data_center_match else None, money_symbol)}", "note": "大部分季度增长与利润弹性仍来自 Data Center。", "color": "#76B900"},
                {"title": "净利润继续高弹性", "value": f"{format_money_bn(net_income_bn, money_symbol)} | {format_pct(net_income_yoy_pct, signed=True)} YoY", "note": "收入增长继续快速传导到净利润。", "color": "#111827"},
                {"title": "下一季指引仍强", "value": f"{format_money_bn(guidance_revenue_bn, money_symbol)}", "note": "在不计入中国数据中心计算收入的前提下仍给出高位展望。", "color": "#0EA5E9"},
            ],
        },
    }
    return _finalize(company, fallback, facts, materials)


def _parse_tsla(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    presentation = _pick_material(materials, kind="presentation") or _pick_material(materials, kind="official_release")
    if presentation is None:
        return {}

    flat = presentation["flat_text"]
    money_symbol = company["money_symbol"]

    revenue_bn, prior_revenue_bn, revenue_yoy_pct = _five_quarter_row(flat, "Total revenues")
    gross_margin_pct, _, _ = _five_quarter_pct_row(flat, "Total GAAP gross margin")
    operating_income_bn, prior_operating_income_bn, operating_income_yoy_pct = _five_quarter_row(flat, "Income from operations")
    net_income_bn, prior_net_income_bn, net_income_yoy_pct = _five_quarter_row(flat, "Net income attributable to common stockholders (GAAP)")
    ocf_bn, _, ocf_yoy_pct = _five_quarter_row(flat, "Net cash provided by operating activities")
    fcf_bn, _, fcf_yoy_pct = _five_quarter_row(flat, "Free cash flow")
    automotive_bn, _, automotive_yoy = _five_quarter_row(flat, "Total automotive revenues")
    energy_bn, _, energy_yoy = _five_quarter_row(flat, "Energy generation and storage revenue")
    services_bn, _, services_yoy = _five_quarter_row(flat, "Services and other revenue")
    r_and_d_bn, _, _ = _five_quarter_row(flat, "Research and development")
    sga_bn, _, _ = _five_quarter_row(flat, "Selling, general and administrative")
    restructuring_bn, _, _ = _five_quarter_row(flat, "Restructuring and other")
    delivery_match = _search(r"produced(?: just over)?\s*([0-9,]+)\s*vehicles and delivered(?: nearly)?\s*([0-9,]+)\s*vehicles", flat)
    china_model_y_match = _search(r"We are encouraged by the strong reception of the Model Y in China and are quickly progressing to full production capacity", flat)
    sx_ramp_match = _search(r"The new Model S and Model X have also been exceptionally well received, with the new equipment installed and tested in Q1 and we are in the early stages of ramping production", flat)
    delivery_caution_match = _search(r"Our delivery count should be viewed as slightly conservative.*?Final numbers could vary by up to 0\.5% or more", flat)

    segments = _segment_list(
        _segment("Automotive", automotive_bn, automotive_yoy),
        _segment("Energy Generation and Storage", energy_bn, energy_yoy),
        _segment("Services and Other", services_bn, services_yoy),
    )

    guidance_bits: list[str] = []
    if delivery_match:
        guidance_bits.append(f"官方交付更新显示 Q1 生产约 {delivery_match.group(1)} 辆、交付约 {delivery_match.group(2)} 辆")
    if china_model_y_match:
        guidance_bits.append("管理层强调 Model Y 在中国的接受度强，并正快速迈向满产")
    if sx_ramp_match:
        guidance_bits.append("新款 Model S / X 已完成设备安装测试，仍处于产能爬坡早期")
    guidance_commentary = "；".join(guidance_bits) + "。" if guidance_bits else ""

    driver_bits: list[str] = []
    if delivery_match:
        driver_bits.append(f"交付约 {delivery_match.group(2)} 辆")
    if china_model_y_match:
        driver_bits.append("中国 Model Y 放量")
    if sx_ramp_match:
        driver_bits.append("新款 S/X 爬坡")
    elif energy_bn is not None and energy_yoy is not None:
        driver_bits.append(f"能源业务同比 {format_pct(energy_yoy, signed=True)}")

    quotes: list[dict[str, str]] = []
    if delivery_match:
        quotes.append(
            _quote_card(
                "Tesla update",
                f"In the first quarter, we produced just over {delivery_match.group(1)} vehicles and delivered nearly {delivery_match.group(2)} vehicles.",
                "这段官方更新直接定义了当季最核心的运营锚点，即产销规模是否延续放量。",
                presentation["label"],
            )
        )
    combined_quote = " ".join(
        match.group(0)
        for match in (china_model_y_match, sx_ramp_match)
        if match is not None
    ).strip()
    if combined_quote:
        quotes.append(
            _quote_card(
                "Tesla update",
                combined_quote,
                "中国 Model Y 放量与新款 S/X 爬坡，是当季交付质量和后续产能节奏的关键语境。",
                presentation["label"],
            )
        )

    management_theme_items: list[dict[str, Any]] = []
    if delivery_match:
        management_theme_items.append(
            _theme("交付规模继续放量", 86, f"Q1 生产约 {delivery_match.group(1)} 辆、交付约 {delivery_match.group(2)} 辆，说明需求与执行仍在放量。")
        )
    if china_model_y_match:
        management_theme_items.append(_theme("中国 Model Y 放量", 80, china_model_y_match.group(0)))
    if sx_ramp_match:
        management_theme_items.append(_theme("新款 S/X 进入爬坡期", 76, sx_ramp_match.group(0)))
    elif energy_bn is not None:
        management_theme_items.append(
            _theme("能源业务继续放量", 74, f"Energy Generation and Storage 收入 {format_money_bn(energy_bn, money_symbol)}，同比 {format_pct(energy_yoy, signed=True)}。")
        )

    qna_theme_items: list[dict[str, Any]] = []
    if automotive_bn is not None:
        qna_theme_items.append(
            _theme("汽车收入与利润何时修复", 82, f"Automotive 收入 {format_money_bn(automotive_bn, money_symbol)}，同比 {format_pct(automotive_yoy, signed=True)}。")
        )
    if china_model_y_match or sx_ramp_match:
        qna_theme_items.append(
            _theme("产能爬坡节奏", 76, guidance_commentary or "中国工厂与新车型的产能爬坡速度，会继续主导市场对后续交付节奏的判断。")
        )

    risk_items: list[dict[str, Any]] = []
    if automotive_bn is not None:
        risk_items.append(_theme("汽车业务仍是主要波动源", 72, f"Automotive 收入 {format_money_bn(automotive_bn, money_symbol)}，仍决定整体收入与利润弹性。"))
    if gross_margin_pct is not None:
        risk_items.append(_theme("利润率仍需观察", 64, f"GAAP 毛利率 {format_pct(gross_margin_pct)}，价格、成本与产能爬坡都会影响利润表现。"))
    if delivery_caution_match:
        risk_items.append(_theme("交付统计存在小幅波动", 58, "官方提醒交付统计略偏保守，最终数字可能仍有小幅调整。"))

    catalyst_items: list[dict[str, Any]] = []
    if china_model_y_match:
        catalyst_items.append(_theme("中国 Model Y 满产", 80, "若中国 Model Y 继续顺利爬坡，将直接支撑后续交付与收入兑现。"))
    if sx_ramp_match:
        catalyst_items.append(_theme("新款 S/X 放量", 74, "新款 Model S / X 的量产节奏若持续改善，会提升高端产品线贡献。"))
    if energy_bn is not None and energy_yoy is not None and float(energy_yoy) > 0:
        catalyst_items.append(_theme("能源业务高增延续", 72, f"能源业务同比 {format_pct(energy_yoy, signed=True)}，正在形成更稳的第二增长曲线。"))

    facts = {
        "primary_source_label": presentation["label"],
        "structure_source_label": presentation["label"],
        "guidance_source_label": presentation["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct,
        "operating_cash_flow_bn": ocf_bn,
        "free_cash_flow_bn": fcf_bn,
        "segments": segments,
        "driver": "、".join(driver_bits[:3]) if driver_bits else "交付节奏、车型爬坡与盈利修复决定本季阅读重点",
        "guidance": {
            "mode": "official_context",
            "commentary": guidance_commentary,
        },
        "quotes": quotes,
        "management_theme_items": management_theme_items,
        "qna_theme_items": qna_theme_items,
        "risk_items": risk_items,
        "catalyst_items": catalyst_items,
        "income_statement": {
            "subtitle": "利润表页改用 Tesla shareholder deck 的季度收入结构与费用口径。",
            "sources": segments,
            "opex_breakdown": [
                {"name": "Research and development", "value_bn": round(r_and_d_bn or 0.0, 3), "pct_of_revenue": round((r_and_d_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#E11D48"},
                {"name": "Selling, general and administrative", "value_bn": round(sga_bn or 0.0, 3), "pct_of_revenue": round((sga_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#F43F5E"},
                {"name": "Restructuring and other", "value_bn": round(restructuring_bn or 0.0, 3), "pct_of_revenue": round((restructuring_bn or 0.0) / (revenue_bn or 1) * 100, 1), "color": "#FB7185"},
            ],
            "annotations": [
                {"title": "能源业务继续放量", "value": f"{format_money_bn(energy_bn, money_symbol)} | {format_pct(energy_yoy, signed=True)} YoY", "note": "能源正在成为更高质量的第二增长曲线。", "color": "#10B981"},
                {"title": "汽车仍是主要收入池", "value": f"{format_money_bn(automotive_bn, money_symbol)}", "note": "汽车业务仍占绝对大头，但同比仍在承压。", "color": "#DC2626"},
                {"title": "现金流仍为正", "value": f"{format_money_bn(ocf_bn, money_symbol)} OCF / {format_money_bn(fcf_bn, money_symbol)} FCF", "note": "在重投入周期中仍保持正自由现金流。", "color": "#0EA5E9"},
            ],
        },
    }
    parsed = _finalize(company, fallback, facts, materials)
    coverage_notes = list(parsed.get("coverage_notes") or [])
    revenue_bn = parsed.get("latest_kpis", {}).get("revenue_bn")
    if not parsed.get("current_geographies"):
        annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        annual_geographies = _extract_company_geographies(str(company["id"]), annual_materials, revenue_bn) if annual_materials else []
        if len(annual_geographies) >= 2:
            parsed["current_geographies"] = annual_geographies
            coverage_notes.append("Tesla 的地区结构已优先改为官方 10-K 年报中的 geographic revenue table 口径。")
    if parsed.get("current_geographies") and _geographies_look_suspicious(list(parsed["current_geographies"]), revenue_bn):
        parsed["current_geographies"] = []
    if not parsed.get("current_geographies"):
        coverage_notes.append("Tesla 当前未稳定得到可信地区收入拆分时，系统会主动拦截异常口径，避免错误地区圆环进入 PDF。")
    parsed["coverage_notes"] = coverage_notes
    return parsed


def _parse_jnj(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    fiscal_label = str(fallback.get("fiscal_label") or fallback.get("calendar_quarter") or "")
    expected_quarter_header = {
        "Q1": "FIRST QUARTER",
        "Q2": "SECOND QUARTER",
        "Q3": "THIRD QUARTER",
        "Q4": "FOURTH QUARTER",
    }.get(fiscal_label[-2:].upper())

    def pick_supplement() -> Optional[dict[str, Any]]:
        for material in materials:
            label = str(material.get("label") or "").lower()
            role = str(material.get("role") or "")
            kind = str(material.get("kind") or "")
            if "99.2" in label and kind in {"presentation", "official_release"}:
                return material
            if "supplement" in label and kind in {"presentation", "official_release"}:
                return material
            if role == "earnings_commentary" and kind == "presentation":
                return material
        return _pick_material(materials, kind="presentation") or _pick_material(materials, role="earnings_commentary")

    def material_lines(material: Optional[dict[str, Any]]) -> list[str]:
        if material is None:
            return []
        return [
            flattened
            for line in _clean_text(str(material.get("raw_text") or "")).splitlines()
            if (flattened := _flatten_text(line))
        ]

    def quarter_table(lines: list[str], category: str) -> list[str]:
        fallback_section: list[str] = []
        quarter_headers = {"FIRST QUARTER", "SECOND QUARTER", "THIRD QUARTER", "FOURTH QUARTER"}
        normalized_category = f"sales to customers by {category}".casefold()
        for index in range(len(lines) - 1):
            current_line = lines[index].casefold()
            matches_category = (
                current_line == normalized_category
                or (
                    lines[index] == "Sales to customers by"
                    and index + 1 < len(lines)
                    and lines[index + 1].casefold() == category.casefold()
                )
            )
            if not matches_category:
                continue
            leading_window = lines[max(0, index - 12) : index]
            if expected_quarter_header and expected_quarter_header not in leading_window:
                if not fallback_section and any(header in leading_window for header in quarter_headers):
                    end = next(
                        (
                            cursor
                            for cursor in range(index + 2, len(lines))
                            if lines[cursor].startswith("Note:") or lines[cursor] == "Johnson & Johnson and Subsidiaries"
                        ),
                        len(lines),
                    )
                    fallback_section = lines[index:end]
                continue
            end = next(
                (
                    cursor
                    for cursor in range(index + 2, len(lines))
                    if lines[cursor].startswith("Note:") or lines[cursor] == "Johnson & Johnson and Subsidiaries"
                ),
                len(lines),
            )
            return lines[index:end]
        return fallback_section

    def note_marker(line: str) -> bool:
        return bool(re.fullmatch(r"\(\d+(?:,\d+)*\)", line.strip()))

    def _bn_from_maybe_millions(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return _bn_from_millions(str(value))

    def numeric_row(tokens: list[str], start: int) -> tuple[list[Optional[float]], int]:
        values: list[Optional[float]] = []
        index = start
        while index < len(tokens):
            token = tokens[index].strip()
            if not token or token in {"$", "%"} or note_marker(token):
                index += 1
                continue
            if token in {"-", "—", "–"}:
                values.append(0.0)
                index += 1
                if len(values) >= 5:
                    break
                continue
            if token == "*":
                values.append(None)
                index += 1
                if len(values) >= 5:
                    break
                continue
            parsed = _parse_number(token)
            if parsed is None:
                if values:
                    break
                index += 1
                continue
            values.append(float(parsed))
            index += 1
            if len(values) >= 5:
                break
        return (values, index)

    def row_after_label(
        tokens: list[str],
        labels: list[str],
        *,
        start: int = 0,
        stop_labels: Optional[list[str]] = None,
    ) -> tuple[Optional[float], Optional[float], Optional[float], int]:
        label_set = {label.casefold() for label in labels}
        stop_set = {label.casefold() for label in (stop_labels or [])}
        for index in range(start, len(tokens)):
            token = tokens[index].casefold()
            if stop_set and token in stop_set:
                break
            if token not in label_set:
                continue
            values, end_index = numeric_row(tokens, index + 1)
            if len(values) >= 2:
                yoy_pct = values[2] if len(values) >= 3 and values[2] is not None else None
                return (
                    _bn_from_maybe_millions(values[0]),
                    _bn_from_maybe_millions(values[1]),
                    yoy_pct,
                    end_index,
                )
        return (None, None, None, start)

    def segment_total(tokens: list[str], labels: list[str], next_labels: list[str]) -> tuple[Optional[float], Optional[float], Optional[float]]:
        label_set = {label.casefold() for label in labels}
        next_label_set = {label.casefold() for label in next_labels}
        for index in range(len(tokens)):
            if tokens[index].casefold() not in label_set:
                continue
            current_cursor = index + 1
            while current_cursor < len(tokens) and note_marker(tokens[current_cursor]):
                current_cursor += 1
            _us_current, _us_prior, _us_yoy, current_cursor = row_after_label(
                tokens,
                ["U.S."],
                start=current_cursor,
                stop_labels=next_labels,
            )
            _intl_current, _intl_prior, _intl_yoy, current_cursor = row_after_label(
                tokens,
                ["International"],
                start=current_cursor,
                stop_labels=next_labels,
            )
            while current_cursor < len(tokens):
                current_token = tokens[current_cursor].casefold()
                if current_token in next_label_set or current_token in {"u.s.", "international", "worldwide", "worldwide excluding covid-19 vaccine"}:
                    break
                row_values, row_end = numeric_row(tokens, current_cursor)
                if len(row_values) >= 2:
                    yoy_pct = row_values[2] if len(row_values) >= 3 and row_values[2] is not None else None
                    return (
                        _bn_from_maybe_millions(row_values[0]),
                        _bn_from_maybe_millions(row_values[1]),
                        yoy_pct,
                    )
                current_cursor = row_end + 1 if row_end > current_cursor else current_cursor + 1
            break
        return (None, None, None)

    release = (
        _pick_material(materials, kind="official_release", label_contains="press release")
        or _pick_material(materials, kind="official_release", label_contains="99.1")
        or _pick_material(materials, kind="official_release")
    )
    supplement = pick_supplement()
    if release is None and supplement is None:
        return {}
    primary_material = release or supplement or materials[0]
    flat = str(primary_material.get("flat_text") or "")
    supplement_lines = material_lines(supplement)
    geographic_section = quarter_table(supplement_lines, "geographic area")
    segment_section = quarter_table(supplement_lines, "segment of business")

    revenue_match = _search(r"sales growth of ([0-9().-]+)% to \$([0-9.]+)\s*Billion", flat)
    revenue_bn = _bn_from_billions(revenue_match.group(2)) if revenue_match else None
    revenue_yoy_pct = _pct_value(revenue_match.group(1)) if revenue_match else None
    if revenue_bn is None:
        revenue_alt_match = _search(
            r"Sales of \$([0-9.]+)\s*billion.*?(?:increase|decrease|decline)\s+of\s+([0-9().-]+)%",
            flat,
        )
        if revenue_alt_match is not None:
            revenue_bn = _bn_from_billions(revenue_alt_match.group(1))
            revenue_yoy_pct = _pct_value(revenue_alt_match.group(2))
    if revenue_bn is None:
        revenue_table_bn, _revenue_table_prior_bn, revenue_table_yoy_pct = _extract_table_metric(flat, ["Sales to customers"])
        if revenue_table_bn is not None:
            revenue_bn = revenue_table_bn
            revenue_yoy_pct = revenue_table_yoy_pct
    eps_match = _search(
        r"(?:Earnings per share \(EPS\)|EPS)(?:\s+of)?\s+\$([0-9.]+)(?:\s+(?:increasing|decreasing|decreased|declining|increased|up|down)\s+([0-9.]+)%)?",
        flat,
    )
    eps_current = _parse_number(eps_match.group(1)) if eps_match else None
    eps_prior: Optional[float] = None
    eps_yoy_pct = _pct_value(eps_match.group(2)) if eps_match and eps_match.group(2) else None
    if eps_current is None:
        eps_table_match = _search(
            r"Net earnings per share\s*\(?(?:Diluted|diluted)\)?\s+\$?\s*([0-9.]+)\s+\$?\s*([0-9.]+)\s+\(?([0-9().-]+)\)?",
            flat,
        )
        if eps_table_match is not None:
            eps_current = _parse_number(eps_table_match.group(1))
            eps_prior = _parse_number(eps_table_match.group(2))
            eps_yoy_pct = _pct_value(eps_table_match.group(3))
    net_income_match = _search(
        r"Net Earnings\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+\$?\s*([0-9,]+(?:\.[0-9]+)?)\s+([0-9().-]+)",
        flat,
    )
    net_income_bn = _bn_from_millions(net_income_match.group(1)) if net_income_match else None
    net_income_yoy_pct = _pct_value(net_income_match.group(3)) if net_income_match else None
    if net_income_bn is None:
        net_income_table_bn, _net_income_table_prior_bn, net_income_table_yoy_pct = _extract_table_metric(flat, ["Net earnings"])
        if net_income_table_bn is not None:
            net_income_bn = net_income_table_bn
            net_income_yoy_pct = net_income_table_yoy_pct
    us_bn, _, us_yoy_pct, _ = row_after_label(geographic_section, ["U.S."])
    europe_bn, _, europe_yoy_pct, _ = row_after_label(geographic_section, ["Europe"])
    wh_ex_us_bn, _, wh_ex_us_yoy_pct, _ = row_after_label(geographic_section, ["Western Hemisphere excluding U.S."])
    apac_africa_bn, _, apac_africa_yoy_pct, _ = row_after_label(geographic_section, ["Asia-Pacific, Africa"])
    intl_bn, _, intl_yoy_pct, _ = row_after_label(geographic_section, ["International"])
    consumer_bn, _, consumer_yoy_pct = segment_total(
        segment_section,
        ["Consumer Health", "Consumer"],
        ["Pharmaceutical", "Innovative Medicine", "MedTech", "Medical Devices", "Worldwide", "Worldwide excluding COVID-19 Vaccine"],
    )
    pharma_bn, _, pharma_yoy_pct = segment_total(
        segment_section,
        ["Pharmaceutical", "Innovative Medicine"],
        ["Pharmaceutical excluding COVID-19 Vaccine", "MedTech", "Medical Devices", "Worldwide", "Worldwide excluding COVID-19 Vaccine"],
    )
    medtech_bn, _, medtech_yoy_pct = segment_total(
        segment_section,
        ["MedTech", "Medical Devices", "Medical Devices and Diagnostics"],
        ["Worldwide", "Worldwide excluding COVID-19 Vaccine"],
    )
    guidance_match = _search(
        r"Estimated Reported Sales\s+/?\s+Mid-point\s+\$([0-9.]+)B\s+[–-]\s+\$([0-9.]+)B\s+/\s+\$([0-9.]+)B",
        flat,
    )
    if guidance_match is None:
        guidance_match = _search(r"Estimated Reported Sales.*?\$([0-9.]+)B\s*[–-]\s*\$([0-9.]+)B", flat)
    low_guidance_bn = _bn_from_billions(guidance_match.group(1)) if guidance_match else None
    high_guidance_bn = _bn_from_billions(guidance_match.group(2)) if guidance_match else None

    detailed_geographies = [
        item
        for item in [
            _geography("U.S.", us_bn, us_yoy_pct),
            _geography("Europe", europe_bn, europe_yoy_pct),
            _geography("Western Hemisphere excluding U.S.", wh_ex_us_bn, wh_ex_us_yoy_pct),
            _geography("Asia-Pacific, Africa", apac_africa_bn, apac_africa_yoy_pct),
        ]
        if item
    ]
    geographies = detailed_geographies or [
        item
        for item in [
            _geography("U.S.", us_bn, us_yoy_pct),
            _geography("International", intl_bn, intl_yoy_pct),
        ]
        if item
    ]

    facts = {
        "primary_source_label": str((release or primary_material).get("label") or "Johnson & Johnson official materials"),
        "structure_source_label": str((supplement or release or primary_material).get("label") or "Johnson & Johnson official materials"),
        "guidance_source_label": str((release or primary_material).get("label") or "Johnson & Johnson official materials"),
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct,
        "gaap_eps": eps_current,
        "gaap_eps_yoy_pct": eps_yoy_pct if eps_yoy_pct is not None else _pct_change(eps_current, eps_prior),
        "segments": _segment_list(
            _segment("Consumer Health", consumer_bn, consumer_yoy_pct),
            _segment("Pharmaceutical", pharma_bn, pharma_yoy_pct),
            _segment("MedTech", medtech_bn, medtech_yoy_pct),
        ),
        "geographies": geographies,
        "guidance": {
            "mode": "official",
            "revenue_bn": _midpoint(low_guidance_bn, high_guidance_bn),
            "revenue_low_bn": low_guidance_bn,
            "revenue_high_bn": high_guidance_bn,
            "comparison_label": "全年收入指引中枢",
            "commentary": _guidance_midpoint_commentary(low_guidance_bn, high_guidance_bn, extra="公司在官方财报材料中给出全年收入展望。"),
        },
        "driver": "强生的业务结构应优先从补充销售表动态解析，而不是只读新闻稿叙述；药品、器械与地区拆分都在官方表中稳定披露。",
        "quotes": [
            _quote_card(
                "Joaquin Duato",
                "Our robust performance in the second quarter and first half of 2023 is a testament to the hard work and commitment of our colleagues around the world.",
                "这句管理层原话比模板化摘要更有信息量，也和当季药品与器械同步增长的官方结果直接对应。",
                str((release or primary_material).get("label") or "Johnson & Johnson official materials"),
            )
        ] if release is not None else [],
        "coverage_notes": [
            "强生已切换为优先解析 EX-99.2 supplementary sales data 中的业务与地区收入表，而不是只依赖新闻稿正文。"
        ],
    }
    return _finalize(company, fallback, facts, materials)


def _parse_visa(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release", label_contains="earningsrelease") or _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing")
    if release is None:
        return {}
    flat = release["flat_text"]
    revenue_match = _search(r"Net revenue in the fiscal fourth quarter was \$([0-9.]+) billion, an increase of ([0-9]+)%", flat)
    net_income_match = _search(
        r"GAAP net income in the fiscal fourth quarter was \$([0-9.]+) billion or \$([0-9.]+) per share, a (?:decrease|increase) of ([0-9]+)%",
        flat,
    )
    opex_match = _search(r"GAAP operating expenses were \$([0-9.]+) billion for the fiscal fourth quarter, a ([0-9]+)% (?:increase|decrease)", flat)
    service_match = _search(r"Fiscal fourth quarter service revenue was \$([0-9.]+) billion, an increase of ([0-9]+)%", flat)
    data_processing_match = _search(r"Data processing revenue rose ([0-9]+)% over the prior year to \$([0-9.]+) billion", flat)
    intl_match = _search(r"International transaction revenue grew ([0-9]+)% over the prior year to \$([0-9.]+) billion", flat)
    other_match = _search(r"Other revenue of \$([0-9.]+) billion rose ([0-9]+)% over the prior year", flat)
    incentives_match = _search(r"Client incentives were \$([0-9.]+) billion, up ([0-9]+)% over the prior year", flat)
    payments_volume_match = _search(r"Payments volume.*?increased ([0-9]+)%", flat)
    cross_border_match = _search(r"Cross-border volume excluding transactions within Europe.*?increased ([0-9]+)%", flat)
    guidance_commentary = []
    if payments_volume_match:
        guidance_commentary.append(f"支付量同比 +{payments_volume_match.group(1)}%")
    if cross_border_match:
        guidance_commentary.append(f"跨境量同比 +{cross_border_match.group(1)}%")

    geographies: list[dict[str, Any]] = []
    if sec is not None:
        sec_flat = sec["flat_text"]
        us_match = _search(r"U\.S\.\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*[0-9,]+", sec_flat)
        intl_geo_match = _search(r"International\s+([0-9,]+)\s+([0-9,]+)\s+[0-9,]+", sec_flat)
        geographies = [
            item
            for item in [
                _annual_geography("United States", _bn_from_millions(us_match.group(1)) if us_match else None, _bn_from_millions(us_match.group(2)) if us_match else None),
                _annual_geography("International", _bn_from_millions(intl_geo_match.group(1)) if intl_geo_match else None, _bn_from_millions(intl_geo_match.group(2)) if intl_geo_match else None),
            ]
            if item
        ]

    facts = {
        "primary_source_label": release["label"],
        "structure_source_label": release["label"],
        "guidance_source_label": release["label"],
        "revenue_bn": _bn_from_billions(revenue_match.group(1)) if revenue_match else None,
        "revenue_yoy_pct": _pct_value(revenue_match.group(2)) if revenue_match else None,
        "net_income_bn": _bn_from_billions(net_income_match.group(1)) if net_income_match else None,
        "net_income_yoy_pct": (-_pct_value(net_income_match.group(3)) if net_income_match and "decrease" in str(net_income_match.group(0)).lower() else _pct_value(net_income_match.group(3)) if net_income_match else None),
        "gaap_eps": _parse_number(net_income_match.group(2)) if net_income_match else None,
        "segments": _segment_list(
            _segment("Service revenue", _bn_from_billions(service_match.group(1)) if service_match else None, _pct_value(service_match.group(2)) if service_match else None),
            _segment("Data processing revenue", _bn_from_billions(data_processing_match.group(2)) if data_processing_match else None, _pct_value(data_processing_match.group(1)) if data_processing_match else None),
            _segment("International transaction revenue", _bn_from_billions(intl_match.group(2)) if intl_match else None, _pct_value(intl_match.group(1)) if intl_match else None),
            _segment("Other revenue", _bn_from_billions(other_match.group(1)) if other_match else None, _pct_value(other_match.group(2)) if other_match else None),
        ),
        "geographies": geographies,
        "guidance": {
            "mode": "official_context",
            "commentary": "；".join(guidance_commentary) + "。"
            if guidance_commentary
            else "公司未提供明确数值收入指引，但支付量与跨境量增速仍然稳健。",
        },
        "driver": "支付量、数据处理收入与跨境交易仍在共同推升净收入",
        "coverage_notes": [
            "Visa 官方业务结构按 service / data processing / international transaction / other 四类收入池披露，未扣减 client incentives，因此结构图不与净收入直接加总。"
        ],
        "quotes": [
            _quote_card(
                "Visa",
                "Fiscal fourth quarter growth in payments volume, cross-border volume and processed transactions remained strong.",
                "官方原文直接指出支付量、跨境量与处理笔数仍是本季最核心的增长驱动。",
                release["label"],
            )
        ],
        "evidence_cards": [
            {
                "title": "Client incentives",
                "text": f"Client incentives {format_money_bn(_bn_from_billions(incentives_match.group(1)) if incentives_match else None, company['money_symbol'])}，同比 +{incentives_match.group(2)}%。" if incentives_match else "",
                "source_label": release["label"],
            },
            {
                "title": "季度费用",
                "text": f"GAAP operating expenses {format_money_bn(_bn_from_billions(opex_match.group(1)) if opex_match else None, company['money_symbol'])}。" if opex_match else "",
                "source_label": release["label"],
            },
        ],
    }
    return _finalize(company, fallback, facts, materials)


def _parse_micron(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release", label_contains="pressrelease") or _pick_material(materials, kind="official_release")
    if release is None:
        return {}
    flat = release["flat_text"]
    revenue_row = _search(r"Revenue\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", flat)
    gross_profit_row = _search(r"Gross margin\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)", flat)
    opex_row = _search(r"Operating expenses\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)", flat)
    operating_income_row = _search(r"Operating income\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)", flat)
    net_income_row = _search(r"Net income\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)", flat)
    eps_row = _search(r"Diluted earnings per share\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)", flat)
    ocf_match = _search(r"Operating cash flow of \$([0-9.]+) billion versus \$([0-9.]+) billion.*?\$([0-9.]+) billion", flat)
    fcf_match = _search(r"adjusted free cash flow.*?\$([0-9.]+) billion", flat)
    guidance_match = _search(r"Revenue\s+\$([0-9.]+) billion ± \$([0-9.]+) million", flat)
    gross_margin_guidance_match = _search(r"Gross margin\s+([0-9.]+)% ± ([0-9.]+)%", flat)
    cmbu_row = _search(r"Cloud Memory Business Unit\s+Revenue\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", flat)
    cdbu_row = _search(r"Core Data Center Business Unit\s+Revenue\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", flat)
    mcbu_row = _search(r"Mobile and Client Business Unit\s+Revenue\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", flat)
    aebu_row = _search(r"Automotive and Embedded Business Unit\s+Revenue\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)", flat)

    revenue_bn = _bn_from_millions(revenue_row.group(1)) if revenue_row else None
    revenue_yoy_pct = _pct_change(_bn_from_millions(revenue_row.group(1)) if revenue_row else None, _bn_from_millions(revenue_row.group(3)) if revenue_row else None)
    gross_margin_pct = None
    if revenue_bn not in (None, 0) and gross_profit_row is not None:
        gross_margin_pct = float(_bn_from_millions(gross_profit_row.group(1)) or 0.0) / float(revenue_bn) * 100
    guidance_bn = _bn_from_billions(guidance_match.group(1)) if guidance_match else None
    guidance_delta_bn = _bn_from_millions(guidance_match.group(2)) if guidance_match else None
    annual_materials = [item for item in materials if _is_annual_material(item)]
    if not annual_materials:
        annual_materials = _load_nearby_annual_materials(company, fallback, materials)
    annual_sec = _pick_material(annual_materials, kind="sec_filing")
    geographies = _micron_geographies(annual_sec["flat_text"]) if annual_sec is not None else []

    facts = {
        "primary_source_label": release["label"],
        "structure_source_label": release["label"],
        "guidance_source_label": release["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": _bn_from_millions(operating_income_row.group(1)) if operating_income_row else None,
        "net_income_bn": _bn_from_millions(net_income_row.group(1)) if net_income_row else None,
        "net_income_yoy_pct": _pct_change(_bn_from_millions(net_income_row.group(1)) if net_income_row else None, _bn_from_millions(net_income_row.group(3)) if net_income_row else None),
        "operating_cash_flow_bn": _bn_from_billions(ocf_match.group(1)) if ocf_match else None,
        "free_cash_flow_bn": _bn_from_billions(fcf_match.group(1)) if fcf_match else None,
        "gaap_eps": _parse_number(eps_row.group(1)) if eps_row else None,
        "gaap_eps_yoy_pct": _pct_change(_parse_number(eps_row.group(1)) if eps_row else None, _parse_number(eps_row.group(3)) if eps_row else None),
        "segments": _segment_list(
            _segment("Cloud Memory Business Unit", _bn_from_millions(cmbu_row.group(1)) if cmbu_row else None, _pct_change(_bn_from_millions(cmbu_row.group(1)) if cmbu_row else None, _bn_from_millions(cmbu_row.group(3)) if cmbu_row else None)),
            _segment("Core Data Center Business Unit", _bn_from_millions(cdbu_row.group(1)) if cdbu_row else None, _pct_change(_bn_from_millions(cdbu_row.group(1)) if cdbu_row else None, _bn_from_millions(cdbu_row.group(3)) if cdbu_row else None)),
            _segment("Mobile and Client Business Unit", _bn_from_millions(mcbu_row.group(1)) if mcbu_row else None, _pct_change(_bn_from_millions(mcbu_row.group(1)) if mcbu_row else None, _bn_from_millions(mcbu_row.group(3)) if mcbu_row else None)),
            _segment("Automotive and Embedded Business Unit", _bn_from_millions(aebu_row.group(1)) if aebu_row else None, _pct_change(_bn_from_millions(aebu_row.group(1)) if aebu_row else None, _bn_from_millions(aebu_row.group(3)) if aebu_row else None)),
        ),
        "geographies": geographies,
        "guidance": {
            "mode": "official",
            "revenue_bn": guidance_bn,
            "revenue_low_bn": None if guidance_bn is None or guidance_delta_bn is None else guidance_bn - guidance_delta_bn,
            "revenue_high_bn": None if guidance_bn is None or guidance_delta_bn is None else guidance_bn + guidance_delta_bn,
            "comparison_label": "下一季收入指引",
            "commentary": (
                f"下一季收入指引为 {format_money_bn(guidance_bn, company['money_symbol'])}，容差约 ±{format_money_bn(guidance_delta_bn, company['money_symbol'])}；"
                f"GAAP 毛利率指引约 {gross_margin_guidance_match.group(1)}% ± {gross_margin_guidance_match.group(2)}%。"
                if guidance_match and gross_margin_guidance_match
                else None
            ),
        },
        "driver": "AI 相关需求继续推高云与数据中心相关业务，季度现金流同步刷新",
        "quotes": [
            _quote_card(
                "Sanjay Mehrotra",
                "In fiscal Q1, Micron delivered record revenue and significant margin expansion at the company level and also in each of our business units.",
                "管理层明确指出公司层面与各业务单元都出现了显著的收入和利润率扩张。",
                release["label"],
            )
        ],
        "coverage_notes": [
            "Micron 的地区结构在季度未披露时，会自动连接最近一份官方 10-K 的 Geographic Information 表进行映射。",
            "Micron 较早季度已补入 10-Q 中的 CNBU / MBU / SBU / EBU 业务单元表，避免历史结构页退化成纯地区口径。",
        ]
        if geographies
        else [
            "Micron 较早季度已补入 10-Q 中的 CNBU / MBU / SBU / EBU 业务单元表，避免历史结构页退化成纯地区口径。"
        ],
    }
    return _finalize(company, fallback, facts, materials)


def _parse_asml(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    fallback_latest_kpis = dict(fallback.get("latest_kpis") or {})
    fiscal_label = str(fallback.get("fiscal_label") or "")
    quarter_match = re.fullmatch(r"(\d{4})Q([1-4])", fiscal_label)
    quarter_tag = f"Q{quarter_match.group(2)} {quarter_match.group(1)}" if quarter_match else ""

    primary_material: Optional[dict[str, Any]] = None
    extracted: dict[str, Any] = {}
    for material in _ordered_narrative_materials(materials):
        if material.get("kind") not in {"official_release", "presentation", "sec_filing"}:
            continue
        flat = str(material.get("flat_text") or "")
        if not flat:
            continue

        summary_block_match = _search(
            r"Q[1-4]\s+results summary.{0,1200}?(?:(?:Net system sales breakdown|Total net sales|Q[1-4]\s+Outlook|Business highlights|Consolidated statements))",
            flat,
        )
        summary_block = summary_block_match.group(0) if summary_block_match else ""
        if summary_block:
            revenue_bn = None
            system_sales_bn = None
            installed_base_bn = None
            gross_margin_pct = None
            operating_margin_pct = None
            net_margin_pct = None
            gaap_eps = None

            revenue_match = _search(r"Net sales of €\s*([0-9,]+(?:\.[0-9]+)?)\s+million", summary_block)
            system_sales_match = _search(
                r"(?:net systems? sales(?: valued)?|litho systems sold valued)\s+(?:at\s+)?€\s*([0-9,]+(?:\.[0-9]+)?)\s+million",
                summary_block,
            )
            installed_base_match = _search(
                r"(?:net\s+Installed Base Management sales|Installed Base Management sales|net service and field option sales|net service and field options sales)\s+(?:of|at)\s+€\s*([0-9,]+(?:\.[0-9]+)?)\s+million",
                summary_block,
            )
            gross_margin_match = _search(r"Gross margin of\s+([0-9]+(?:\.[0-9]+)?)%", summary_block)
            operating_margin_match = _search(r"Operating margin of\s+([0-9]+(?:\.[0-9]+)?)%", summary_block)
            net_margin_match = _search(r"Net income as a percentage of net sales of\s+([0-9]+(?:\.[0-9]+)?)%", summary_block)
            eps_match = _search(r"Earnings per share \(basic\)\s*€\s*([0-9]+(?:\.[0-9]+)?)", summary_block)

            revenue_bn = _bn_from_millions(revenue_match.group(1)) if revenue_match else None
            system_sales_bn = _bn_from_millions(system_sales_match.group(1)) if system_sales_match else None
            installed_base_bn = _bn_from_millions(installed_base_match.group(1)) if installed_base_match else None
            gross_margin_pct = _pct_value(gross_margin_match.group(1)) if gross_margin_match else None
            operating_margin_pct = _pct_value(operating_margin_match.group(1)) if operating_margin_match else None
            net_margin_pct = _pct_value(net_margin_match.group(1)) if net_margin_match else None
            gaap_eps = _parse_number(eps_match.group(1)) if eps_match else None

            if system_sales_bn is None and revenue_bn is not None and installed_base_bn is not None:
                system_sales_bn = max(0.0, float(revenue_bn) - float(installed_base_bn))

            if revenue_bn is not None and installed_base_bn is not None:
                extracted = {
                    "revenue_bn": revenue_bn,
                    "gross_margin_pct": gross_margin_pct,
                    "operating_income_bn": None if revenue_bn is None or operating_margin_pct is None else revenue_bn * float(operating_margin_pct) / 100,
                    "net_income_bn": None if revenue_bn is None or net_margin_pct is None else revenue_bn * float(net_margin_pct) / 100,
                    "gaap_eps": gaap_eps,
                    "segments": _segment_list(
                        _segment("Net system sales", system_sales_bn, None),
                        _segment("Installed Base Management", installed_base_bn, None),
                    ),
                }
                primary_material = material
                break

        latest_summary = _search(
            r"Q[1-4]\s+20\d{2}\s+Total net sales €([0-9.]+) billion Net system sales €([0-9.]+) billion Installed Base Management1 sales €([0-9.]+) billion Gross Margin ([0-9.]+)% Operating margin2 ([0-9.]+)% Net income as a percentage of total net sales ([0-9.]+)% Earnings per share \(basic\) €([0-9.]+)",
            flat,
        )
        if latest_summary:
            revenue_bn = _bn_from_billions(latest_summary.group(1))
            net_margin_pct = _pct_value(latest_summary.group(6))
            extracted = {
                "revenue_bn": revenue_bn,
                "gross_margin_pct": _pct_value(latest_summary.group(4)),
                "operating_income_bn": None if revenue_bn is None else revenue_bn * float(latest_summary.group(5)) / 100,
                "net_income_bn": None if revenue_bn is None or net_margin_pct is None else revenue_bn * float(net_margin_pct) / 100,
                "gaap_eps": _parse_number(latest_summary.group(7)),
                "segments": _segment_list(
                    _segment("Net system sales", _bn_from_billions(latest_summary.group(2)), None),
                    _segment("Installed Base Management", _bn_from_billions(latest_summary.group(3)), None),
                ),
            }
            primary_material = material
            break

        release_headline_match = _search(
            r"Q([1-4])\s+net sales of EUR\s+([0-9]+(?:\.[0-9]+)?)\s+billion\s*,\s*gross margin\s+([0-9]+(?:\.[0-9]+)?)\s+percent",
            flat,
        )
        if release_headline_match:
            revenue_bn = _bn_from_billions(release_headline_match.group(2))
            service_pair_match = _search(
                r"(?:\.\.\.of which\s+)?service and field option sales(?:\s+\d+)?\s+([0-9,]+)\s+([0-9,]+)",
                flat,
            )
            net_income_pair_match = _search(r"Net income\s+([0-9,]+)\s+([0-9,]+)", flat)
            eps_pair_match = _search(r"EPS \(basic; in euros\)\s+([0-9]+(?:\.[0-9]+)?)\s+([0-9]+(?:\.[0-9]+)?)", flat)
            installed_base_bn = _bn_from_millions(service_pair_match.group(2)) if service_pair_match else None
            net_income_bn = _bn_from_millions(net_income_pair_match.group(2)) if net_income_pair_match else None
            if revenue_bn is not None:
                extracted = {
                    "revenue_bn": revenue_bn,
                    "gross_margin_pct": _pct_value(release_headline_match.group(3)),
                    "net_income_bn": net_income_bn,
                    "gaap_eps": _parse_number(eps_pair_match.group(2)) if eps_pair_match else None,
                    "segments": _segment_list(
                        _segment(
                            "Net system sales",
                            None if revenue_bn is None or installed_base_bn is None else revenue_bn - installed_base_bn,
                            None,
                        ),
                        _segment("Installed Base Management", installed_base_bn, None),
                    ),
                }
                primary_material = material
                break

        legacy_presentation_match = _search(
            r"Q[1-4]\s+results summary.{0,120}?"
            r"Net sales of €\s*([0-9,]+(?:\.[0-9]+)?)\s+million,\s+net systems? sales(?: valued)? at €\s*([0-9,]+(?:\.[0-9]+)?)\s+million,\s+Installed\s+Base\s+Management\*?\s+sales of €\s*([0-9,]+(?:\.[0-9]+)?)\s+million"
            r".{0,180}?Gross margin of ([0-9]+(?:\.[0-9]+)?)%"
            r".{0,120}?Operating margin of ([0-9]+(?:\.[0-9]+)?)%"
            r".{0,140}?Net income as a percentage of net sales of ([0-9]+(?:\.[0-9]+)?)%",
            flat,
        )
        if legacy_presentation_match:
            revenue_bn = _bn_from_millions(legacy_presentation_match.group(1))
            net_margin_pct = _pct_value(legacy_presentation_match.group(6))
            eps_match = _search(
                r"Earnings per share \(basic\) €\s+[0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+(?:\.[0-9]+)?){0,4}\s+([0-9]+(?:\.[0-9]+)?)",
                flat,
            )
            extracted = {
                "revenue_bn": revenue_bn,
                "gross_margin_pct": _pct_value(legacy_presentation_match.group(4)),
                "operating_income_bn": None if revenue_bn is None else revenue_bn * float(legacy_presentation_match.group(5)) / 100,
                "net_income_bn": None if revenue_bn is None or net_margin_pct is None else revenue_bn * float(net_margin_pct) / 100,
                "gaap_eps": _parse_number(eps_match.group(1)) if eps_match else None,
                "segments": _segment_list(
                    _segment("Net system sales", _bn_from_millions(legacy_presentation_match.group(2)), None),
                    _segment("Installed Base Management", _bn_from_millions(legacy_presentation_match.group(3)), None),
                ),
            }
            primary_material = material
            break

        release_table_match = _search(
            r"Net sales\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9,]+(?:\.[0-9]+)?)"
            r".{0,260}?(?:Installed Base Management1? sales|Installed Base Management sales|net service and field option sales)\s+(?:\d+\s+)?([0-9,]+(?:\.[0-9]+)?)\s+([0-9,]+(?:\.[0-9]+)?)"
            r".{0,260}?Gross margin\s*\(%\)\s+([0-9]+(?:\.[0-9]+)?)\s+([0-9]+(?:\.[0-9]+)?)"
            r".{0,260}?Net income\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9,]+(?:\.[0-9]+)?)"
            r".{0,220}?EPS\s*\(basic(?:;\s*in euros)?\)\s+([0-9]+(?:\.[0-9]+)?)\s+([0-9]+(?:\.[0-9]+)?)",
            flat,
        )
        if release_table_match:
            revenue_bn = _bn_from_millions(release_table_match.group(2))
            installed_base_bn = _bn_from_millions(release_table_match.group(4))
            extracted = {
                "revenue_bn": revenue_bn,
                "gross_margin_pct": _pct_value(release_table_match.group(6)),
                "net_income_bn": _bn_from_millions(release_table_match.group(8)),
                "gaap_eps": _parse_number(release_table_match.group(10)),
                "segments": _segment_list(
                    _segment(
                        "Net system sales",
                        None if revenue_bn is None or installed_base_bn is None else revenue_bn - installed_base_bn,
                        None,
                    ),
                    _segment("Installed Base Management", installed_base_bn, None),
                ),
            }
            primary_material = material
            break

        if quarter_tag:
            narrative_match = _search(
                rf"(?:In\s+)?{re.escape(quarter_tag)}.*?net sales of EUR\s+([0-9,]+(?:\.[0-9]+)?)\s+million.*?"
                r"net system sales of EUR\s+([0-9,]+(?:\.[0-9]+)?)\s+million.*?"
                r"(?:Installed Base Management sales|net service and field option sales|net service and field options sales)\s+of EUR\s+([0-9,]+(?:\.[0-9]+)?)\s+million",
                flat,
            )
            if narrative_match:
                extracted = {
                    "revenue_bn": _bn_from_millions(narrative_match.group(1)),
                    "segments": _segment_list(
                        _segment("Net system sales", _bn_from_millions(narrative_match.group(2)), None),
                        _segment("Installed Base Management", _bn_from_millions(narrative_match.group(3)), None),
                    ),
                }
                primary_material = material
                break

    if primary_material is None:
        return {}

    primary_flat = str(primary_material.get("flat_text") or "")
    outlook_match = _search(
        r"expects\s+Q[1-4]\s+20\d{2}\s+net sales to be between €([0-9.]+)\s+billion and €([0-9.]+)\s+billion(?:.*?gross margin between ([0-9.]+)% and ([0-9.]+)%)?",
        primary_flat,
    ) or _search(
        r"Q[1-4]\s+20\d{2}\s+Total net sales between €([0-9.]+)\s+billion and €([0-9.]+)\s+billion(?:\s+of which Installed Base Management1 sales around €([0-9.]+)\s+billion)?\s+Gross margin between ([0-9.]+)% and ([0-9.]+)%",
        primary_flat,
    )

    revenue_bn = _coalesce_number(extracted.get("revenue_bn"), fallback_latest_kpis.get("revenue_bn"))
    gross_margin_pct = _coalesce_number(extracted.get("gross_margin_pct"), fallback_latest_kpis.get("gaap_gross_margin_pct"))
    net_income_bn = _coalesce_number(extracted.get("net_income_bn"), fallback_latest_kpis.get("net_income_bn"))
    operating_income_bn = _coalesce_number(
        extracted.get("operating_income_bn"),
        fallback_latest_kpis.get("operating_income_bn"),
    )
    facts = {
        "primary_source_label": primary_material["label"],
        "structure_source_label": primary_material["label"],
        "guidance_source_label": primary_material["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": fallback_latest_kpis.get("revenue_yoy_pct"),
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": fallback_latest_kpis.get("net_income_yoy_pct"),
        "gaap_eps": _coalesce_number(extracted.get("gaap_eps"), fallback_latest_kpis.get("gaap_eps")),
        "segments": list(extracted.get("segments") or []),
        "guidance": {
            "mode": "official",
            "revenue_bn": _midpoint(_bn_from_billions(outlook_match.group(1)) if outlook_match else None, _bn_from_billions(outlook_match.group(2)) if outlook_match else None),
            "revenue_low_bn": _bn_from_billions(outlook_match.group(1)) if outlook_match else None,
            "revenue_high_bn": _bn_from_billions(outlook_match.group(2)) if outlook_match else None,
            "comparison_label": "下一季收入指引",
            "commentary": (
                f"下一季收入指引区间为 EUR {outlook_match.group(1)}B 到 EUR {outlook_match.group(2)}B。"
                if outlook_match
                else None
            ),
        },
        "driver": "系统销售、Installed Base Management 与订单/积压变化共同决定景气与盈利兑现节奏",
    }
    parsed = _finalize(company, fallback, facts, materials)
    if not parsed.get("current_geographies"):
        annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        annual_geographies = _extract_company_geographies(str(company["id"]), annual_materials, revenue_bn) if annual_materials else []
        if len(annual_geographies) >= 3:
            parsed["current_geographies"] = annual_geographies
            coverage_notes = list(parsed.get("coverage_notes") or [])
            coverage_notes.append("ASML 的地区结构已补充连接最近官方 20-F 年报口径。")
            parsed["coverage_notes"] = coverage_notes
    if not parsed.get("current_geographies"):
        coverage_notes = list(parsed.get("coverage_notes") or [])
        coverage_notes.append("ASML 当前可抓到的官方季度与年报材料仍未稳定给出可直接落地的地区收入拆分。")
        parsed["coverage_notes"] = coverage_notes
    return parsed


def _parse_tsmc(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = _pick_material(materials, kind="official_release")
    presentation = _pick_material(materials, kind="presentation")
    release_flat = str(release.get("flat_text") or "") if release else ""
    presentation_flat = str(presentation.get("flat_text") or "") if presentation else ""
    release_revenue_match = _search(
        r"In US dollars,[^$]{0,200}\$([0-9]+(?:\.[0-9]+)?)\s*billion",
        release_flat,
    )
    release_revenue_yoy_match = _search(
        r"revenue was \$[0-9]+(?:\.[0-9]+)?\s*billion,\s*which\s*(?:increased|grew|rose|up)\s*([0-9]+(?:\.[0-9]+)?)%\s*(?:year-over-year|year over year|yoy)",
        release_flat,
    )
    release_gross_margin_match = _search(
        r"gross margin[^%]{0,80}?was\s*([0-9]+(?:\.[0-9]+)?)%",
        release_flat,
    )
    release_operating_margin_match = _search(
        r"operating margin[^%]{0,80}?was\s*([0-9]+(?:\.[0-9]+)?)%",
        release_flat,
    )
    release_net_margin_match = _search(
        r"net (?:profit|income) margin[^%]{0,80}?was\s*([0-9]+(?:\.[0-9]+)?)%",
        release_flat,
    )
    release_eps_match = _search(
        r"diluted earnings per share of (?:NT\$|US\$|\$)?([0-9]+(?:\.[0-9]+)?)",
        release_flat,
    )
    release_revenue_bn = _bn_from_billions(release_revenue_match.group(1)) if release_revenue_match else None
    release_revenue_yoy_pct = _pct_value(release_revenue_yoy_match.group(1)) if release_revenue_yoy_match else None
    release_gross_margin_pct = _pct_value(release_gross_margin_match.group(1)) if release_gross_margin_match else None
    release_operating_margin_pct = _pct_value(release_operating_margin_match.group(1)) if release_operating_margin_match else None
    release_net_margin_pct = _pct_value(release_net_margin_match.group(1)) if release_net_margin_match else None
    release_eps = _parse_number(release_eps_match.group(1)) if release_eps_match else None
    fallback_latest_kpis = dict(fallback.get("latest_kpis") or {})
    fallback_revenue_bn = _coalesce_number(
        fallback_latest_kpis.get("revenue_bn"),
        fallback.get("revenue_bn"),
    )
    revenue_bn = _coalesce_number(
        fallback_revenue_bn,
        release_revenue_bn,
    )
    quarterly_geographies = _tsmc_quarterly_geographies(materials, revenue_bn)
    segments = _tsmc_platform_segments(company, str(fallback.get("fiscal_label") or ""), materials, revenue_bn)
    statement_metrics = _tsmc_statement_metrics(materials)
    statement_gross_margin = _coalesce_number(
        statement_metrics.get("gross_margin_pct"),
        release_gross_margin_pct,
        fallback_latest_kpis.get("gaap_gross_margin_pct"),
    )
    statement_operating_margin = _coalesce_number(
        statement_metrics.get("operating_margin_pct"),
        release_operating_margin_pct,
    )
    statement_net_margin = _coalesce_number(
        statement_metrics.get("net_margin_pct"),
        release_net_margin_pct,
    )
    statement_revenue_yoy = _coalesce_number(
        statement_metrics.get("revenue_yoy_pct"),
        release_revenue_yoy_pct,
        fallback_latest_kpis.get("revenue_yoy_pct"),
    )
    statement_net_income_yoy = _coalesce_number(
        statement_metrics.get("net_income_yoy_pct"),
        fallback_latest_kpis.get("net_income_yoy_pct"),
    )
    statement_eps = _coalesce_number(
        statement_metrics.get("gaap_eps"),
        release_eps,
        fallback_latest_kpis.get("gaap_eps"),
    )
    statement_eps_yoy = _coalesce_number(
        statement_metrics.get("gaap_eps_yoy_pct"),
        fallback_latest_kpis.get("gaap_eps_yoy_pct"),
    )
    ending_equity_bn = _coalesce_number(
        _extract_ending_equity_bn(materials),
        fallback_latest_kpis.get("ending_equity_bn"),
    )
    primary_material = presentation or release or (materials[0] if materials else None)
    guidance_material = release or presentation or primary_material
    net_income_bn = _coalesce_number(
        fallback_latest_kpis.get("net_income_bn"),
        None if revenue_bn in (None, 0) or statement_net_margin is None else float(statement_net_margin) * float(revenue_bn) / 100,
    )
    operating_income_bn = _coalesce_number(
        fallback_latest_kpis.get("operating_income_bn"),
        None if revenue_bn in (None, 0) or statement_operating_margin is None else float(revenue_bn) * float(statement_operating_margin) / 100,
    )
    facts = {
        "primary_source_label": str(primary_material.get("label") or "TSMC official materials") if primary_material else "TSMC official materials",
        "structure_source_label": str((presentation or primary_material or {}).get("label") or "TSMC official materials"),
        "guidance_source_label": str((guidance_material or {}).get("label") or "TSMC official materials"),
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": statement_revenue_yoy,
        "gross_margin_pct": statement_gross_margin,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": statement_net_income_yoy,
        "operating_cash_flow_bn": _coalesce_number(fallback_latest_kpis.get("operating_cash_flow_bn")),
        "free_cash_flow_bn": _coalesce_number(fallback_latest_kpis.get("free_cash_flow_bn")),
        "gaap_eps": statement_eps,
        "gaap_eps_yoy_pct": statement_eps_yoy,
        "non_gaap_eps": statement_eps,
        "ending_equity_bn": ending_equity_bn,
        "segments": segments,
        "geographies": quarterly_geographies,
        "guidance": _extract_generic_guidance(guidance_material) or dict(fallback.get("guidance") or {}),
        "quotes": _extract_quote_cards(release) or _extract_quote_cards(presentation),
        "driver": "平台结构与地区结构已直接从 TSMC 官方季度 presentation / financial report 动态解析",
        "coverage_notes": [
            "TSMC 的季度 KPI、结构与摘要已优先改为从官方 presentation / financial report 动态解析，而不再先跑通用全文分部扫描。"
        ],
    }
    parsed = _finalize(company, fallback, facts, materials)

    if segments:
        parsed["current_segments"] = segments
    if quarterly_geographies:
        parsed["current_geographies"] = quarterly_geographies
    if revenue_bn is not None or statement_gross_margin is not None or statement_eps is not None:
        latest_kpis = dict(parsed.get("latest_kpis") or {})
        if revenue_bn is not None:
            latest_kpis.setdefault("revenue_bn", float(revenue_bn))
        if statement_revenue_yoy is not None and latest_kpis.get("revenue_yoy_pct") is None:
            latest_kpis["revenue_yoy_pct"] = float(statement_revenue_yoy)
        if statement_gross_margin is not None:
            latest_kpis["gaap_gross_margin_pct"] = float(statement_gross_margin)
            latest_kpis.setdefault("non_gaap_gross_margin_pct", float(statement_gross_margin))
        if statement_net_income_yoy is not None and latest_kpis.get("net_income_yoy_pct") is None:
            latest_kpis["net_income_yoy_pct"] = float(statement_net_income_yoy)
        if statement_eps is not None:
            latest_kpis["gaap_eps"] = float(statement_eps)
            latest_kpis.setdefault("non_gaap_eps", float(statement_eps))
        if statement_eps_yoy is not None and latest_kpis.get("gaap_eps_yoy_pct") is None:
            latest_kpis["gaap_eps_yoy_pct"] = float(statement_eps_yoy)
        if ending_equity_bn is not None and latest_kpis.get("ending_equity_bn") is None:
            latest_kpis["ending_equity_bn"] = float(ending_equity_bn)
        parsed["latest_kpis"] = latest_kpis

    coverage_notes = list(parsed.get("coverage_notes") or [])
    if segments:
        coverage_notes.append("TSMC 的平台结构已改为从官方季度 presentation 图片页 OCR 动态解析，不再依赖预置样本。")
        if presentation is not None:
            presentation_text = _clean_text(str(presentation.get("raw_text") or ""))
            if "Revenue by Application" in presentation_text and "Revenue by Platform" not in presentation_text:
                coverage_notes.append("TSMC 早期季度仍使用 Application 分类时，系统会按官方应用结构并结合平台 crosswalk 统一映射到当前业务类型。")
    if quarterly_geographies:
        coverage_notes.append("TSMC 的地区结构已优先改为从官方季度 presentation 的 Geography 表动态解析。")
    if not parsed.get("current_geographies"):
        annual_materials = [
            item
            for item in materials
            if item.get("kind") == "sec_filing" and _is_annual_material(item)
        ]
        if not annual_materials:
            annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        annual_materials = _ensure_loaded_materials(annual_materials)
        annual_geographies = _extract_company_geographies(str(company["id"]), annual_materials, revenue_bn) if annual_materials else []
        if len(annual_geographies) >= 2:
            parsed["current_geographies"] = annual_geographies
            coverage_notes.append("TSMC 的地区结构已增加最近官方年报口径的自动映射通道。")
    if not parsed.get("current_geographies"):
        coverage_notes.append("TSMC 当前可解析的 SEC 季度附件尚未稳定提供地区收入拆分，地区结构页将保留披露口径说明。")
    parsed["coverage_notes"] = coverage_notes

    if segments:
        segment_map = {str(item.get("name") or ""): item for item in segments}
        top_segment = max(segments, key=lambda item: float(item.get("value_bn") or 0.0))
        top_share = None if revenue_bn in (None, 0) else float(top_segment["value_bn"]) / float(revenue_bn) * 100
        hpc = segment_map.get("HPC")
        smartphone = segment_map.get("Smartphone")
        guidance = dict(parsed.get("guidance") or {})
        gross_margin_match = _search(r"Gross profit margin (?:is expected to be )?between ([0-9.]+)% and ([0-9.]+)%", release_flat + " " + presentation_flat)
        operating_margin_match = _search(r"Operating profit margin (?:is expected to be )?between ([0-9.]+)% and ([0-9.]+)%", release_flat + " " + presentation_flat)
        full_year_growth_match = _search(r"2026 revenue to increase by close to ([0-9.]+)%", presentation_flat)
        long_term_cagr_match = _search(r"Revenue CAGR to approach ([0-9.]+)%", presentation_flat)
        long_term_gm_match = _search(r"Long-term gross margin to be ([0-9.]+)% and higher", presentation_flat)
        long_term_roe_match = _search(r"ROE to be high-([0-9]+)s%", presentation_flat)

        guidance_note_parts: list[str] = []
        if guidance.get("revenue_low_bn") is not None and guidance.get("revenue_high_bn") is not None:
            guidance_note_parts.append(
                f"1Q26 收入指引区间为 {format_money_bn(guidance['revenue_low_bn'])} 到 {format_money_bn(guidance['revenue_high_bn'])}"
            )
        if gross_margin_match:
            guidance_note_parts.append(f"毛利率区间 {gross_margin_match.group(1)}%-{gross_margin_match.group(2)}%")
        if operating_margin_match:
            guidance_note_parts.append(f"营业利润率区间 {operating_margin_match.group(1)}%-{operating_margin_match.group(2)}%")
        guidance_note = "，".join(guidance_note_parts) + "。" if guidance_note_parts else ""

        management_themes = [
            _theme(
                "领先制程需求",
                86,
                "管理层明确表示四季度和进入 1Q26 的经营都受到领先制程需求强劲支撑，阅读重点仍应放在先进制程与 AI 相关需求兑现。",
            ),
            _theme(
                "平台结构重心",
                82,
                (
                    f"HPC 收入约 {format_money_bn(float(hpc['value_bn']), company['money_symbol'])}，"
                    f"占比约 {format_pct(float(hpc['value_bn']) / float(revenue_bn) * 100)}；"
                    if hpc and revenue_bn not in (None, 0)
                    else ""
                )
                + (
                    f"Smartphone 收入约 {format_money_bn(float(smartphone['value_bn']), company['money_symbol'])}。"
                    if smartphone
                    else f"{top_segment['name']} 仍是当季最大平台，占比约 {format_pct(top_share)}。"
                ),
            ),
        ]
        if guidance_note:
            management_themes.append(_theme("下一季指引", 80, guidance_note))
        if full_year_growth_match or long_term_cagr_match or long_term_gm_match or long_term_roe_match:
            long_term_bits: list[str] = []
            if full_year_growth_match:
                long_term_bits.append(f"2026 年收入预计接近 +{full_year_growth_match.group(1)}%")
            if long_term_cagr_match:
                long_term_bits.append(f"2024-2029 年收入 CAGR 目标接近 {long_term_cagr_match.group(1)}%")
            if long_term_gm_match:
                long_term_bits.append(f"长期毛利率目标 {long_term_gm_match.group(1)}% 以上")
            if long_term_roe_match:
                long_term_bits.append(f"长期 ROE 目标为 high-{long_term_roe_match.group(1)}s%")
            management_themes.append(_theme("中长期框架", 76, "；".join(long_term_bits) + "。"))
        parsed["management_themes"] = management_themes[:4]

        qna_themes = [
            _theme(
                "HPC / AI 持续性",
                84,
                "问答核心会继续围绕 HPC 与 AI 需求的可持续性，以及其对 2026 年收入加速的拉动是否仍在扩散。",
            ),
            _theme(
                "毛利率兑现",
                78,
                (
                    f"本季毛利率 {format_pct(parsed.get('latest_kpis', {}).get('gaap_gross_margin_pct'))}，"
                    if parsed.get("latest_kpis", {}).get("gaap_gross_margin_pct") is not None
                    else ""
                )
                + (guidance_note if guidance_note else "市场会继续追问先进制程、利用率与平台结构变化能否支撑利润率。"),
            ),
            _theme(
                "平台轮动与手机周期",
                72,
                "Smartphone 与 HPC 的相对权重变化，会被用来判断季节性回升之外，是否还有更广的终端需求修复。",
            ),
        ]
        if parsed.get("current_geographies"):
            top_geo = max(parsed["current_geographies"], key=lambda item: float(item.get("value_bn") or 0.0))
            qna_themes.append(
                _theme(
                    "区域需求分布",
                    68,
                    f"地区结构显示 {top_geo['name']} 仍是重要出货去向，问答中通常会继续关注区域客户需求是否更集中。",
                )
            )
        parsed["qna_themes"] = qna_themes[:4]

        risks = [
            _theme(
                "先进需求集中度",
                72,
                f"{top_segment['name']} 占比约 {format_pct(top_share)}，若领先制程需求边际放缓，会比过去更快传导到整体收入曲线。"
                if top_share is not None
                else "先进平台收入集中度较高，需求边际变化会更快传导到整体收入曲线。",
            ),
            _theme(
                "利润率回落风险",
                68,
                "当前毛利率已处高位，若利用率、海外扩产爬坡或平台 mix 出现变化，利润率弹性可能先于收入承压。",
            ),
        ]
        if parsed.get("current_geographies"):
            geo_names = [str(item.get("name") or "") for item in parsed["current_geographies"][:3]]
            risks.append(_theme("区域与客户暴露", 64, f"地区收入仍集中在 {' / '.join(geo_names)} 等主要市场，区域需求与客户采购节奏波动会放大季度波动。"))
        if guidance_note:
            risks.append(_theme("指引兑现下沿", 66, guidance_note))
        parsed["risks"] = risks[:4]

        catalysts = []
        if guidance_note:
            catalysts.append(_theme("下一季指引抬升", 82, guidance_note))
        if full_year_growth_match:
            catalysts.append(_theme("2026 年加速框架", 80, f"管理层明确给出 2026 年收入接近 +{full_year_growth_match.group(1)}% 的官方框架。"))
        if hpc and revenue_bn not in (None, 0):
            catalysts.append(
                _theme(
                    "HPC 持续扩张",
                    78,
                    f"HPC 已占收入约 {format_pct(float(hpc['value_bn']) / float(revenue_bn) * 100)}，若 AI 相关需求继续扩张，会继续抬升整体结构质量。",
                )
            )
        if long_term_gm_match or long_term_roe_match:
            text_bits: list[str] = []
            if long_term_gm_match:
                text_bits.append(f"长期毛利率 {long_term_gm_match.group(1)}%+")
            if long_term_roe_match:
                text_bits.append(f"长期 ROE high-{long_term_roe_match.group(1)}s%")
            catalysts.append(_theme("长期目标锚", 70, "管理层继续维持 " + "、".join(text_bits) + " 的经营框架。"))
        parsed["catalysts"] = catalysts[:4]

        if len(management_themes) < 3:
            fallback_note = None
            if parsed.get("current_geographies"):
                top_geo = max(parsed["current_geographies"], key=lambda item: float(item.get("value_bn") or 0.0))
                fallback_note = f"地区结构显示 {top_geo['name']} 仍是主要收入去向，管理层阅读应继续结合区域需求与客户节奏来判断后续订单强度。"
            elif parsed.get("latest_kpis", {}).get("gaap_gross_margin_pct") is not None:
                fallback_note = f"当前毛利率约 {format_pct(parsed['latest_kpis']['gaap_gross_margin_pct'])}，管理层主题需要继续围绕先进制程利用率与结构 mix 的兑现来阅读。"
            if fallback_note:
                management_themes.append(_theme("地区与利润率锚点", 74, fallback_note))
        parsed["management_themes"] = management_themes[:4]
    return parsed


def _parse_costco(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    release = _pick_material(materials, kind="official_release")
    sec = _pick_material(materials, kind="sec_filing")
    if not parsed and release is None and sec is None:
        return {}

    if release is not None:
        release_text = str(release.get("raw_text") or release.get("flat_text") or "")
        revenue_bn, _revenue_prior_bn, revenue_yoy_pct = _extract_table_metric(release_text, ["Total revenue"])
        net_sales_bn, _net_sales_prior_bn, net_sales_yoy_pct = _extract_table_metric(release_text, ["Net sales"])
        membership_bn, _membership_prior_bn, membership_yoy_pct = _extract_table_metric(
            release_text,
            ["Membership fees", "Membership fee revenue"],
        )
        net_income_bn, _net_income_prior_bn, net_income_yoy_pct = _extract_table_metric(
            release_text,
            ["NET INCOME ATTRIBUTABLE TO COSTCO", "Net income attributable to Costco", "Net income"],
        )
        latest_kpis = dict(parsed.get("latest_kpis") or {})
        if latest_kpis.get("revenue_bn") is None and revenue_bn is not None:
            latest_kpis["revenue_bn"] = revenue_bn
        if latest_kpis.get("revenue_yoy_pct") is None and revenue_yoy_pct is not None:
            latest_kpis["revenue_yoy_pct"] = revenue_yoy_pct
        if latest_kpis.get("net_income_bn") is None and net_income_bn is not None:
            latest_kpis["net_income_bn"] = net_income_bn
        if latest_kpis.get("net_income_yoy_pct") is None and net_income_yoy_pct is not None:
            latest_kpis["net_income_yoy_pct"] = net_income_yoy_pct
        if latest_kpis:
            parsed["latest_kpis"] = latest_kpis
        if not parsed.get("current_segments"):
            parsed["current_segments"] = _segment_list(
                _segment("Net sales", net_sales_bn, net_sales_yoy_pct),
                _segment("Membership fees", membership_bn, membership_yoy_pct),
            )
        coverage_notes = list(parsed.get("coverage_notes") or [])
        if revenue_bn is not None:
            coverage_notes.append("Costco 老季度已优先解析 press release 表格中的 Total revenue / Net sales / Membership fees。")
        parsed["coverage_notes"] = coverage_notes

    if sec is None:
        return parsed

    flat = sec["flat_text"]
    segment_match = _search(
        r"United States Total revenue \$?\s*([0-9,]+)\s+\$?\s*([0-9,]+).*?"
        r"Canada Total revenue \$?\s*([0-9,]+)\s+\$?\s*([0-9,]+).*?"
        r"Other International Total revenue \$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)",
        flat,
    )
    if segment_match:
        geographies = [
            item
            for item in [
                _geography(
                    "United States",
                    _bn_from_millions(segment_match.group(1)),
                    _pct_change(_bn_from_millions(segment_match.group(1)), _bn_from_millions(segment_match.group(2))),
                ),
                _geography(
                    "Canada",
                    _bn_from_millions(segment_match.group(3)),
                    _pct_change(_bn_from_millions(segment_match.group(3)), _bn_from_millions(segment_match.group(4))),
                ),
                _geography(
                    "Other International",
                    _bn_from_millions(segment_match.group(5)),
                    _pct_change(_bn_from_millions(segment_match.group(5)), _bn_from_millions(segment_match.group(6))),
                ),
            ]
            if item
        ]
    else:
        sec_text = str(sec.get("raw_text") or flat)
        us_bn, us_prior_bn, us_yoy_pct = _extract_table_metric(sec_text, ["United States Operations", "United States"])
        canada_bn, canada_prior_bn, canada_yoy_pct = _extract_table_metric(sec_text, ["Canadian Operations", "Canada"])
        other_bn, other_prior_bn, other_yoy_pct = _extract_table_metric(sec_text, ["Other International Operations", "Other International"])
        geographies = [
            item
            for item in [
                _geography("United States", us_bn, us_yoy_pct if us_yoy_pct is not None else _pct_change(us_bn, us_prior_bn)),
                _geography("Canada", canada_bn, canada_yoy_pct if canada_yoy_pct is not None else _pct_change(canada_bn, canada_prior_bn)),
                _geography(
                    "Other International",
                    other_bn,
                    other_yoy_pct if other_yoy_pct is not None else _pct_change(other_bn, other_prior_bn),
                ),
            ]
            if item
        ]
    if not geographies:
        return parsed
    parsed["current_geographies"] = sorted(geographies, key=lambda item: float(item.get("value_bn") or 0.0), reverse=True)
    coverage_notes = list(parsed.get("coverage_notes") or [])
    coverage_notes.append("Costco 的地区结构已改为优先解析官方 10-Q 中的 reportable segment 收入表。")
    parsed["coverage_notes"] = coverage_notes
    return parsed


def _parse_avgo(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    sec = _pick_material(materials, kind="sec_filing")
    if sec is None:
        return parsed
    geographies = _avgo_geographies(sec["flat_text"])
    if geographies:
        parsed["current_geographies"] = geographies
        coverage_notes = list(parsed.get("coverage_notes") or [])
        coverage_notes.append("Broadcom 的地区结构已改为优先解析官方 10-Q 中按地区拆分的 revenue disaggregation 表。")
        parsed["coverage_notes"] = coverage_notes
    return parsed


def _parse_xom(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    sec = _pick_material(materials, kind="sec_filing", label_contains="10-k") or _pick_material(materials, kind="sec_filing")
    if sec is None:
        return parsed
    geographies = _xom_geographies(sec["flat_text"])
    if geographies:
        parsed["current_geographies"] = geographies
        coverage_notes = list(parsed.get("coverage_notes") or [])
        coverage_notes.append("ExxonMobil 的地区结构已补入年报中按业务分部披露的 U.S. / Non-U.S. 销售口径汇总。")
        parsed["coverage_notes"] = coverage_notes
    return parsed


def _parse_berkshire(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    parsed = _parse_generic(company, fallback, materials) or {}
    sec = _pick_material(materials, kind="sec_filing", label_contains="10-k") or _pick_material(materials, kind="sec_filing")
    if sec is None:
        return parsed
    geographies = _berkshire_geographies(sec["flat_text"])
    if geographies:
        parsed["current_geographies"] = geographies
        coverage_notes = list(parsed.get("coverage_notes") or [])
        coverage_notes.append("Berkshire 主要在年报层面披露美国收入占比，地区结构当前采用 U.S. / Other 的官方口径映射。")
        parsed["coverage_notes"] = coverage_notes
    return parsed


def _parse_jpm(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    narrative = (
        _pick_material(materials, kind="official_release", label_contains="narrative")
        or _pick_material(materials, kind="presentation", label_contains="results")
        or _pick_material(materials, kind="official_release")
        or _pick_material(materials, kind="presentation")
    )
    supplement = (
        _pick_material(materials, kind="presentation", label_contains="supplement")
        or _pick_material(materials, kind="official_release", label_contains="supplement")
        or _pick_material(materials, kind="presentation")
        or _pick_material(materials, kind="official_release")
    )
    if narrative is None or supplement is None:
        return {}
    narrative_flat = narrative["flat_text"]
    narrative_lines = [_flatten_text(line) for line in _clean_text(str(narrative.get("raw_text") or "")).splitlines()]
    supplement_flat = supplement["flat_text"]
    heading_text = " ".join(narrative_lines[:20])
    heading_match = _search(r"NET INCOME OF \$([0-9.]+)\s*BILLION.*?\(\s*\$([0-9.]+)\s*PER SHARE", heading_text)
    revenue_match = _search(r"Reported revenue of \$([0-9.]+) billion and managed revenue of \$([0-9.]+) billion", narrative_flat)
    def segment_row(label: str) -> tuple[Optional[float], Optional[float]]:
        match = _search(
            rf"{re.escape(label)}\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+\$?\s*([0-9,]+)\s+[—()0-9-]+\s+([()0-9-]+)",
            supplement_flat,
        )
        if not match:
            return (None, None)
        return (_bn_from_millions(match.group(1)), _pct_value(match.group(6)))

    ccb_bn, ccb_yoy = segment_row("Consumer & Community Banking")
    cib_bn, cib_yoy = segment_row("Commercial & Investment Bank")
    awm_bn, awm_yoy = segment_row("Asset & Wealth Management")
    annual_sec = _pick_material(materials, kind="sec_filing", label_contains="10-k") or _pick_material(materials, kind="sec_filing")
    geographies = _jpm_geographies(annual_sec["flat_text"]) if annual_sec is not None else []
    facts = {
        "primary_source_label": narrative["label"],
        "structure_source_label": supplement["label"],
        "guidance_source_label": narrative["label"],
        "revenue_bn": _bn_from_billions(revenue_match.group(1)) if revenue_match else None,
        "net_income_bn": _bn_from_billions(heading_match.group(1)) if heading_match else None,
        "gaap_eps": _parse_number(heading_match.group(2)) if heading_match else None,
        "segments": _segment_list(
            _segment("Consumer & Community Banking", ccb_bn, ccb_yoy),
            _segment("Commercial & Investment Bank", cib_bn, cib_yoy),
            _segment("Asset & Wealth Management", awm_bn, awm_yoy),
        ),
        "geographies": geographies,
        "guidance": {
            "mode": "official_context",
            "commentary": "管理层未给出明确收入指引，但继续强调美国经济韧性、消费者支出稳定以及对地缘和通胀风险保持警惕。",
        },
        "driver": "CIB、CCB 与 AWM 在当季都保持了较强韧性，市场与支付业务仍是重要增量来源",
        "quotes": [
            _quote_card(
                "Jamie Dimon",
                "The Firm concluded the year with a strong fourth quarter, generating net income of $14.7 billion excluding a significant item.",
                "管理层延续偏积极口径，同时也将 significant item 单独剥离，方便市场判断核心盈利能力。",
                narrative["label"],
            )
        ],
    }
    return _finalize(company, fallback, facts, materials)


def _parse_generic(
    company: dict[str, Any],
    fallback: dict[str, Any],
    materials: list[dict[str, Any]],
) -> dict[str, Any]:
    release = (
        _pick_material(materials, role="earnings_release")
        or _pick_material(materials, kind="official_release")
        or _pick_material(materials, role="earnings_commentary")
        or _pick_material(materials, role="earnings_presentation")
        or _pick_material(materials, kind="presentation")
    )
    commentary = _pick_material(materials, role="earnings_commentary")
    sec = _pick_material(materials, kind="sec_filing")
    primary = release or commentary or sec or (materials[0] if materials else None)
    if primary is None:
        return {}

    quarterly_sec = sec if sec and not _is_annual_material(sec) else None
    metric_materials = [item for item in _ordered_narrative_materials(materials) if item.get("kind") != "sec_filing"]
    if quarterly_sec is not None and quarterly_sec not in metric_materials:
        metric_materials.append(quarterly_sec)
    if not metric_materials:
        metric_materials = [primary]

    revenue_bn = None
    revenue_yoy_pct = None
    operating_income_bn = None
    net_income_bn = None
    net_income_yoy_pct = None
    gaap_eps = None
    non_gaap_eps = None
    gaap_eps_yoy_pct = None
    gross_margin_pct = None
    operating_cash_flow_bn = None
    free_cash_flow_bn = None
    profit_signal: dict[str, Any] = {}
    income_statement: dict[str, Any] = {}

    for material in metric_materials:
        flat = material["flat_text"]
        profit_signal = _merge_profit_signal(profit_signal, _extract_profit_signal(flat))
        if revenue_bn is None:
            revenue_bn, revenue_yoy_pct = _extract_company_level_narrative_metric(
                flat,
                r"(?:net operating\s+revenue|total\s+net\s+revenue|total\s+revenue(?:s)?|net\s+revenue(?:s)?|revenue(?:s)?|sales)",
            )
        if operating_income_bn is None:
            operating_income_bn, _ = _extract_company_level_narrative_metric(flat, r"(?:operating income|income from operations)")
        if net_income_bn is None and profit_signal.get("reported_net_income_bn") is not None:
            net_income_bn = float(profit_signal["reported_net_income_bn"])
            net_income_yoy_pct = _pct_change(
                profit_signal.get("reported_net_income_bn"),
                profit_signal.get("reported_prior_net_income_bn"),
            )
        if net_income_bn is None:
            net_income_bn, net_income_yoy_pct = _extract_company_level_narrative_metric(
                flat,
                r"(?:net income|net earnings|earnings attributable to [A-Za-z& .]+)",
            )
        if gaap_eps is None:
            if profit_signal.get("reported_eps") is not None:
                gaap_eps = float(profit_signal["reported_eps"])
                gaap_eps_yoy_pct = _pct_change(
                    profit_signal.get("reported_eps"),
                    profit_signal.get("reported_prior_eps"),
                )
            else:
                gaap_eps, gaap_eps_yoy_pct = _extract_narrative_eps(flat)
        if non_gaap_eps is None and profit_signal.get("adjusted_eps") is not None:
            non_gaap_eps = float(profit_signal["adjusted_eps"])
        if gross_margin_pct is None:
            gross_margin_pct = _extract_pct_metric(flat, ["Gross margin", "gross margin"])
        if operating_cash_flow_bn is None:
            operating_cash_flow_bn, _ = _extract_company_level_narrative_metric(
                flat,
                r"(?:operating cash flow|cash flow from operations|net cash provided by operating activities)",
            )
        if free_cash_flow_bn is None:
            free_cash_flow_bn, _ = _extract_company_level_narrative_metric(flat, r"(?:free cash flow)")

    if quarterly_sec is not None:
        sec_flat = quarterly_sec["flat_text"]
        html_statement = _extract_generic_statement_from_html_tables(
            quarterly_sec,
            str(fallback.get("calendar_quarter") or ""),
        )
        html_statement_revenue_bn = html_statement.get("revenue_bn") if html_statement else None
        if html_statement:
            if html_statement.get("revenue_bn") is not None:
                revenue_bn = float(html_statement["revenue_bn"])
            if revenue_yoy_pct is None and html_statement.get("revenue_yoy_pct") is not None:
                revenue_yoy_pct = float(html_statement["revenue_yoy_pct"])
            if operating_income_bn is None and html_statement.get("operating_income_bn") is not None:
                operating_income_bn = float(html_statement["operating_income_bn"])
            if net_income_bn is None and html_statement.get("net_income_bn") is not None:
                net_income_bn = float(html_statement["net_income_bn"])
            if net_income_yoy_pct is None and html_statement.get("net_income_yoy_pct") is not None:
                net_income_yoy_pct = float(html_statement["net_income_yoy_pct"])
            if gross_margin_pct is None and html_statement.get("gross_margin_pct") is not None:
                gross_margin_pct = float(html_statement["gross_margin_pct"])
            if not income_statement and isinstance(html_statement.get("income_statement"), dict):
                income_statement = dict(html_statement["income_statement"])
        table_revenue_bn, table_revenue_prior_bn, table_revenue_yoy_pct = _extract_table_metric(
            sec_flat,
            ["Total revenue", "Total revenues", "Total net sales", "Net sales", "Net revenues", "Revenue", "Revenues"],
        )
        if table_revenue_bn is not None and html_statement_revenue_bn is None:
            revenue_bn = table_revenue_bn
        if revenue_yoy_pct is None and table_revenue_yoy_pct is not None:
            revenue_yoy_pct = table_revenue_yoy_pct
        elif revenue_yoy_pct is None:
            revenue_yoy_pct = _pct_change(table_revenue_bn, table_revenue_prior_bn)
        operating_income_bn = operating_income_bn or _extract_table_metric(
            sec_flat,
            ["Operating income", "Income from operations"],
        )[0]
        table_net_income_bn, table_net_income_prior_bn, table_net_income_yoy_pct = _extract_table_metric(
            sec_flat,
            [
                "Net income",
                "Net income available to common shareholders",
                "Net income attributable to Walmart",
                "Net income attributable to Costco",
                "Net income attributable to common shareholders",
                "Net income attributable to Berkshire Hathaway shareholders",
            ],
        )
        if net_income_bn is None and table_net_income_bn is not None:
            net_income_bn = table_net_income_bn
        if net_income_yoy_pct is None:
            net_income_yoy_pct = table_net_income_yoy_pct if table_net_income_yoy_pct is not None else _pct_change(table_net_income_bn, table_net_income_prior_bn)
        if gross_margin_pct is None:
            gross_margin_pct = _extract_pct_metric(sec_flat, ["Gross margin", "gross margin"])
            if gross_margin_pct is None:
                gross_profit_bn = _extract_table_metric(sec_flat, ["Gross profit", "Gross margin"])[0]
                cost_of_revenue_bn = _extract_table_metric(
                    sec_flat,
                    ["Cost of sales", "Cost of revenue", "Total cost of revenue", "Total cost of sales"],
                )[0]
                if revenue_bn not in (None, 0):
                    if gross_profit_bn is not None:
                        gross_margin_pct = float(gross_profit_bn) / float(revenue_bn) * 100
                    elif cost_of_revenue_bn is not None:
                        gross_margin_pct = (float(revenue_bn) - float(cost_of_revenue_bn)) / float(revenue_bn) * 100
        if gaap_eps is None:
            gaap_eps, prior_eps = _per_share_row(sec_flat, "Diluted")
            gaap_eps_yoy_pct = _pct_change(gaap_eps, prior_eps)
        if non_gaap_eps is None and profit_signal.get("adjusted_eps") is not None:
            non_gaap_eps = float(profit_signal["adjusted_eps"])

    uses_adjusted_profit = _should_prefer_adjusted_profit_signal(profit_signal, revenue_bn)
    if uses_adjusted_profit:
        adjusted_net_income_bn = profit_signal.get("adjusted_net_income_bn")
        if adjusted_net_income_bn is not None:
            net_income_bn = float(adjusted_net_income_bn)
            adjusted_yoy_pct = _pct_change(
                profit_signal.get("adjusted_net_income_bn"),
                profit_signal.get("adjusted_prior_net_income_bn"),
            )
            if adjusted_yoy_pct is not None:
                net_income_yoy_pct = adjusted_yoy_pct
        if gaap_eps is None and profit_signal.get("reported_eps") is not None:
            gaap_eps = float(profit_signal["reported_eps"])
            gaap_eps_yoy_pct = _pct_change(
                profit_signal.get("reported_eps"),
                profit_signal.get("reported_prior_eps"),
            )
    elif net_income_bn is None and profit_signal.get("adjusted_net_income_bn") is not None:
        net_income_bn = float(profit_signal["adjusted_net_income_bn"])
        net_income_yoy_pct = _pct_change(
            profit_signal.get("adjusted_net_income_bn"),
            profit_signal.get("adjusted_prior_net_income_bn"),
        )

    ending_equity_bn = _extract_ending_equity_bn(metric_materials)
    if ending_equity_bn is None and sec is not None:
        ending_equity_bn = _extract_ending_equity_bn([sec])

    segments = _extract_company_segments(company["id"], materials, revenue_bn)
    if not segments:
        annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        if annual_materials:
            segments = _extract_company_segments(company["id"], annual_materials, revenue_bn)
    lower_ratio, upper_ratio = COMPANY_SEGMENT_RATIO_BOUNDS.get(str(company["id"]), (0.55, 1.35))
    if not _segments_reasonable_for_revenue(segments, revenue_bn, lower_ratio=lower_ratio, upper_ratio=upper_ratio):
        segments = []
    geographies = _extract_company_geographies(str(company["id"]), materials, revenue_bn)
    if not geographies:
        annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        if annual_materials:
            geographies = _extract_company_geographies(str(company["id"]), annual_materials, revenue_bn)
    guidance_material = (
        commentary
        or release
        or _pick_material(materials, role="earnings_presentation")
        or _pick_material(materials, kind="presentation")
    )
    guidance = _extract_generic_guidance(guidance_material)
    quotes = _extract_quote_cards(release) or _extract_quote_cards(commentary)
    top_segment = _top_segment(segments)
    driver = None
    if top_segment is not None:
        driver = f"{top_segment['name']} 仍是本季最主要的收入结构锚点"

    facts = {
        "primary_source_label": primary["label"],
        "structure_source_label": (release or quarterly_sec or primary)["label"],
        "guidance_source_label": (release or primary)["label"],
        "revenue_bn": revenue_bn,
        "revenue_yoy_pct": revenue_yoy_pct,
        "gross_margin_pct": gross_margin_pct,
        "operating_income_bn": operating_income_bn,
        "net_income_bn": net_income_bn,
        "net_income_yoy_pct": net_income_yoy_pct,
        "operating_cash_flow_bn": operating_cash_flow_bn,
        "free_cash_flow_bn": free_cash_flow_bn,
        "gaap_eps": gaap_eps,
        "non_gaap_eps": non_gaap_eps,
        "gaap_eps_yoy_pct": gaap_eps_yoy_pct,
        "ending_equity_bn": ending_equity_bn,
        "segments": segments,
        "geographies": geographies,
        "income_statement": income_statement,
        "guidance": guidance,
        "quotes": quotes,
        "driver": driver or "关键 KPI 已切到动态抓取的官方原文口径",
        "coverage_notes": [
            "当前公司的季度 KPI、结构与摘要已改为优先解析自动发现的官方 release / SEC filing，而不是依赖预写静态样本。"
        ]
        + (
            [
                "当季官方材料同时披露 reported 与 adjusted 利润口径且存在特殊项目扰动时，系统会优先采用 adjusted 净利润做可比展示，并保留 EPS 口径区分。"
            ]
            if uses_adjusted_profit
            else []
        ),
    }
    if revenue_bn is None and net_income_bn is None:
        return {}
    parsed = _finalize(company, fallback, facts, materials)
    if uses_adjusted_profit:
        parsed["profit_basis"] = "adjusted_special_items"
    return parsed


COMPANY_PARSERS = {
    "apple": _parse_apple,
    "microsoft": _parse_microsoft,
    "alphabet": _parse_alphabet,
    "amazon": _parse_amazon,
    "meta": _parse_meta,
    "nvidia": _parse_nvidia,
    "tsla": _parse_tsla,
    "tsmc": _parse_tsmc,
    "avgo": _parse_avgo,
    "berkshire": _parse_berkshire,
    "costco": _parse_costco,
    "walmart": _parse_walmart,
    "jnj": _parse_jnj,
    "visa": _parse_visa,
    "micron": _parse_micron,
    "asml": _parse_asml,
    "jpm": _parse_jpm,
    "xom": _parse_xom,
}

SELF_CONTAINED_PARSERS = {"tsmc"}


def parse_official_materials(
    company: dict[str, Any],
    fallback: dict[str, Any],
    source_materials: list[dict[str, Any]],
    progress_callback: Optional[ParserProgressCallback] = None,
) -> dict[str, Any]:
    fallback = _ensure_parser_context(fallback)
    materials = _load_materials(
        source_materials,
        progress_callback=_scaled_progress_callback(progress_callback, start=0.0, end=0.16),
    )
    if not materials:
        return {}
    parser = COMPANY_PARSERS.get(company["id"], _parse_generic)
    parsed: dict[str, Any] = {}
    heartbeat_stop = threading.Event()
    heartbeat_thread: Optional[threading.Thread] = None
    try:
        if progress_callback is not None:
            progress_callback(0.22, f"已载入 {len(materials)} 份材料，正在应用 {company['ticker']} 官方解析器...")
            heartbeat_thread = threading.Thread(
                target=_parser_progress_heartbeat,
                args=(progress_callback, heartbeat_stop),
                daemon=True,
            )
            heartbeat_thread.start()
        parsed = parser(company, fallback, materials)
    except Exception:
        parsed = {}
    finally:
        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=0.1)
    if parser is _parse_generic or str(company.get("id") or "") in SELF_CONTAINED_PARSERS:
        if progress_callback is not None:
            progress_callback(1.0, f"{company['ticker']} 官方解析阶段已完成。")
        return parsed
    try:
        if progress_callback is not None:
            progress_callback(0.9, "官方解析完成，正在合并通用解析结果...")
        generic = _parse_generic(company, fallback, materials)
    except Exception:
        generic = {}
    merged = _merge_parsed_payload(parsed, generic)
    current_geographies = list(merged.get("current_geographies") or [])
    should_probe_annual_geographies = (
        not current_geographies
        or len(_geography_names(current_geographies)) <= 2
        or _ambiguous_geography_profile([{"name": item.get("name")} for item in current_geographies])
    )
    if should_probe_annual_geographies:
        if progress_callback is not None:
            progress_callback(0.95, "正在比对最近官方年报中的地区收入表，优先保留更细地区口径...")
        try:
            annual_materials = _load_nearby_annual_materials(company, fallback, materials)
        except Exception:
            annual_materials = []
        annual_materials = _ensure_loaded_materials(annual_materials)
        if annual_materials:
            annual_geographies = _extract_company_geographies(
                str(company["id"]),
                annual_materials,
                merged.get("latest_kpis", {}).get("revenue_bn"),
            )
            preferred_geographies = _prefer_richer_geographies(
                current_geographies,
                list(annual_geographies or []),
                merged.get("latest_kpis", {}).get("revenue_bn"),
            )
            if preferred_geographies and preferred_geographies != current_geographies:
                merged["current_geographies"] = preferred_geographies
                coverage_notes = list(merged.get("coverage_notes") or [])
                coverage_notes.append("系统已自动比对当季与最近官方年报口径，并优先保留更细的地区收入拆分。")
                merged["coverage_notes"] = coverage_notes
    if progress_callback is not None:
        progress_callback(1.0, f"{company['ticker']} 官方解析阶段已完成。")
    return merged
