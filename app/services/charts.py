from __future__ import annotations

import html
import math
import re
import unicodedata
from typing import Any, Iterable, Optional


def format_money_bn(value: Optional[float], symbol: str = "$") -> str:
    if value is None:
        return "-"
    return f"{symbol}{value:.1f}B"


def format_pct(value: Optional[float], signed: bool = False) -> str:
    if value is None:
        return "-"
    return f"{value:+.1f}%" if signed else f"{value:.1f}%"


def _escape(value: str) -> str:
    return html.escape(str(value), quote=True)


def _svg(width: int, height: int, body: str) -> str:
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-hidden="true" '
        'xmlns="http://www.w3.org/2000/svg">'
        f"{body}</svg>"
    )


def _svg_body(svg_markup: str) -> str:
    match = re.match(r"^<svg[^>]*>(.*)</svg>$", str(svg_markup), flags=re.DOTALL)
    return match.group(1) if match else str(svg_markup)


def _polar_to_cartesian(cx: float, cy: float, radius: float, angle_deg: float) -> tuple[float, float]:
    radians = math.radians(angle_deg - 90)
    return cx + radius * math.cos(radians), cy + radius * math.sin(radians)


def _donut_segment_path(
    cx: float,
    cy: float,
    outer_radius: float,
    inner_radius: float,
    start_angle: float,
    end_angle: float,
) -> str:
    if end_angle - start_angle >= 359.99:
        return (
            f"M {cx:.1f} {cy - outer_radius:.1f} "
            f"A {outer_radius:.1f} {outer_radius:.1f} 0 1 1 {cx - 0.01:.1f} {cy - outer_radius:.1f} "
            f"L {cx - 0.01:.1f} {cy - inner_radius:.1f} "
            f"A {inner_radius:.1f} {inner_radius:.1f} 0 1 0 {cx:.1f} {cy - inner_radius:.1f} Z"
        )
    outer_start = _polar_to_cartesian(cx, cy, outer_radius, start_angle)
    outer_end = _polar_to_cartesian(cx, cy, outer_radius, end_angle)
    inner_end = _polar_to_cartesian(cx, cy, inner_radius, end_angle)
    inner_start = _polar_to_cartesian(cx, cy, inner_radius, start_angle)
    large_arc = 1 if end_angle - start_angle > 180 else 0
    return (
        f"M {outer_start[0]:.1f} {outer_start[1]:.1f} "
        f"A {outer_radius:.1f} {outer_radius:.1f} 0 {large_arc} 1 {outer_end[0]:.1f} {outer_end[1]:.1f} "
        f"L {inner_end[0]:.1f} {inner_end[1]:.1f} "
        f"A {inner_radius:.1f} {inner_radius:.1f} 0 {large_arc} 0 {inner_start[0]:.1f} {inner_start[1]:.1f} Z"
    )


def _flow_path(
    x0: float,
    y0_top: float,
    y0_bottom: float,
    x1: float,
    y1_top: float,
    y1_bottom: float,
) -> str:
    c1 = x0 + (x1 - x0) * 0.38
    c2 = x0 + (x1 - x0) * 0.62
    return (
        f"M {x0:.1f} {y0_top:.1f} "
        f"C {c1:.1f} {y0_top:.1f} {c2:.1f} {y1_top:.1f} {x1:.1f} {y1_top:.1f} "
        f"L {x1:.1f} {y1_bottom:.1f} "
        f"C {c2:.1f} {y1_bottom:.1f} {c1:.1f} {y0_bottom:.1f} {x0:.1f} {y0_bottom:.1f} Z"
    )


def _wrap_label(text: str, max_chars: int = 22) -> list[str]:
    raw = str(text)
    if len(raw) <= max_chars:
        return [raw]
    if " " not in raw:
        return [raw[index : index + max_chars] for index in range(0, len(raw), max_chars)][:2]
    words = raw.split()
    if not words:
        return [raw]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(word) > max_chars and len(current) == 0:
            chunks = [word[index : index + max_chars] for index in range(0, len(word), max_chars)]
            lines.extend(chunks[:-1])
            current = chunks[-1]
        elif len(current) + 1 + len(word) <= max_chars:
            current += f" {word}"
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines[:2]


def _wrap_text_lines(text: str, max_chars: int = 22, max_lines: int = 4) -> list[str]:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return []
    if len(raw) <= max_chars:
        return [raw]
    if " " not in raw:
        chunks = [raw[index : index + max_chars] for index in range(0, len(raw), max_chars)]
        return chunks[:max_lines]
    words = raw.split()
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(word) > max_chars and len(current) == 0:
            chunks = [word[index : index + max_chars] for index in range(0, len(word), max_chars)]
            lines.extend(chunks[:-1])
            current = chunks[-1]
        elif len(current) + 1 + len(word) <= max_chars:
            current += f" {word}"
        else:
            lines.append(current)
            if len(lines) >= max_lines:
                return lines[:max_lines]
            current = word
    lines.append(current)
    return lines[:max_lines]


def _char_visual_units(char: str) -> float:
    if not char:
        return 0.0
    if char.isspace():
        return 0.34
    width = unicodedata.east_asian_width(char)
    if width in {"W", "F"}:
        return 1.0
    if char.isupper():
        return 0.72
    if char.isdigit():
        return 0.62
    if char in {"&", "@", "%", "$"}:
        return 0.72
    if char in {"-", "+", "/", "|"}:
        return 0.42
    if char in {",", ".", ";", ":", "!", "?", "'", '"'}:
        return 0.28
    return 0.56


def _visual_units(text: str) -> float:
    return sum(_char_visual_units(char) for char in str(text or ""))


def _wrap_visual_text_lines(text: str, max_units: float = 18.0, max_lines: int = 4) -> list[str]:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return []
    lines: list[str] = []
    current = ""
    current_units = 0.0
    for char in raw:
        char_units = _char_visual_units(char)
        if current and current_units + char_units > max_units:
            break_at = current.rfind(" ")
            if break_at > 0:
                line_candidate = current[:break_at].rstrip()
                carry_candidate = current[break_at + 1 :].lstrip()
                line_units = _visual_units(line_candidate)
                carry_units = _visual_units(carry_candidate)
                if line_candidate and line_units >= max_units * 0.56 and carry_units <= max_units * 0.72:
                    line = line_candidate
                    carry = carry_candidate
                else:
                    line = current.rstrip()
                    carry = ""
            else:
                line = current.rstrip()
                carry = ""
            lines.append(line)
            if len(lines) >= max_lines:
                return lines[:max_lines]
            current = carry
            current_units = sum(_char_visual_units(item) for item in current)
            if char != " ":
                if current and not current.endswith(" "):
                    current += char
                else:
                    current = (current + char).lstrip()
                current_units = sum(_char_visual_units(item) for item in current)
            continue
        current += char
        current_units += char_units
    if current:
        lines.append(current.rstrip())
    return lines[:max_lines]


def _text_block(
    x: float,
    y: float,
    lines: list[str],
    *,
    font_size: float,
    fill: str,
    weight: int = 400,
    anchor: str = "start",
    line_height: float = 14,
) -> str:
    if not lines:
        return ""
    tspans = []
    for index, line in enumerate(lines):
        dy = 0 if index == 0 else line_height
        tspans.append(f'<tspan x="{x:.1f}" dy="{dy:.1f}">{_escape(line)}</tspan>')
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-size="{font_size}" '
        f'font-weight="{weight}" fill="{fill}">{"".join(tspans)}</text>'
    )


def _condense_statement_sources(
    sources: list[dict[str, Any]],
    accent: str,
    *,
    min_share_pct: float = 7.5,
    max_items: int = 5,
) -> list[dict[str, Any]]:
    if len(sources) <= max_items:
        return sources
    total = sum(float(item.get("value_bn") or 0.0) for item in sources) or 1.0
    keep_indexes: set[int] = set()
    for index, item in sorted(
        enumerate(sources),
        key=lambda pair: float(pair[1].get("value_bn") or 0.0),
        reverse=True,
    ):
        value_bn = float(item.get("value_bn") or 0.0)
        share_pct = value_bn / total * 100
        if share_pct < min_share_pct:
            continue
        keep_indexes.add(index)
        if len(keep_indexes) >= max_items - 1:
            break
    condensed: list[dict[str, Any]] = []
    other_value = 0.0
    other_names: list[str] = []
    for index, item in enumerate(sources):
        value_bn = float(item.get("value_bn") or 0.0)
        if index in keep_indexes:
            condensed.append(item)
        else:
            other_value += value_bn
            other_names.append(str(item.get("name") or "Other"))
    if other_value > 0:
        condensed.append(
            {
                "name": "Other",
                "value_bn": other_value,
                "fill": "#CBD5E1",
                "color": accent,
                "note": ", ".join(other_names[:3]),
            }
        )
    return condensed or sources[:max_items]


def _condense_opex_items(
    items: list[dict[str, Any]],
    *,
    max_items: int = 4,
    min_share_pct: float = 5.0,
) -> list[dict[str, Any]]:
    if len(items) <= max_items:
        return items
    total = sum(float(item.get("value_bn") or 0.0) for item in items) or 1.0
    condensed: list[dict[str, Any]] = []
    other_value = 0.0
    other_pct = 0.0
    for item in items:
        value_bn = float(item.get("value_bn") or 0.0)
        share_pct = value_bn / total * 100
        if share_pct >= min_share_pct and len(condensed) < max_items - 1:
            condensed.append(item)
            continue
        other_value += value_bn
        other_pct += float(item.get("pct_of_revenue") or 0.0)
    if other_value > 0:
        condensed.append(
            {
                "name": "Other operating items",
                "value_bn": other_value,
                "pct_of_revenue": other_pct if other_pct > 0 else None,
                "color": "#BE123C",
            }
        )
    return condensed or items[:max_items]


def _series_points(values: list[float], left: float, top: float, width: float, height: float) -> tuple[str, list[tuple[float, float]]]:
    if not values:
        return "", []
    min_value = min(values)
    max_value = max(values)
    if math.isclose(max_value, min_value):
        max_value += 1
        min_value -= 1
    step_x = width / max(1, len(values) - 1)
    points: list[tuple[float, float]] = []
    for index, value in enumerate(values):
        x = left + index * step_x
        y = top + height - ((value - min_value) / (max_value - min_value) * height)
        points.append((x, y))
    path = "M " + " L ".join(f"{x:.1f} {y:.1f}" for x, y in points)
    return path, points


def render_current_quarter_svg(metrics: list[dict[str, Any]], primary: str, secondary: str) -> str:
    width, height = 940, 304
    body = [
        '<rect x="0" y="0" width="940" height="304" rx="26" fill="#FFFFFF"/>',
        '<text x="32" y="34" font-size="18" font-weight="700" fill="#0F172A">当季 vs 上季 vs 去年同期</text>',
    ]
    group_left = 42
    card_width = 272
    chart_top = 102
    chart_height = 96
    bar_width = 42
    for index, item in enumerate(metrics):
        left = group_left + index * (card_width + 16)
        values = [float(item.get(key) or 0) for key in ("year_ago", "previous", "current")]
        local_max = max(values) or 1
        body.append(f'<rect x="{left}" y="52" width="{card_width}" height="212" rx="22" fill="#F8FAFC" stroke="#E2E8F0"/>')
        body.append(
            f'<text x="{left + 18}" y="78" font-size="14" font-weight="600" fill="#0F172A">{_escape(item["label"])}</text>'
        )
        for offset, key, color in [
            (0, "year_ago", "#CBD5E1"),
            (44, "previous", secondary),
            (88, "current", primary),
        ]:
            value = float(item.get(key) or 0)
            bar_height = value / local_max * chart_height
            x = left + 28 + offset
            y = chart_top + chart_height - bar_height
            body.append(f'<rect x="{x}" y="{y:.1f}" width="{bar_width}" height="{bar_height:.1f}" rx="14" fill="{color}"/>')
            label_y = max(y - 10, chart_top - 8)
            body.append(
                f'<text x="{x + bar_width/2:.1f}" y="{label_y:.1f}" text-anchor="middle" font-size="11" fill="#0F172A">{_escape(item["labels"][key])}</text>'
            )
            label_x = x + bar_width / 2
            body.append(f'<text x="{label_x:.1f}" y="228" text-anchor="middle" font-size="11" fill="#64748B">{"去年同期" if key == "year_ago" else "上季" if key == "previous" else "当季"}</text>')
        body.append(
            f'<text x="{left + 18}" y="252" font-size="12" fill="#94A3B8">同比变化 {format_pct(float(item["delta_yoy"]), signed=True)}</text>'
        )
    return _svg(width, height, "".join(body))


def render_guidance_svg(
    current_revenue: Optional[float],
    guidance_revenue: Optional[float],
    current_margin: Optional[float],
    guidance_margin: Optional[float],
    primary: str,
    accent: str,
    money_symbol: str = "$",
    current_label: str = "本季收入",
    comparison_label: str = "下一季收入指引",
    current_margin_label: str = "本季毛利率",
    comparison_margin_label: str = "下一季毛利率指引",
    chart_title: str = "当前业绩与下一季指引",
) -> str:
    width, height = 940, 244
    revenue_values = [float(value) for value in (current_revenue, guidance_revenue) if value is not None]
    max_revenue = max(revenue_values) * 1.1 if revenue_values else 1.0
    rev_scale = 290 / max_revenue
    current_revenue_width = float(current_revenue) * rev_scale if current_revenue is not None else 0.0
    guidance_revenue_width = float(guidance_revenue) * rev_scale if guidance_revenue is not None else 0.0
    has_revenue_comparison = current_revenue not in (None, 0) and guidance_revenue is not None
    revenue_delta = ((float(guidance_revenue) / float(current_revenue) - 1) * 100) if has_revenue_comparison else None
    margin_delta = (float(guidance_margin) - float(current_margin)) if current_margin is not None and guidance_margin is not None else None
    body = [
        '<rect x="0" y="0" width="940" height="244" rx="26" fill="#FFFFFF"/>',
        f'<text x="32" y="36" font-size="18" font-weight="700" fill="#0F172A">{_escape(chart_title)}</text>',
        f'<text x="42" y="74" font-size="12" fill="#64748B">{_escape(current_label)}</text>',
        f'<rect x="42" y="84" width="{current_revenue_width:.1f}" height="28" rx="14" fill="{primary}"/>',
        f'<text x="{42 + current_revenue_width + 12:.1f}" y="104" font-size="13" fill="#0F172A">{format_money_bn(current_revenue, money_symbol)}</text>',
        f'<text x="42" y="132" font-size="12" fill="#64748B">{_escape(comparison_label)}</text>',
        f'<rect x="42" y="142" width="{guidance_revenue_width:.1f}" height="28" rx="14" fill="{accent}"/>',
        f'<text x="{42 + guidance_revenue_width + 12:.1f}" y="162" font-size="13" fill="#0F172A">{format_money_bn(guidance_revenue, money_symbol)}</text>',
        '<rect x="532" y="56" width="366" height="146" rx="22" fill="#F8FAFC" stroke="#E2E8F0"/>',
        f'<text x="560" y="92" font-size="12" fill="#64748B">{_escape(current_margin_label)}</text>',
        f'<text x="560" y="130" font-size="32" font-weight="700" fill="#0F172A">{format_pct(current_margin)}</text>',
        f'<text x="728" y="92" font-size="12" fill="#64748B">{_escape(comparison_margin_label)}</text>',
        f'<text x="728" y="130" font-size="32" font-weight="700" fill="#0F172A">{format_pct(guidance_margin)}</text>',
        f'<text x="560" y="166" font-size="12" fill="#94A3B8">收入环比对照 {format_pct(revenue_delta, signed=True)}</text>',
        f'<text x="728" y="166" font-size="12" fill="#94A3B8">利润率变化 {format_pct(margin_delta, signed=True)}</text>',
    ]
    if not revenue_values:
        body.extend(
            [
                '<rect x="42" y="84" width="290" height="86" rx="20" fill="#F8FAFC" stroke="#E2E8F0" stroke-dasharray="6 6"/>',
                '<text x="62" y="118" font-size="13" font-weight="600" fill="#334155">当前季度与下一阶段收入口径暂缺</text>',
                '<text x="62" y="142" font-size="12" fill="#64748B">系统已保留利润率与文字指引摘要，避免因单项缺失导致整页失败。</text>',
            ]
        )
    elif current_revenue is None or guidance_revenue is None:
        missing_label = current_label if current_revenue is None else comparison_label
        body.append(
            f'<text x="42" y="198" font-size="12" fill="#94A3B8">{_escape(missing_label)}未披露明确数值，图表已按现有字段保留。</text>'
        )
    return _svg(width, height, "".join(body))


def _compact_mix_items(items: list[dict[str, Any]], max_items: int = 5) -> list[dict[str, Any]]:
    visible_items = [item for item in items if float(item.get("value_bn") or 0.0) > 0]
    if len(visible_items) <= max_items:
        return visible_items
    keep_indexes = {
        index
        for index, _ in sorted(
            enumerate(visible_items),
            key=lambda pair: float(pair[1].get("value_bn") or 0.0),
            reverse=True,
        )[: max_items - 1]
    }
    kept = [item for index, item in enumerate(visible_items) if index in keep_indexes]
    other_value = sum(float(item.get("value_bn") or 0.0) for index, item in enumerate(visible_items) if index not in keep_indexes)
    if other_value > 0:
        kept.append({"name": "Other", "value_bn": other_value})
    return kept


def _compact_category_label(value: str) -> str:
    label = str(value or "").replace(" and ", " & ")
    label = label.replace("Generation", "Gen.")
    label = label.replace("iPhone & related products & services", "iPhone & related products")
    label = label.replace("iPad & related products & services", "iPad & related products")
    label = label.replace("Other music related products & services", "Other music & services")
    label = label.replace("Software, service, & other sales", "Software, services & other")
    label = label.replace("Peripherals & other hardware", "Peripherals & hardware")
    label = label.replace("subscriptions, platforms, & devices", "subscriptions, platforms & devices")
    label = label.replace("subscriptions, platforms & devices", "Subs, platforms & dev.")
    label = label.replace("Productivity & Business Processes", "Productivity & Biz")
    label = label.replace("Productivity and Business Processes", "Productivity & Biz")
    label = label.replace("More Personal Computing", "Personal Computing")
    label = label.replace("Wearables, Home & Accessories", "Wearables & Acc.")
    label = label.replace("Wearables, Home and Accessories", "Wearables & Acc.")
    label = label.replace("Google Search & other", "Search & other")
    label = label.replace("Google subscriptions, platforms & devices", "Subs, platforms & dev.")
    label = label.replace("Google subscriptions, platforms, & devices", "Subs, platforms & dev.")
    label = label.replace("Google subscriptions, platforms, and devices", "Subs, platforms & dev.")
    label = label.replace("Google Subs, platforms & dev.", "Subs, platforms & dev.")
    label = label.replace("Google Subs, platforms & devices", "Subs, platforms & devices")
    return label


def _donut_panel_svg(
    title: str,
    subtitle: str,
    items: list[dict[str, Any]],
    palette: dict[str, str],
    accent: str,
    money_symbol: str,
    empty_note: str,
    center_label: str = "当季营收",
) -> str:
    width, height = 560, 342
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="26" fill="#FFFFFF"/>',
        f'<rect x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="25.5" fill="none" stroke="#E2E8F0"/>',
        f'<text x="24" y="34" font-size="15.2" font-weight="700" fill="#0F172A">{_escape(title)}</text>',
        f'<text x="24" y="52" font-size="10.8" fill="#64748B">{_escape(subtitle)}</text>',
    ]
    compact_items = _compact_mix_items(items, max_items=6)
    if not compact_items:
        body.extend(
            [
                f'<rect x="22" y="72" width="{width - 44}" height="218" rx="22" fill="#F8FAFC" stroke="#E2E8F0"/>',
                f'<text x="40" y="108" font-size="16" font-weight="700" fill="{accent}">当前未稳定披露</text>',
                _text_block(40, 132, _wrap_label(empty_note, 44), font_size=11.4, fill="#334155", line_height=15),
            ]
        )
        return _svg(width, height, "".join(body))

    total = sum(float(item.get("value_bn") or 0.0) for item in compact_items) or 1.0
    top_item = max(compact_items, key=lambda item: float(item.get("value_bn") or 0.0))
    top_share = float(top_item.get("value_bn") or 0.0) / total * 100
    cx, cy = 156, 190
    outer_radius, inner_radius = 101, 58
    body.append(f'<circle cx="{cx}" cy="{cy}" r="{outer_radius}" fill="#F8FAFC" stroke="#E2E8F0" stroke-width="16"/>')
    start_angle = 0.0
    for item in compact_items:
        value_bn = float(item.get("value_bn") or 0.0)
        share = value_bn / total * 100
        item_name = str(item.get("name") or "")
        color = str(palette.get(item_name) or item.get("color") or ("#64748B" if item_name == "Other" else accent))
        end_angle = start_angle + share * 3.6
        body.append(
            f'<path d="{_donut_segment_path(cx, cy, outer_radius, inner_radius, start_angle, end_angle)}" fill="{color}" stroke="#FFFFFF" stroke-width="2"/>'
        )
        start_angle = end_angle
    body.extend(
        [
            f'<text x="{cx:.1f}" y="{cy - 18:.1f}" text-anchor="middle" font-size="11" fill="#64748B">{_escape(center_label)}</text>',
            f'<text x="{cx:.1f}" y="{cy + 8:.1f}" text-anchor="middle" font-size="23" font-weight="700" fill="#0F172A">{format_money_bn(total, money_symbol)}</text>',
            f'<text x="{cx:.1f}" y="{cy + 32:.1f}" text-anchor="middle" font-size="10.6" fill="#94A3B8">头部占比 {format_pct(top_share)}</text>',
        ]
    )
    row_cursor = 70.0 if len(compact_items) >= 6 else 78.0 if len(compact_items) >= 5 else 84.0
    label_width = 17 if len(compact_items) >= 6 else 19 if len(compact_items) >= 5 else 22
    detail_width = 24 if len(compact_items) >= 6 else 26 if len(compact_items) >= 5 else 28
    label_font = 10.4 if len(compact_items) >= 6 else 10.9 if len(compact_items) >= 5 else 11.4
    detail_font = 9.8 if len(compact_items) >= 6 else 10.2 if len(compact_items) >= 5 else 10.8
    label_line_height = 10.8 if len(compact_items) >= 6 else 11.2
    detail_line_height = 10.2 if len(compact_items) >= 6 else 10.6
    for index, item in enumerate(compact_items[:6]):
        value_bn = float(item.get("value_bn") or 0.0)
        share = value_bn / total * 100
        item_name = str(item.get("name") or "")
        color = str(palette.get(item_name) or item.get("color") or ("#64748B" if item_name == "Other" else accent))
        label = _compact_category_label(item_name or "-")
        label_lines = _wrap_label(label, label_width)[:3]
        detail_lines = _wrap_label(f"{format_money_bn(value_bn, money_symbol)} | 占比 {format_pct(share)}", detail_width)[:2]
        row_y = row_cursor
        details_y = row_y + label_line_height * len(label_lines) + 2
        body.extend(
            [
                f'<circle cx="300" cy="{row_y + 5:.1f}" r="8.2" fill="{color}"/>',
                _text_block(318, row_y - 6, label_lines, font_size=label_font, fill="#0F172A", weight=700, line_height=label_line_height),
                _text_block(318, details_y, detail_lines, font_size=detail_font, fill="#475569", line_height=detail_line_height),
            ]
        )
        row_cursor = details_y + detail_line_height * len(detail_lines) + (10 if len(compact_items) >= 6 else 12)
    return _svg(width, height, "".join(body))


def render_segment_mix_svg(
    segments: list[dict[str, Any]],
    geographies: list[dict[str, Any]],
    colors: dict[str, str],
    accent: str,
    money_symbol: str = "$",
) -> str:
    segment_regional_mode = bool(segments) and all(
        str(item.get("scope") or "").casefold() == "regional_segment" for item in segments
    )
    business_names = [str(item.get("name") or "Business") for item in segments]
    business_palette = _segment_palette(business_names, colors, accent)
    annual_geography_mode = bool(geographies) and all(str(item.get("scope") or "") == "annual_filing" for item in geographies)
    geography_regional_mode = bool(geographies) and all(
        str(item.get("scope") or "").casefold() == "regional_segment" for item in geographies
    )
    duplicate_regional_panels = (
        geography_regional_mode
        and bool(segments)
        and {
            (
                str(item.get("name") or "").casefold(),
                round(float(item.get("value_bn") or 0.0), 3),
            )
            for item in segments
        }
        == {
            (
                str(item.get("name") or "").casefold(),
                round(float(item.get("value_bn") or 0.0), 3),
            )
            for item in geographies
        }
    )
    business_svg = _donut_panel_svg(
        "区域经营分部结构" if segment_regional_mode else "业务营收结构",
        "公司按区域经营分部披露，这里直接保留官方分部口径，不把它误写成营收类型。"
        if segment_regional_mode
        else "优先采用财报分部口径，保持品牌映射一致。"
        if not duplicate_regional_panels
        else "公司按区域经营分部披露时，这里直接保留官方分部口径。",
        segments,
        business_palette,
        accent,
        money_symbol,
        "当前缺少稳定分部披露，系统已改为总量成长、利润质量与管理层结构摘要。",
        center_label="经营分部" if segment_regional_mode else "当季营收",
    )
    geography_svg = _donut_panel_svg(
        "地区营收结构",
        "公司未单列终端地理收入，因此这一栏不重复展示与分部完全相同的区域口径。"
        if duplicate_regional_panels
        else
        "若财报披露地理口径，则自动补入第二张圆环结构图。"
        if not annual_geography_mode and not geography_regional_mode
        else "若公司按区域经营分部披露，则采用区域经营分部口径，并显式区分于业务分部。"
        if geography_regional_mode
        else "若当季未单独披露，则回退到最近年报地理口径，并显式标注为年度视角。",
        [] if duplicate_regional_panels else geographies,
        _geography_palette([str(item.get("name") or "Geography") for item in geographies], accent),
        accent,
        money_symbol,
        "当前季度未在已接入官方材料中发现稳定地区收入拆分，因此本页保留业务结构主图。"
        if not duplicate_regional_panels
        else "当前官方材料只披露了区域经营分部，没有单独给出终端地理收入拆分。",
        center_label="当季营收"
        if not annual_geography_mode and not geography_regional_mode
        else "区域经营分部"
        if geography_regional_mode
        else "年度地区口径",
    )
    return _svg(
        1140,
        342,
        f'<g transform="translate(0,0)">{_svg_body(business_svg)}</g>'
        f'<g transform="translate(580,0)">{_svg_body(geography_svg)}</g>',
    )


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    cleaned = color.strip().lstrip("#")
    if len(cleaned) == 3:
        cleaned = "".join(channel * 2 for channel in cleaned)
    if len(cleaned) != 6:
        return (15, 23, 42)
    return tuple(int(cleaned[index : index + 2], 16) for index in (0, 2, 4))


def _mix_hex(color_a: str, color_b: str, blend: float) -> str:
    blend = max(0.0, min(1.0, blend))
    red_a, green_a, blue_a = _hex_to_rgb(color_a)
    red_b, green_b, blue_b = _hex_to_rgb(color_b)
    mixed = (
        round(red_a + (red_b - red_a) * blend),
        round(green_a + (green_b - green_a) * blend),
        round(blue_a + (blue_b - blue_a) * blend),
    )
    return f"#{mixed[0]:02X}{mixed[1]:02X}{mixed[2]:02X}"


def _color_distance(color_a: str, color_b: str) -> float:
    red_a, green_a, blue_a = _hex_to_rgb(color_a)
    red_b, green_b, blue_b = _hex_to_rgb(color_b)
    return math.sqrt((red_a - red_b) ** 2 + (green_a - green_b) ** 2 + (blue_a - blue_b) ** 2)


def _default_segment_colors() -> list[str]:
    return [
        "#2563EB",
        "#0EA5E9",
        "#14B8A6",
        "#F59E0B",
        "#8B5CF6",
        "#E11D48",
        "#84CC16",
        "#F97316",
        "#06B6D4",
        "#D946EF",
        "#22C55E",
        "#3B82F6",
        "#64748B",
    ]


def _distinct_palette(
    item_names: list[str],
    preferred_colors: dict[str, str],
    primary: str,
    *,
    other_color: str = "#CBD5E1",
    min_distance: float = 72.0,
) -> dict[str, str]:
    palette: dict[str, str] = {}
    used_colors: list[str] = []
    fallback_candidates = _default_segment_colors() + [
        _mix_hex(primary, "#FFFFFF", 0.18),
        _mix_hex(primary, "#FFFFFF", 0.34),
        _mix_hex(primary, "#0F172A", 0.18),
        "#0F766E",
        "#C2410C",
    ]
    for index, name in enumerate(item_names):
        if name == "Other":
            palette[name] = other_color
            used_colors.append(other_color)
            continue
        preferred = str(preferred_colors.get(name) or "").strip()
        if preferred and all(_color_distance(preferred, used) >= min_distance for used in used_colors):
            palette[name] = preferred
            used_colors.append(preferred)
            continue
        chosen = ""
        for candidate in fallback_candidates[index % len(fallback_candidates) :] + fallback_candidates[: index % len(fallback_candidates)]:
            if all(_color_distance(candidate, used) >= min_distance for used in used_colors):
                chosen = candidate
                break
        if not chosen:
            chosen = preferred or fallback_candidates[index % len(fallback_candidates)] or primary
        palette[name] = chosen
        used_colors.append(chosen)
    return palette


def _segment_palette(segment_names: list[str], colors: dict[str, str], primary: str) -> dict[str, str]:
    return _distinct_palette(segment_names, colors, primary)


def _geography_palette(geography_names: list[str], accent: str) -> dict[str, str]:
    preferred = {
        "Americas": "#2563EB",
        "United States": "#2563EB",
        "North America": "#2563EB",
        "Europe": "#0F766E",
        "EMEA": "#0F766E",
        "Greater China": "#D97706",
        "China": "#F59E0B",
        "China (including Hong Kong)": "#F59E0B",
        "Hong Kong": "#F97316",
        "Taiwan": "#14B8A6",
        "Japan": "#E11D48",
        "APAC": "#7C3AED",
        "APJ": "#7C3AED",
        "Rest of Asia Pacific": "#8B5CF6",
        "International": _mix_hex(accent, "#FFFFFF", 0.28),
        "Other": "#CBD5E1",
    }
    return _distinct_palette(geography_names, preferred, accent)


def _statement_source_lane_height(card_height: float, dense_layout: bool) -> float:
    if dense_layout:
        return max(14.0, min(card_height - 18.0, 22.0))
    return max(16.0, min(card_height - 20.0, 28.0))


def _statement_source_color(item_name: str, palette: dict[str, str], fallback: str) -> str:
    return palette.get(item_name) or fallback


def _statement_detail_text(value_bn: float, revenue_bn: float, yoy_pct: Optional[float]) -> str:
    parts = [f"{format_pct(value_bn / revenue_bn * 100 if revenue_bn else None)} rev"]
    if yoy_pct is not None:
        parts.append(format_pct(yoy_pct, signed=True))
    return " | ".join(parts)


def _compact_fiscal_label(label: Optional[str]) -> str:
    if not label:
        return "-"
    compact = str(label).replace("Fiscal", "FY").replace("fiscal", "FY")
    compact = compact.replace("FY20", "FY")
    compact = compact.replace("FY1", "FY1")
    if "FY" in compact and "Q" in compact:
        tokens = compact.replace(",", " ").split()
        quarter = next((token for token in tokens if token.startswith("Q")), None)
        fiscal = next((token for token in tokens if token.startswith("FY")), None)
        if quarter and fiscal:
            return f"{quarter} {fiscal}"
    return compact


def _statement_period_badge(period_end: Optional[str]) -> str:
    if not period_end or len(period_end) < 7:
        return "Official period"
    month_labels = {
        "01": "Jan.",
        "02": "Feb.",
        "03": "Mar.",
        "04": "Apr.",
        "05": "May",
        "06": "Jun.",
        "07": "Jul.",
        "08": "Aug.",
        "09": "Sept.",
        "10": "Oct.",
        "11": "Nov.",
        "12": "Dec.",
    }
    year, month = period_end[:7].split("-")
    return f"Ending {month_labels.get(month, month)} {year}"


def _normalize_flow_items(
    items: list[dict[str, Any]],
    target_total: float,
    *,
    fallback_name: str,
    fallback_color: str,
) -> list[dict[str, Any]]:
    if target_total <= 0:
        return []
    cleaned: list[dict[str, Any]] = []
    for raw in items:
        value_bn = float(raw.get("value_bn") or 0.0)
        if value_bn <= 0.03:
            continue
        item = dict(raw)
        item["value_bn"] = value_bn
        item["_flow_value_bn"] = value_bn
        cleaned.append(item)
    if not cleaned:
        return [{"name": fallback_name, "value_bn": target_total, "_flow_value_bn": target_total, "color": fallback_color}]
    actual_total = sum(float(item["value_bn"]) for item in cleaned)
    if actual_total <= 0:
        return [{"name": fallback_name, "value_bn": target_total, "_flow_value_bn": target_total, "color": fallback_color}]
    difference = target_total - actual_total
    tolerance = max(0.2, target_total * 0.06)
    if difference > tolerance:
        cleaned.append(
            {
                "name": fallback_name,
                "value_bn": difference,
                "_flow_value_bn": difference,
                "color": fallback_color,
            }
        )
    elif difference < -tolerance:
        scale = target_total / actual_total
        for item in cleaned:
            item["_flow_value_bn"] = float(item["value_bn"]) * scale
    elif cleaned:
        cleaned[-1]["_flow_value_bn"] = max(float(cleaned[-1]["_flow_value_bn"]) + difference, 0.0)
    return cleaned


def _statement_annotation_items(statement: dict[str, Any], money_symbol: str) -> list[dict[str, Any]]:
    chosen: list[dict[str, Any]] = []
    for item in list(statement.get("annotations") or []):
        if len(chosen) >= 3:
            break
        value = str(item.get("value") or "").strip()
        if not value or value == "-":
            continue
        chosen.append(item)
    fallbacks = [
        {
            "title": "Revenue growth",
            "value": format_pct(statement.get("revenue_yoy_pct"), signed=True),
            "note": f"Quarterly revenue reached {format_money_bn(statement.get('revenue_bn'), money_symbol)}.",
            "color": "#2563EB",
        },
        {
            "title": "Gross margin",
            "value": format_pct(statement.get("gross_margin_pct")),
            "note": "Gross profit remains the clearest read-through for monetization quality.",
            "color": "#16A34A",
        },
        {
            "title": "Net margin",
            "value": format_pct(statement.get("net_margin_pct")),
            "note": f"Net profit landed at {format_money_bn(statement.get('net_profit_bn'), money_symbol)}.",
            "color": "#0F172A",
        },
    ]
    existing_titles = {str(item.get("title") or "") for item in chosen}
    for item in fallbacks:
        if len(chosen) >= 3:
            break
        if item["title"] in existing_titles or item["value"] == "-":
            continue
        chosen.append(item)
    return chosen[:3]


_COMMON_STATEMENT_TRANSLATIONS = {
    "Revenue": "收入",
    "Total revenue": "总收入",
    "Total net sales": "总净销售额",
    "Net sales": "净销售额",
    "Products": "产品收入",
    "Product revenue": "产品收入",
    "Services": "服务收入",
    "Service and other revenue": "服务及其他收入",
    "Services and other": "服务及其他",
    "Cost of revenue": "营收成本",
    "Total cost of revenue": "总营收成本",
    "Cost of sales": "销售成本",
    "Gross profit": "毛利润",
    "Gross margin": "毛利率",
    "Operating expenses": "经营费用",
    "Operating profit": "经营利润",
    "Operating income": "经营利润",
    "Net profit": "净利润",
    "Net income": "净利润",
    "Research and development": "研发费用",
    "Sales and marketing": "销售与营销",
    "Marketing and sales": "市场与销售",
    "Selling, general and administrative": "销售、一般及行政费用",
    "General and administrative": "一般及行政费用",
    "Technology and infrastructure": "技术与基础设施",
    "Fulfillment": "履约成本",
    "Restructuring and other": "重组及其他",
    "Other operating": "其他经营项",
    "Other operating items": "其他经营项",
    "Other income": "其他收益",
    "Tax & Other": "税项及其他",
    "Tax & other": "税项及其他",
    "Tax and other": "税项及其他",
    "Operating cash flow": "经营现金流",
    "Free cash flow": "自由现金流",
    "Other": "其他",
    "Software": "软件",
    "Hardware": "硬件",
    "Cloud": "云业务",
    "Cloud revenue": "云业务收入",
    "Advertising": "广告",
}


_COMPANY_STATEMENT_TRANSLATIONS = {
    "microsoft": {
        "Productivity and Business Processes": "生产力与业务流程",
        "Intelligent Cloud": "智能云",
        "More Personal Computing": "更多个人计算",
    },
    "apple": {
        "iPhone": "iPhone",
        "Mac": "Mac",
        "iPad": "iPad",
        "Wearables, Home and Accessories": "可穿戴设备、家居与配件",
        "Services": "服务收入",
        "Products": "产品收入",
    },
    "alphabet": {
        "Google Search & other": "Google 搜索及其他",
        "YouTube ads": "YouTube 广告",
        "Google Network": "Google 网络",
        "Google subscriptions, platforms, and devices": "Google 订阅、平台与设备",
        "Google Cloud": "Google 云",
        "Other Bets": "Other Bets 创新业务",
    },
    "amazon": {
        "North America": "北美",
        "International": "国际",
        "AWS": "AWS 云服务",
    },
    "meta": {
        "Family of Apps": "应用家族",
        "Reality Labs": "Reality Labs",
    },
    "nvidia": {
        "Data Center": "数据中心",
        "Gaming": "游戏",
        "Professional Visualization": "专业可视化",
        "Automotive": "汽车",
    },
    "tsla": {
        "Automotive": "汽车业务",
        "Energy Generation and Storage": "能源发电与储能",
        "Services and other": "服务及其他",
        "Automotive regulatory credits": "汽车监管积分",
    },
    "avgo": {
        "Semiconductor Solutions": "半导体解决方案",
        "Infrastructure Software": "基础设施软件",
    },
    "baba": {
        "China commerce": "中国商业",
        "International commerce": "国际商业",
        "Cloud intelligence": "云智能",
        "Cainiao": "菜鸟",
        "Local services": "本地生活",
        "Digital media and entertainment": "数字媒体与娱乐",
    },
}


def _translate_statement_label(label: Any, company_id: str = "") -> str:
    raw = str(label or "").strip()
    if not raw:
        return "-"
    company_map = _COMPANY_STATEMENT_TRANSLATIONS.get(str(company_id or ""), {})
    if raw in company_map:
        return company_map[raw]
    if raw in _COMMON_STATEMENT_TRANSLATIONS:
        return _COMMON_STATEMENT_TRANSLATIONS[raw]
    lowered = raw.lower()
    if lowered.startswith("taxes net of"):
        return "税项净额及其他"
    if "tax" in lowered and "other" in lowered:
        return "税项及其他"
    if lowered == "other":
        return "其他"
    suffix_map = {
        " revenue": "收入",
        " profit": "利润",
        " income": "收益",
        " margin": "利润率",
    }
    for suffix, translated in suffix_map.items():
        if lowered.endswith(suffix):
            prefix = raw[: -len(suffix)].strip()
            return f"{prefix} {translated}".strip()
    return raw


def _statement_meta_text(item: dict[str, Any], revenue_bn: Optional[float]) -> str:
    parts: list[str] = []
    value_bn = float(item.get("value_bn") or 0.0)
    if item.get("pct_of_revenue") is not None:
        parts.append(f"占收入 {format_pct(item.get('pct_of_revenue'))}")
    elif revenue_bn not in (None, 0) and value_bn > 0:
        parts.append(f"占收入 {format_pct(value_bn / float(revenue_bn) * 100)}")
    if item.get("yoy_pct") is not None:
        parts.append(f"同比 {format_pct(item.get('yoy_pct'), signed=True)}")
    elif item.get("margin_pct") is not None:
        parts.append(f"利润率 {format_pct(item.get('margin_pct'))}")
    return " | ".join(parts[:2])


def _statement_bridge_display_name(label: Any) -> str:
    raw = str(label or "").strip()
    lowered = raw.lower()
    if lowered.startswith("research and development"):
        return "R&D"
    if lowered.startswith("selling, general and administrative"):
        return "SG&A"
    if lowered.startswith("sales and marketing"):
        return "Sales & marketing"
    if lowered.startswith("general and administrative"):
        return "General & admin"
    if lowered.startswith("technology and infrastructure"):
        return "Tech & infra"
    if lowered.startswith("taxes net of other income"):
        return "Tax & other"
    if lowered.startswith("tax & other"):
        return "Tax & other"
    if lowered.startswith("other operating"):
        return "Other opex"
    return raw


def _resolve_vertical_positions(
    centers: list[float],
    item_height: float,
    top: float,
    bottom: float,
    gap: float,
) -> list[float]:
    if not centers:
        return []
    positions = [max(top, min(center - item_height / 2, bottom - item_height)) for center in centers]
    for index in range(1, len(positions)):
        positions[index] = max(positions[index], positions[index - 1] + item_height + gap)
    overflow = positions[-1] + item_height - bottom
    if overflow > 0:
        positions = [value - overflow for value in positions]
        for index in range(len(positions) - 2, -1, -1):
            positions[index] = min(positions[index], positions[index + 1] - item_height - gap)
        positions[0] = max(positions[0], top)
        for index in range(1, len(positions)):
            positions[index] = max(positions[index], positions[index - 1] + item_height + gap)
    return positions


def _aggregate_statement_bridge(
    items: list[dict[str, Any]],
    *,
    fallback_name: str,
    color: str,
) -> Optional[dict[str, Any]]:
    if not items:
        return None
    cleaned = [item for item in items if float(item.get("value_bn") or 0.0) > 0.03]
    if not cleaned:
        return None
    if len(cleaned) == 1:
        item = dict(cleaned[0])
        item.setdefault("color", color)
        return item
    total = sum(float(item.get("value_bn") or 0.0) for item in cleaned)
    return {"name": fallback_name, "value_bn": total, "color": color}


def _statement_row_svg(
    x: float,
    y: float,
    width: float,
    height: float,
    *,
    label_cn: str,
    label_en: str,
    value_text: str,
    meta_text: str,
    accent: str,
    fill: str,
    icon_key: Optional[str] = None,
) -> str:
    compact = height < 50
    label_lines = _wrap_visual_text_lines(
        label_cn,
        8.8 if compact and icon_key else 9.6 if icon_key else 10.2 if compact else 11.8,
        2,
    )
    english_lines = _wrap_visual_text_lines(label_en, 16.5 if compact else 20.5, 2)
    meta_excerpt = _trim_note(meta_text, 24 if compact else 34)
    label_font = 10.6 if compact else 12.2
    label_line_height = 9.8 if compact else 11.2
    english_font = 8.8
    english_line_height = 8.8
    label_block_height = label_font + max(len(label_lines) - 1, 0) * label_line_height
    content_mid = y + height / 2
    show_english = (not compact) and bool(english_lines) and len(label_lines) == 1 and height >= 54
    english_block_height = english_font + max(len(english_lines) - 1, 0) * english_line_height
    combined_block_height = label_block_height + (english_block_height + 5.5 if show_english else 0.0)
    block_top = y + max(6.0 if compact else 8.0, (height - combined_block_height) / 2)
    label_y = block_top + label_font * 0.86
    english_y = block_top + label_block_height + 5.5 + english_font * 0.84
    value_y = content_mid + (1.2 if meta_excerpt else 5.6 if compact else 6.2)
    meta_y = y + height - (4.6 if compact else 7.8)
    body = [
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height:.1f}" rx="18" fill="{fill}" stroke="{_mix_hex(accent, "#CBD5E1", 0.58)}"/>',
        f'<rect x="{x + 12:.1f}" y="{y + (8 if compact else 12):.1f}" width="4" height="{max(height - (16 if compact else 24), 12):.1f}" rx="2" fill="{accent}"/>',
    ]
    label_x = x + 26
    if icon_key:
        icon_radius = 11 if compact else 14
        icon_size = 14 if compact else 18
        icon_y = content_mid
        body.append(
            f'<circle cx="{x + 38:.1f}" cy="{icon_y:.1f}" r="{icon_radius:.1f}" fill="#FFFFFF" stroke="{_mix_hex(accent, "#CBD5E1", 0.55)}"/>'
        )
        body.append(_logo_icon(x + (31 if compact else 29), icon_y - icon_size / 2, icon_size, icon_key))
        label_x = x + (54 if compact else 58)
    value_font = 16 if compact and len(value_text) <= 10 else 14 if compact else 20 if len(value_text) <= 10 else 17 if len(value_text) <= 14 else 14
    body.append(
        _text_block(
            label_x,
            label_y,
            label_lines,
            font_size=label_font,
            fill="#0F172A",
            weight=700,
            line_height=label_line_height,
        )
    )
    if show_english:
        body.append(
            _text_block(
                label_x,
                english_y,
                english_lines,
                font_size=english_font,
                fill="#64748B",
                line_height=english_line_height,
            )
        )
    body.append(
        f'<text x="{x + width - 16:.1f}" y="{value_y:.1f}" text-anchor="end" font-size="{value_font}" font-weight="800" fill="#0F172A">{_escape(value_text)}</text>'
    )
    if meta_excerpt:
        body.append(
            f'<text x="{x + width - 16:.1f}" y="{meta_y:.1f}" text-anchor="end" font-size="{8.8 if compact else 10.8}" font-weight="600" fill="#475569">{_escape(meta_excerpt)}</text>'
        )
    return "".join(body)


def _statement_metric_tile_svg(
    x: float,
    y: float,
    width: float,
    height: float,
    *,
    label_cn: str,
    label_en: str,
    value_text: str,
    note_text: str,
    accent: str,
) -> str:
    fill = _mix_hex(accent, "#FFFFFF", 0.9)
    label_lines = _wrap_visual_text_lines(label_cn, 10.2, 2)
    note_lines = _wrap_visual_text_lines(note_text, 15.6, 2)
    value_fill = _mix_hex(accent, "#0F172A", 0.18)
    value_font = 18.8 if len(value_text) <= 10 else 16.4 if len(value_text) <= 14 else 13.8
    label_font = 11.8
    label_line_height = 10.8
    note_font = 9.6
    note_line_height = 9.8
    label_y = y + 35
    english_y = label_y + label_font + max(len(label_lines) - 1, 0) * label_line_height + 4.5
    value_y = y + (74 if len(label_lines) == 1 else 80)
    note_y = y + height - 10.5 - (len(note_lines) - 1) * note_line_height
    return "".join(
        [
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height:.1f}" rx="22" fill="{fill}" stroke="{_mix_hex(accent, "#CBD5E1", 0.55)}"/>',
            f'<rect x="{x + 16:.1f}" y="{y + 16:.1f}" width="34" height="4" rx="2" fill="{accent}"/>',
            _text_block(x + 16, label_y, label_lines, font_size=label_font, fill="#0F172A", weight=700, line_height=label_line_height),
            f'<text x="{x + 16:.1f}" y="{english_y:.1f}" font-size="9.0" fill="#64748B">{_escape(_trim_note(label_en, 18))}</text>',
            f'<text x="{x + 16:.1f}" y="{value_y:.1f}" font-size="{value_font}" font-weight="800" fill="{value_fill}">{_escape(value_text)}</text>',
            _text_block(x + 16, note_y, note_lines, font_size=note_font, fill="#475569", line_height=note_line_height),
        ]
    )


def _keyword_logo_key(label: str) -> str:
    lowered = label.lower()
    if "cloud" in lowered or "azure" in lowered or "aws" in lowered:
        return "cloud"
    if "search" in lowered:
        return "search"
    if "youtube" in lowered or "video" in lowered:
        return "play"
    if "xbox" in lowered or "gaming" in lowered:
        return "gaming"
    if "window" in lowered or "personal computing" in lowered or "device" in lowered:
        return "windows"
    if "services" in lowered or "productivity" in lowered or "software" in lowered:
        return "suite"
    if "iphone" in lowered or "phone" in lowered:
        return "phone"
    if "ipad" in lowered or "tablet" in lowered:
        return "tablet"
    if "mac" in lowered or "pc" in lowered or "laptop" in lowered:
        return "laptop"
    if "wearable" in lowered or "watch" in lowered:
        return "wearable"
    if "family of apps" in lowered or "social" in lowered:
        return "chat"
    if "reality" in lowered or "vision" in lowered:
        return "vr"
    if "automotive" in lowered or "car" in lowered:
        return "car"
    if "energy" in lowered or "storage" in lowered:
        return "bolt"
    if "data center" in lowered or "semiconductor" in lowered or "chip" in lowered:
        return "chip"
    if "advertising" in lowered or "ads" in lowered:
        return "megaphone"
    if "international" in lowered:
        return "globe"
    if "north america" in lowered or "stores" in lowered or "seller" in lowered:
        return "cart"
    return "generic"


def _business_group_presentation(company_id: str, item: dict[str, Any]) -> dict[str, Any]:
    name = str(item.get("name") or "Business")
    mapping = {
        "microsoft": {
            "Productivity and Business Processes": {
                "display_name": "Productivity & Biz",
                "chips": [
                    {"key": "microsoft-four", "label": "Microsoft 365"},
                    {"key": "linkedin", "label": "LinkedIn"},
                ],
            },
            "Intelligent Cloud": {
                "display_name": "Intelligent Cloud",
                "chips": [{"key": "azure", "label": "Azure"}],
            },
            "More Personal Computing": {
                "display_name": "Personal Computing",
                "chips": [
                    {"key": "windows", "label": "Windows"},
                    {"key": "xbox", "label": "Xbox"},
                ],
            },
        },
        "apple": {
            "iPhone": {"display_name": "iPhone", "chips": [{"key": "phone", "label": "iPhone"}]},
            "Services": {"display_name": "Services", "chips": [{"key": "suite", "label": "Services"}]},
            "Mac": {"display_name": "Mac", "chips": [{"key": "laptop", "label": "Mac"}]},
            "iPad": {"display_name": "iPad", "chips": [{"key": "tablet", "label": "iPad"}]},
            "Wearables, Home and Accessories": {
                "display_name": "Wearables & Acc.",
                "chips": [{"key": "wearable", "label": "Wearables"}],
            },
        },
        "alphabet": {
            "Google Search & other": {
                "display_name": "Search & other",
                "chips": [{"key": "search", "label": "Google Search"}],
            },
            "YouTube ads": {
                "display_name": "YouTube ads",
                "chips": [{"key": "play", "label": "YouTube"}],
            },
            "Google Network": {
                "display_name": "Google Network",
                "chips": [{"key": "megaphone", "label": "Network"}],
            },
            "Google subscriptions, platforms, and devices": {
                "display_name": "Subs, platforms & dev.",
                "chips": [{"key": "suite", "label": "Subscriptions"}],
            },
            "Google Cloud": {
                "display_name": "Google Cloud",
                "chips": [{"key": "cloud", "label": "Google Cloud"}],
            },
            "Other Bets": {
                "display_name": "Other Bets",
                "chips": [{"key": "generic", "label": "Other Bets"}],
            },
        },
        "amazon": {
            "AWS": {"chips": [{"key": "cloud", "label": "AWS"}]},
            "North America": {"chips": [{"key": "cart", "label": "North America"}]},
            "International": {"chips": [{"key": "globe", "label": "International"}]},
        },
        "meta": {
            "Family of Apps": {"chips": [{"key": "chat", "label": "Family of Apps"}]},
            "Reality Labs": {"chips": [{"key": "vr", "label": "Reality Labs"}]},
        },
        "tsla": {
            "Automotive": {"chips": [{"key": "car", "label": "Automotive"}]},
            "Energy Generation and Storage": {"chips": [{"key": "bolt", "label": "Energy"}]},
        },
        "nvidia": {
            "Data Center": {"chips": [{"key": "chip", "label": "Data Center"}]},
            "Gaming": {"chips": [{"key": "gaming", "label": "Gaming"}]},
        },
    }
    meta = mapping.get(company_id, {}).get(name, {})
    chip_labels = item.get("chip_labels")
    chips = meta.get("chips")
    if isinstance(chip_labels, list) and chip_labels:
        chips = [{"key": item.get("logo_key") or _keyword_logo_key(str(label)), "label": str(label)} for label in chip_labels]
    if not chips:
        chips = [{"key": item.get("logo_key") or _keyword_logo_key(name), "label": name if len(name) <= 18 else name.split()[0]}]
    return {
        **item,
        "display_name": str(item.get("display_name") or meta.get("display_name") or name),
        "chips": chips,
    }


def _logo_icon(x: float, y: float, size: float, key: str) -> str:
    if key == "microsoft-four":
        gap = size * 0.12
        square = (size - gap) / 2 - gap / 2
        return (
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{square:.1f}" height="{square:.1f}" rx="2.2" fill="#F25022"/>'
            f'<rect x="{x + square + gap:.1f}" y="{y:.1f}" width="{square:.1f}" height="{square:.1f}" rx="2.2" fill="#7FBA00"/>'
            f'<rect x="{x:.1f}" y="{y + square + gap:.1f}" width="{square:.1f}" height="{square:.1f}" rx="2.2" fill="#00A4EF"/>'
            f'<rect x="{x + square + gap:.1f}" y="{y + square + gap:.1f}" width="{square:.1f}" height="{square:.1f}" rx="2.2" fill="#FFB900"/>'
        )
    if key == "linkedin":
        return (
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{size:.1f}" height="{size:.1f}" rx="{size * 0.22:.1f}" fill="#0A66C2"/>'
            f'<text x="{x + size / 2:.1f}" y="{y + size * 0.73:.1f}" text-anchor="middle" font-size="{size * 0.56:.1f}" font-weight="700" fill="#FFFFFF">in</text>'
        )
    if key == "azure":
        return (
            f'<path d="M {x + size * 0.12:.1f} {y + size * 0.92:.1f} L {x + size * 0.48:.1f} {y + size * 0.08:.1f} L {x + size * 0.88:.1f} {y + size * 0.92:.1f} Z" fill="#0078D4"/>'
            f'<path d="M {x + size * 0.42:.1f} {y + size * 0.52:.1f} L {x + size * 0.63:.1f} {y + size * 0.92:.1f} L {x + size * 0.28:.1f} {y + size * 0.92:.1f} Z" fill="#52B3FF"/>'
        )
    if key == "windows":
        pane = size * 0.38
        gap = size * 0.08
        return (
            f'<rect x="{x:.1f}" y="{y + size * 0.06:.1f}" width="{pane:.1f}" height="{pane:.1f}" fill="#0078D4"/>'
            f'<rect x="{x + pane + gap:.1f}" y="{y:.1f}" width="{pane:.1f}" height="{pane:.1f}" fill="#60A5FA"/>'
            f'<rect x="{x:.1f}" y="{y + pane + gap:.1f}" width="{pane:.1f}" height="{pane:.1f}" fill="#0EA5E9"/>'
            f'<rect x="{x + pane + gap:.1f}" y="{y + pane + gap:.1f}" width="{pane:.1f}" height="{pane:.1f}" fill="#38BDF8"/>'
        )
    if key == "xbox":
        return (
            f'<circle cx="{x + size / 2:.1f}" cy="{y + size / 2:.1f}" r="{size * 0.48:.1f}" fill="#107C10"/>'
            f'<path d="M {x + size * 0.24:.1f} {y + size * 0.26:.1f} Q {x + size * 0.50:.1f} {y + size * 0.46:.1f} {x + size * 0.76:.1f} {y + size * 0.26:.1f}" fill="none" stroke="#FFFFFF" stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
            f'<path d="M {x + size * 0.24:.1f} {y + size * 0.78:.1f} Q {x + size * 0.50:.1f} {y + size * 0.54:.1f} {x + size * 0.76:.1f} {y + size * 0.78:.1f}" fill="none" stroke="#FFFFFF" stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
        )
    if key == "cloud":
        return (
            f'<circle cx="{x + size * 0.38:.1f}" cy="{y + size * 0.58:.1f}" r="{size * 0.20:.1f}" fill="#0EA5E9"/>'
            f'<circle cx="{x + size * 0.58:.1f}" cy="{y + size * 0.48:.1f}" r="{size * 0.24:.1f}" fill="#38BDF8"/>'
            f'<circle cx="{x + size * 0.76:.1f}" cy="{y + size * 0.62:.1f}" r="{size * 0.18:.1f}" fill="#60A5FA"/>'
            f'<rect x="{x + size * 0.24:.1f}" y="{y + size * 0.58:.1f}" width="{size * 0.58:.1f}" height="{size * 0.22:.1f}" rx="{size * 0.11:.1f}" fill="#0EA5E9"/>'
        )
    if key == "suite":
        return (
            f'<rect x="{x + size * 0.08:.1f}" y="{y + size * 0.08:.1f}" width="{size * 0.84:.1f}" height="{size * 0.84:.1f}" rx="{size * 0.20:.1f}" fill="#2563EB"/>'
            f'<path d="M {x + size * 0.24:.1f} {y + size * 0.30:.1f} H {x + size * 0.76:.1f}" stroke="#FFFFFF" stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
            f'<path d="M {x + size * 0.24:.1f} {y + size * 0.50:.1f} H {x + size * 0.76:.1f}" stroke="#FFFFFF" stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
            f'<path d="M {x + size * 0.24:.1f} {y + size * 0.70:.1f} H {x + size * 0.64:.1f}" stroke="#FFFFFF" stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
        )
    if key == "phone":
        return (
            f'<rect x="{x + size * 0.24:.1f}" y="{y + size * 0.08:.1f}" width="{size * 0.52:.1f}" height="{size * 0.84:.1f}" rx="{size * 0.14:.1f}" fill="#111827"/>'
            f'<rect x="{x + size * 0.32:.1f}" y="{y + size * 0.18:.1f}" width="{size * 0.36:.1f}" height="{size * 0.54:.1f}" rx="{size * 0.05:.1f}" fill="#FFFFFF"/>'
            f'<circle cx="{x + size * 0.50:.1f}" cy="{y + size * 0.81:.1f}" r="{size * 0.04:.1f}" fill="#FFFFFF"/>'
        )
    if key == "laptop":
        return (
            f'<rect x="{x + size * 0.18:.1f}" y="{y + size * 0.18:.1f}" width="{size * 0.64:.1f}" height="{size * 0.46:.1f}" rx="{size * 0.06:.1f}" fill="#1F2937"/>'
            f'<rect x="{x + size * 0.24:.1f}" y="{y + size * 0.24:.1f}" width="{size * 0.52:.1f}" height="{size * 0.34:.1f}" rx="{size * 0.03:.1f}" fill="#E2E8F0"/>'
            f'<rect x="{x + size * 0.08:.1f}" y="{y + size * 0.68:.1f}" width="{size * 0.84:.1f}" height="{size * 0.10:.1f}" rx="{size * 0.05:.1f}" fill="#64748B"/>'
        )
    if key == "tablet":
        return (
            f'<rect x="{x + size * 0.18:.1f}" y="{y + size * 0.08:.1f}" width="{size * 0.64:.1f}" height="{size * 0.84:.1f}" rx="{size * 0.12:.1f}" fill="#0F172A"/>'
            f'<rect x="{x + size * 0.26:.1f}" y="{y + size * 0.16:.1f}" width="{size * 0.48:.1f}" height="{size * 0.60:.1f}" rx="{size * 0.04:.1f}" fill="#E2E8F0"/>'
        )
    if key == "wearable":
        return (
            f'<rect x="{x + size * 0.34:.1f}" y="{y + size * 0.02:.1f}" width="{size * 0.32:.1f}" height="{size * 0.96:.1f}" rx="{size * 0.16:.1f}" fill="#94A3B8"/>'
            f'<rect x="{x + size * 0.22:.1f}" y="{y + size * 0.22:.1f}" width="{size * 0.56:.1f}" height="{size * 0.56:.1f}" rx="{size * 0.16:.1f}" fill="#0F172A"/>'
            f'<circle cx="{x + size * 0.50:.1f}" cy="{y + size * 0.50:.1f}" r="{size * 0.10:.1f}" fill="#FFFFFF"/>'
        )
    if key == "search":
        return (
            f'<circle cx="{x + size * 0.45:.1f}" cy="{y + size * 0.45:.1f}" r="{size * 0.24:.1f}" fill="none" stroke="#2563EB" stroke-width="{size * 0.12:.1f}"/>'
            f'<path d="M {x + size * 0.62:.1f} {y + size * 0.62:.1f} L {x + size * 0.86:.1f} {y + size * 0.86:.1f}" stroke="#2563EB" stroke-width="{size * 0.12:.1f}" stroke-linecap="round"/>'
        )
    if key == "play":
        return (
            f'<rect x="{x + size * 0.08:.1f}" y="{y + size * 0.16:.1f}" width="{size * 0.84:.1f}" height="{size * 0.68:.1f}" rx="{size * 0.18:.1f}" fill="#DC2626"/>'
            f'<path d="M {x + size * 0.42:.1f} {y + size * 0.32:.1f} L {x + size * 0.42:.1f} {y + size * 0.68:.1f} L {x + size * 0.70:.1f} {y + size * 0.50:.1f} Z" fill="#FFFFFF"/>'
        )
    if key == "chat":
        return (
            f'<path d="M {x + size * 0.12:.1f} {y + size * 0.20:.1f} H {x + size * 0.88:.1f} V {y + size * 0.70:.1f} H {x + size * 0.52:.1f} L {x + size * 0.34:.1f} {y + size * 0.88:.1f} V {y + size * 0.70:.1f} H {x + size * 0.12:.1f} Z" fill="#0866FF"/>'
            f'<circle cx="{x + size * 0.34:.1f}" cy="{y + size * 0.45:.1f}" r="{size * 0.06:.1f}" fill="#FFFFFF"/>'
            f'<circle cx="{x + size * 0.50:.1f}" cy="{y + size * 0.45:.1f}" r="{size * 0.06:.1f}" fill="#FFFFFF"/>'
            f'<circle cx="{x + size * 0.66:.1f}" cy="{y + size * 0.45:.1f}" r="{size * 0.06:.1f}" fill="#FFFFFF"/>'
        )
    if key == "vr":
        return (
            f'<rect x="{x + size * 0.10:.1f}" y="{y + size * 0.30:.1f}" width="{size * 0.80:.1f}" height="{size * 0.40:.1f}" rx="{size * 0.16:.1f}" fill="#111827"/>'
            f'<rect x="{x + size * 0.18:.1f}" y="{y + size * 0.38:.1f}" width="{size * 0.24:.1f}" height="{size * 0.18:.1f}" rx="{size * 0.08:.1f}" fill="#A5B4FC"/>'
            f'<rect x="{x + size * 0.58:.1f}" y="{y + size * 0.38:.1f}" width="{size * 0.24:.1f}" height="{size * 0.18:.1f}" rx="{size * 0.08:.1f}" fill="#A5B4FC"/>'
        )
    if key == "car":
        return (
            f'<path d="M {x + size * 0.18:.1f} {y + size * 0.60:.1f} L {x + size * 0.30:.1f} {y + size * 0.36:.1f} H {x + size * 0.72:.1f} L {x + size * 0.84:.1f} {y + size * 0.60:.1f} Z" fill="#DC2626"/>'
            f'<circle cx="{x + size * 0.32:.1f}" cy="{y + size * 0.70:.1f}" r="{size * 0.09:.1f}" fill="#111827"/>'
            f'<circle cx="{x + size * 0.70:.1f}" cy="{y + size * 0.70:.1f}" r="{size * 0.09:.1f}" fill="#111827"/>'
        )
    if key == "bolt":
        return f'<path d="M {x + size * 0.56:.1f} {y + size * 0.04:.1f} L {x + size * 0.24:.1f} {y + size * 0.52:.1f} H {x + size * 0.48:.1f} L {x + size * 0.40:.1f} {y + size * 0.96:.1f} L {x + size * 0.76:.1f} {y + size * 0.44:.1f} H {x + size * 0.54:.1f} Z" fill="#16A34A"/>'
    if key == "chip":
        return (
            f'<rect x="{x + size * 0.20:.1f}" y="{y + size * 0.20:.1f}" width="{size * 0.60:.1f}" height="{size * 0.60:.1f}" rx="{size * 0.10:.1f}" fill="#16A34A"/>'
            f'<rect x="{x + size * 0.34:.1f}" y="{y + size * 0.34:.1f}" width="{size * 0.32:.1f}" height="{size * 0.32:.1f}" rx="{size * 0.04:.1f}" fill="#DCFCE7"/>'
        )
    if key == "megaphone":
        return (
            f'<path d="M {x + size * 0.18:.1f} {y + size * 0.42:.1f} L {x + size * 0.70:.1f} {y + size * 0.24:.1f} V {y + size * 0.76:.1f} L {x + size * 0.18:.1f} {y + size * 0.58:.1f} Z" fill="#F97316"/>'
            f'<rect x="{x + size * 0.16:.1f}" y="{y + size * 0.46:.1f}" width="{size * 0.12:.1f}" height="{size * 0.20:.1f}" rx="{size * 0.04:.1f}" fill="#FDBA74"/>'
        )
    if key == "cart":
        return (
            f'<path d="M {x + size * 0.16:.1f} {y + size * 0.24:.1f} H {x + size * 0.28:.1f} L {x + size * 0.36:.1f} {y + size * 0.58:.1f} H {x + size * 0.80:.1f} L {x + size * 0.88:.1f} {y + size * 0.34:.1f}" fill="none" stroke="#111827" stroke-width="{size * 0.10:.1f}" stroke-linecap="round" stroke-linejoin="round"/>'
            f'<circle cx="{x + size * 0.44:.1f}" cy="{y + size * 0.78:.1f}" r="{size * 0.08:.1f}" fill="#111827"/>'
            f'<circle cx="{x + size * 0.74:.1f}" cy="{y + size * 0.78:.1f}" r="{size * 0.08:.1f}" fill="#111827"/>'
        )
    if key == "globe":
        return (
            f'<circle cx="{x + size / 2:.1f}" cy="{y + size / 2:.1f}" r="{size * 0.44:.1f}" fill="none" stroke="#2563EB" stroke-width="{size * 0.10:.1f}"/>'
            f'<path d="M {x + size * 0.12:.1f} {y + size * 0.50:.1f} H {x + size * 0.88:.1f}" stroke="#2563EB" stroke-width="{size * 0.08:.1f}" stroke-linecap="round"/>'
            f'<path d="M {x + size * 0.50:.1f} {y + size * 0.12:.1f} V {y + size * 0.88:.1f}" stroke="#2563EB" stroke-width="{size * 0.08:.1f}" stroke-linecap="round"/>'
        )
    if key == "gaming":
        return (
            f'<path d="M {x + size * 0.20:.1f} {y + size * 0.64:.1f} Q {x + size * 0.22:.1f} {y + size * 0.30:.1f} {x + size * 0.44:.1f} {y + size * 0.34:.1f} H {x + size * 0.56:.1f} Q {x + size * 0.78:.1f} {y + size * 0.30:.1f} {x + size * 0.80:.1f} {y + size * 0.64:.1f} Z" fill="#0F172A"/>'
            f'<circle cx="{x + size * 0.38:.1f}" cy="{y + size * 0.50:.1f}" r="{size * 0.05:.1f}" fill="#FFFFFF"/>'
            f'<circle cx="{x + size * 0.64:.1f}" cy="{y + size * 0.46:.1f}" r="{size * 0.05:.1f}" fill="#FFFFFF"/>'
        )
    return (
        f'<rect x="{x + size * 0.08:.1f}" y="{y + size * 0.08:.1f}" width="{size * 0.84:.1f}" height="{size * 0.84:.1f}" rx="{size * 0.20:.1f}" fill="#CBD5E1"/>'
        f'<text x="{x + size / 2:.1f}" y="{y + size * 0.70:.1f}" text-anchor="middle" font-size="{size * 0.52:.1f}" font-weight="700" fill="#475569">•</text>'
    )


def _render_logo_chip(x: float, y: float, label: str, key: str) -> tuple[float, str]:
    width = max(72, min(140, 32 + len(label) * 6.3))
    height = 26
    icon_size = 16
    body = [
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height}" rx="13" fill="#FFFFFF" stroke="#D9E2EC"/>',
        _logo_icon(x + 8, y + 5, icon_size, key),
        f'<text x="{x + 30:.1f}" y="{y + 17.4:.1f}" font-size="11" font-weight="600" fill="#334155">{_escape(label)}</text>',
    ]
    return width, "".join(body)


def _company_wordmark_svg(company_id: str, company_name: str, x: float, y: float, primary: str) -> str:
    name = str(company_name or company_id or "").strip() or "Company"
    lowered = str(company_id or "").strip().lower()
    body: list[str] = []
    if lowered == "microsoft":
        body.append(_logo_icon(x - 44, y + 2, 16, "microsoft-four"))
        body.append(f'<text x="{x - 16:.1f}" y="{y + 17:.1f}" text-anchor="start" font-size="16.5" font-weight="700" fill="#111827">Microsoft</text>')
        return "".join(body)
    if lowered == "alphabet":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="17.5" font-weight="700" fill="#202124">Alphabet</text>'
    if lowered == "meta":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="18" font-weight="700" fill="#0866FF">Meta</text>'
    if lowered == "amazon":
        body.append(f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="18" font-weight="800" fill="#111827">amazon</text>')
        body.append(
            f'<path d="M {x - 26:.1f} {y + 25:.1f} Q {x:.1f} {y + 38:.1f} {x + 28:.1f} {y + 24:.1f}" fill="none" stroke="#F59E0B" stroke-width="2.4" stroke-linecap="round"/>'
        )
        body.append(
            f'<path d="M {x + 22:.1f} {y + 21:.1f} L {x + 28:.1f} {y + 24:.1f} L {x + 21:.1f} {y + 28:.1f}" fill="none" stroke="#F59E0B" stroke-width="2.0" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        return "".join(body)
    if lowered == "apple":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="18" font-weight="700" fill="#111827">Apple</text>'
    if lowered == "tesla":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="17.5" font-weight="700" letter-spacing="0.12em" fill="#CC0000">TESLA</text>'
    if lowered == "nvidia":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="18" font-weight="700" fill="#76B900">NVIDIA</text>'
    if lowered == "oracle":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="18" font-weight="700" fill="#C74634">ORACLE</text>'
    if lowered == "visa":
        return f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="18" font-weight="800" fill="#1A1F71">VISA</text>'
    fallback_fill = _mix_hex(primary, "#0F172A", 0.18)
    display = name if len(name) <= 16 else str(company_id or name).upper()
    body.append(f'<text x="{x:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-size="17" font-weight="700" letter-spacing="0.04em" fill="{fallback_fill}">{_escape(display)}</text>')
    return "".join(body)


def render_company_wordmark_svg(company_id: str, company_name: str, primary: str) -> str:
    width, height = 220, 46
    return _svg(width, height, _company_wordmark_svg(company_id, company_name, width / 2, 10, primary))


def _microsoft_corporate_logo(x: float, y: float, square: float = 17.0) -> str:
    gap = square * 0.18
    return (
        f'<g transform="translate({x:.1f},{y:.1f})">'
        f'<rect x="0" y="0" width="{square:.1f}" height="{square:.1f}" fill="#F25022"/>'
        f'<rect x="{square + gap:.1f}" y="0" width="{square:.1f}" height="{square:.1f}" fill="#7FBA00"/>'
        f'<rect x="0" y="{square + gap:.1f}" width="{square:.1f}" height="{square:.1f}" fill="#00A4EF"/>'
        f'<rect x="{square + gap:.1f}" y="{square + gap:.1f}" width="{square:.1f}" height="{square:.1f}" fill="#FFB900"/>'
        "</g>"
    )


def _microsoft_business_lockup(name: str, x: float, y: float) -> str:
    if name == "Productivity and Business Processes":
        return (
            f'<g transform="translate({x:.1f},{y:.1f})">'
            '<rect x="0" y="0" width="10" height="10" fill="#F25022"/>'
            '<rect x="12" y="0" width="10" height="10" fill="#7FBA00"/>'
            '<rect x="0" y="12" width="10" height="10" fill="#00A4EF"/>'
            '<rect x="12" y="12" width="10" height="10" fill="#FFB900"/>'
            '<text x="32" y="9" font-size="10.5" font-weight="700" fill="#6B7280">Microsoft 365</text>'
            '<rect x="32" y="13" width="12" height="12" rx="2.5" fill="#0A66C2"/>'
            '<text x="38" y="21.5" text-anchor="middle" font-size="7.5" font-weight="700" fill="#FFFFFF">in</text>'
            '<text x="50" y="23" font-size="11.5" font-weight="700" fill="#2563EB">LinkedIn</text>'
            "</g>"
        )
    if name == "Intelligent Cloud":
        return (
            f'<g transform="translate({x:.1f},{y:.1f})">'
            '<path d="M18 0 L36 56 L27 56 L22 40 L9 40 L2 56 L0 56 L16 0 Z" fill="#2B63C6"/>'
            '<path d="M26 0 L58 0 L33 56 L24 56 Z" fill="#2DB3F1" opacity="0.92"/>'
            '<path d="M26 27 L30 40 L21 40 Z" fill="#FFFFFF" opacity="0.92"/>'
            '<text x="64" y="16" font-size="11" font-weight="700" fill="#6B7280">Azure</text>'
            "</g>"
        )
    if name == "More Personal Computing":
        return (
            f'<g transform="translate({x:.1f},{y:.1f})">'
            '<rect x="0" y="10" width="18" height="18" fill="#1C9CDD" transform="skewY(-8)"/>'
            '<rect x="22" y="6" width="18" height="22" fill="#1C9CDD" transform="skewY(-8)"/>'
            '<circle cx="74" cy="15" r="11" fill="#111111"/>'
            '<path d="M65 8 L69 5 L74 11 L79 5 L83 8 L78 14 L83 20 L79 24 L74 17 L69 24 L65 20 L70 14 Z" fill="#FFFFFF"/>'
            "</g>"
        )
    return ""


def _render_microsoft_income_statement_svg(
    statement: dict[str, Any],
    business_groups: list[dict[str, Any]],
    money_symbol: str,
) -> str:
    width, height = 1180, 540
    revenue = float(statement["revenue_bn"])
    gross_profit = float(statement["gross_profit_bn"])
    cost_of_revenue = float(statement["cost_of_revenue_bn"])
    operating_profit = float(statement["operating_profit_bn"])
    operating_expenses = float(statement["operating_expenses_bn"])
    net_profit = float(statement["net_profit_bn"])
    bridge_delta = operating_profit - net_profit
    if abs(bridge_delta) <= max(0.25, revenue * 0.006):
        bridge_delta = 0.0
    positive_adjustments = []
    below_operating = []
    if bridge_delta < 0:
        positive_adjustments = _normalize_flow_items(
            list(statement.get("positive_adjustments") or []),
            abs(bridge_delta),
            fallback_name="Other income",
            fallback_color="#16A34A",
        )
    elif bridge_delta > 0:
        below_operating = _normalize_flow_items(
            list(statement.get("below_operating_items") or []),
            bridge_delta,
            fallback_name="Tax & other",
            fallback_color="#D92D20",
        )
    opex_items = _normalize_flow_items(
        _condense_opex_items(list(statement.get("opex_breakdown") or []), max_items=4, min_share_pct=4.0),
        operating_expenses,
        fallback_name="Other operating items",
        fallback_color="#F97316",
    )
    visual_net_profit = operating_profit if bridge_delta == 0 else net_profit
    serif = 'Georgia, "Times New Roman", serif'
    panel_bg = "#F8F7F4"
    title_blue = "#145B8E"
    text_soft = "#6B7280"
    neutral_bar = "#696A6E"
    profit_bar = "#2FAF3A"
    profit_fill = "#A9D59D"
    expense_bar = "#E10600"
    expense_fill = "#EA9A9F"
    source_palette = {
        "Productivity and Business Processes": ("#2796D4", "#7DBBE0"),
        "Intelligent Cloud": ("#FFBE0B", "#F8DD83"),
        "More Personal Computing": ("#7A7A7A", "#C8C8C8"),
    }
    display_lines_map = {
        "Productivity and Business Processes": ["Productivity &", "Business Processes"],
        "Intelligent Cloud": ["Intelligent Cloud"],
        "More Personal Computing": ["More Personal", "Computing"],
    }
    left_lockup_x = 48
    value_x = 304
    name_center_x = 170
    left_node_x = 302
    left_node_w = 12
    flow_start_x = left_node_x + left_node_w
    revenue_x, gross_x, op_x, right_x, opex_x = 356, 606, 814, 914, 978
    node_width = 22
    scale = 260 / max(revenue, 1)
    main_top = 136
    revenue_height = revenue * scale
    gross_height = gross_profit * scale
    cost_height = cost_of_revenue * scale
    op_profit_height = operating_profit * scale
    opex_height = operating_expenses * scale
    net_height = visual_net_profit * scale
    revenue_top = main_top
    revenue_bottom = revenue_top + revenue_height
    gross_top = main_top
    gross_bottom = gross_top + gross_height
    cost_top = gross_bottom
    cost_bottom = revenue_bottom
    op_top = gross_top
    op_bottom = op_top + op_profit_height
    opex_top = op_bottom
    opex_bottom = gross_bottom
    net_top = op_top
    net_bottom = net_top + net_height
    period_badge = _statement_period_badge(statement.get("period_end"))
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        f'<rect x="16" y="16" width="{width - 32}" height="{height - 32}" rx="30" fill="{panel_bg}" stroke="#E5E7EB"/>',
        _microsoft_corporate_logo(518, 34, 14),
        f'<text x="1126" y="48" text-anchor="end" font-size="17" font-weight="700" font-family="{serif}" fill="#5E6269">{_escape(_compact_fiscal_label(statement.get("fiscal_label")))}</text>',
        f'<text x="1126" y="67" text-anchor="end" font-size="11.5" fill="#6B7280">{_escape(period_badge)}</text>',
        '<text x="48" y="92" font-size="11" letter-spacing="0.14em" fill="#9AA1AB">BUSINESS MIX</text>',
        '<text x="1100" y="92" text-anchor="end" font-size="11" letter-spacing="0.14em" fill="#9AA1AB">EXPENSE LINES</text>',
    ]

    business_total = sum(float(item.get("value_bn") or 0.0) for item in business_groups) or revenue
    segment_cursor = revenue_top
    for item in business_groups:
        name = str(item.get("name") or "")
        value_bn = float(item.get("value_bn") or 0.0)
        share_of_revenue = value_bn / business_total if business_total else 0.0
        seg_height = revenue_height * share_of_revenue
        seg_top = segment_cursor
        seg_bottom = seg_top + seg_height
        node_color, fill_color = source_palette.get(name, ("#1D4ED8", "#93C5FD"))
        center_y = seg_top + seg_height / 2
        source_height = max(14.0, min(seg_height * 0.44, 30.0))
        source_top = center_y - source_height / 2
        source_bottom = center_y + source_height / 2
        body.append(f'<path d="{_flow_path(flow_start_x, source_top, source_bottom, revenue_x, seg_top, seg_bottom)}" fill="{fill_color}" opacity="0.94"/>')
        body.append(f'<rect x="{left_node_x}" y="{source_top:.1f}" width="{left_node_w}" height="{source_height:.1f}" rx="4" fill="{node_color}"/>')
        lockup_offset = 34 if name == "Productivity and Business Processes" else 30 if name == "Intelligent Cloud" else 26
        body.append(_microsoft_business_lockup(name, left_lockup_x, center_y - lockup_offset))
        body.append(
            f'<text x="{value_x:.1f}" y="{center_y - 4:.1f}" text-anchor="end" font-size="17" font-weight="600" fill="#666666">{format_money_bn(value_bn, money_symbol)}</text>'
        )
        if item.get("yoy_pct") is not None:
            body.append(
                f'<text x="{value_x:.1f}" y="{center_y + 13:.1f}" text-anchor="end" font-size="10.8" fill="#666666">{format_pct(item.get("yoy_pct"), signed=True)} Y/Y</text>'
            )
        name_lines = display_lines_map.get(name, _wrap_label(name, 18))
        body.append(
            _text_block(
                name_center_x,
                center_y + (25 if len(name_lines) == 1 else 20),
                name_lines,
                font_size=11.9,
                fill="#555555",
                weight=700,
                anchor="middle",
                line_height=13,
            ).replace('font-size="11.9"', f'font-size="11.9" font-family="{serif}"')
        )
        if item.get("margin_pct") is not None:
            margin_y = center_y + (44 if len(name_lines) == 1 else 49)
            body.append(
                f'<text x="{name_center_x:.1f}" y="{margin_y:.1f}" text-anchor="middle" font-size="10.5" fill="#666666">{format_pct(item.get("margin_pct"))} operating margin</text>'
            )
        segment_cursor = seg_bottom

    body.extend(
        [
            f'<rect x="{revenue_x}" y="{revenue_top:.1f}" width="{node_width}" height="{revenue_height:.1f}" rx="6" fill="{neutral_bar}"/>',
            f'<text x="{revenue_x + node_width / 2:.1f}" y="112" text-anchor="middle" font-size="17" font-weight="700" font-family="{serif}" fill="#555A63">Revenue</text>',
            f'<text x="{revenue_x + node_width / 2:.1f}" y="136" text-anchor="middle" font-size="18" font-weight="700" font-family="{serif}" fill="#555A63">{format_money_bn(revenue, money_symbol)}</text>',
        ]
    )
    if statement.get("revenue_yoy_pct") is not None:
        body.append(
            f'<text x="{revenue_x + node_width / 2:.1f}" y="156" text-anchor="middle" font-size="11.5" fill="#666666">{format_pct(statement.get("revenue_yoy_pct"), signed=True)} Y/Y</text>'
        )
    body.extend(
        [
            f'<path d="{_flow_path(revenue_x + node_width, revenue_top, gross_bottom, gross_x, gross_top, gross_bottom)}" fill="{profit_fill}" opacity="0.96"/>',
            f'<path d="{_flow_path(revenue_x + node_width, gross_bottom, revenue_bottom, gross_x, cost_top, cost_bottom)}" fill="{expense_fill}" opacity="0.96"/>',
            f'<rect x="{gross_x}" y="{gross_top:.1f}" width="{node_width}" height="{gross_height:.1f}" rx="6" fill="{profit_bar}"/>',
            f'<text x="{gross_x + node_width / 2:.1f}" y="112" text-anchor="middle" font-size="17" font-weight="700" font-family="{serif}" fill="#089256">Gross profit</text>',
            f'<text x="{gross_x + node_width / 2:.1f}" y="136" text-anchor="middle" font-size="18" font-weight="700" font-family="{serif}" fill="#089256">{format_money_bn(gross_profit, money_symbol)}</text>',
            f'<text x="{gross_x + node_width / 2:.1f}" y="156" text-anchor="middle" font-size="11.5" fill="#666666">{format_pct(statement.get("gross_margin_pct"))} margin</text>',
            f'<rect x="{gross_x}" y="{cost_top:.1f}" width="{node_width}" height="{cost_height:.1f}" rx="6" fill="{expense_bar}"/>',
            f'<text x="{gross_x + node_width / 2:.1f}" y="{cost_bottom - 6:.1f}" text-anchor="middle" font-size="15.5" font-weight="700" font-family="{serif}" fill="#8C1F0A">Cost of revenue</text>',
            f'<text x="{gross_x + node_width / 2:.1f}" y="{cost_bottom + 12:.1f}" text-anchor="middle" font-size="16.5" font-weight="700" font-family="{serif}" fill="#8C1F0A">({format_money_bn(cost_of_revenue, money_symbol)})</text>',
            f'<path d="{_flow_path(gross_x + node_width, gross_top, op_bottom, op_x, op_top, op_bottom)}" fill="{profit_fill}" opacity="0.96"/>',
            f'<path d="{_flow_path(gross_x + node_width, op_bottom, gross_bottom, op_x, opex_top, opex_bottom)}" fill="{expense_fill}" opacity="0.96"/>',
            f'<rect x="{op_x}" y="{op_top:.1f}" width="{node_width}" height="{op_profit_height:.1f}" rx="6" fill="{profit_bar}"/>',
            f'<text x="{op_x + node_width / 2:.1f}" y="112" text-anchor="middle" font-size="17" font-weight="700" font-family="{serif}" fill="#089256">Operating profit</text>',
            f'<text x="{op_x + node_width / 2:.1f}" y="136" text-anchor="middle" font-size="18" font-weight="700" font-family="{serif}" fill="#089256">{format_money_bn(operating_profit, money_symbol)}</text>',
            f'<text x="{op_x + node_width / 2:.1f}" y="156" text-anchor="middle" font-size="11.5" fill="#666666">{format_pct(statement.get("operating_margin_pct"))} margin</text>',
            f'<rect x="{op_x}" y="{opex_top:.1f}" width="{node_width}" height="{opex_height:.1f}" rx="6" fill="{expense_bar}"/>',
            f'<text x="{op_x + node_width / 2:.1f}" y="{opex_bottom + 18:.1f}" text-anchor="middle" font-size="15.5" font-weight="700" font-family="{serif}" fill="#8C1F0A">Operating expenses</text>',
            f'<text x="{op_x + node_width / 2:.1f}" y="{opex_bottom + 37:.1f}" text-anchor="middle" font-size="16.5" font-weight="700" font-family="{serif}" fill="#8C1F0A">({format_money_bn(operating_expenses, money_symbol)})</text>',
            f'<text x="{op_x + node_width / 2:.1f}" y="{opex_bottom + 55:.1f}" text-anchor="middle" font-size="11.5" fill="#666666">{format_pct(operating_expenses / revenue * 100 if revenue else None)} of revenue</text>',
            f'<path d="{_flow_path(op_x + node_width, op_top, op_bottom, right_x, net_top, net_top + op_profit_height)}" fill="{profit_fill}" opacity="0.96"/>',
            f'<rect x="{right_x}" y="{net_top:.1f}" width="{node_width}" height="{net_height:.1f}" rx="6" fill="{profit_bar}"/>',
            f'<text x="{right_x + 40:.1f}" y="{net_top + 11:.1f}" font-size="17" font-weight="700" font-family="{serif}" fill="#089256">Net profit</text>',
            f'<text x="{right_x + 40:.1f}" y="{net_top + 34:.1f}" font-size="18" font-weight="700" font-family="{serif}" fill="#089256">{format_money_bn(net_profit, money_symbol)}</text>',
            f'<text x="{right_x + 40:.1f}" y="{net_top + 58:.1f}" font-size="11.5" fill="#666666">{format_pct(statement.get("net_margin_pct"))} margin</text>',
        ]
    )

    net_label_block_bottom = net_top + 72

    if positive_adjustments:
        positive_cursor = net_top + op_profit_height
        source_cursor = op_bottom - sum(float(item.get("_flow_value_bn") or 0.0) for item in positive_adjustments) * scale
        for item in positive_adjustments:
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            item_height = max(flow_value * scale, 4.0)
            target_top = positive_cursor
            target_bottom = target_top + item_height
            source_top = source_cursor
            source_bottom = source_top + item_height
            node_color = item.get("color") or "#16A34A"
            body.append(
                f'<path d="{_flow_path(op_x + node_width, source_top, source_bottom, right_x, target_top, target_bottom)}" fill="{_mix_hex(node_color, "#FFFFFF", 0.60)}" opacity="0.96"/>'
            )
            body.append(f'<rect x="{right_x}" y="{target_top:.1f}" width="{node_width}" height="{item_height:.1f}" rx="6" fill="{node_color}"/>')
            body.append(_text_block(right_x + 40, target_top + 10, _wrap_label(str(item.get("name") or "Other income"), 16), font_size=11.0, fill="#089256", weight=700, line_height=11))
            body.append(f'<text x="{right_x + 40:.1f}" y="{target_top + 28:.1f}" font-size="10.0" fill="#089256">+{format_money_bn(float(item.get("value_bn") or flow_value), money_symbol)}</text>')
            positive_cursor = target_bottom + 5
            source_cursor = source_bottom

    if below_operating:
        deduction_cursor = max(net_top + net_height + 6, net_label_block_bottom + 6)
        source_cursor = op_bottom - sum(float(item.get("_flow_value_bn") or 0.0) for item in below_operating) * scale
        for item in below_operating:
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            item_height = max(flow_value * scale, 6.0)
            target_top = deduction_cursor
            target_bottom = target_top + item_height
            source_top = source_cursor
            source_bottom = source_top + item_height
            body.append(f'<path d="{_flow_path(op_x + node_width, source_top, source_bottom, right_x, target_top, target_bottom)}" fill="{expense_fill}" opacity="0.96"/>')
            body.append(f'<rect x="{right_x}" y="{target_top:.1f}" width="{node_width}" height="{item_height:.1f}" rx="6" fill="{item.get("color") or expense_bar}"/>')
            body.append(_text_block(right_x + 34, target_top + 7, _wrap_label(str(item.get("name") or "Tax & other"), 12), font_size=10.8, fill="#8C1F0A", weight=700, line_height=10))
            body.append(f'<text x="{right_x + 34:.1f}" y="{target_top + 25:.1f}" font-size="9.8" fill="#8C1F0A">({format_money_bn(float(item.get("value_bn") or flow_value), money_symbol)})</text>')
            deduction_cursor = target_bottom + 6
            source_cursor = source_bottom

    if opex_items:
        flow_total = sum(float(item.get("_flow_value_bn") or 0.0) for item in opex_items) or 1.0
        source_cursor = opex_top
        target_cursor = 296
        gap = 10
        for item in opex_items:
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            actual_value = float(item.get("value_bn") or 0.0)
            item_height = max(flow_value * scale, 6.0)
            item_top = target_cursor
            item_bottom = item_top + item_height
            source_top = source_cursor
            source_bottom = source_top + flow_value / flow_total * opex_height
            label_lines = _wrap_label(str(item.get("name") or "Expense"), 14)
            detail = f"({format_money_bn(actual_value, money_symbol)})"
            pct_of_revenue = item.get("pct_of_revenue")
            if pct_of_revenue is not None:
                detail += f" | {format_pct(pct_of_revenue)} rev"
            elif revenue:
                detail += f" | {format_pct(actual_value / revenue * 100)} rev"
            body.append(f'<path d="{_flow_path(op_x + node_width, source_top, source_bottom, opex_x, item_top, item_bottom)}" fill="{expense_fill}" opacity="0.96"/>')
            body.append(f'<rect x="{opex_x}" y="{item_top:.1f}" width="{node_width}" height="{item_height:.1f}" rx="6" fill="{item.get("color") or expense_bar}"/>')
            body.append(_text_block(opex_x + 28, item_top + 8, label_lines[:2], font_size=10.7, fill="#C12A15", weight=700, line_height=10))
            detail_y = item_top + 8 + len(label_lines[:2]) * 10 + 5
            body.append(f'<text x="{opex_x + 28:.1f}" y="{detail_y:.1f}" font-size="9.2" fill="#666666">{_escape(_trim_note(detail, 26))}</text>')
            target_cursor = item_bottom + gap
            source_cursor = source_bottom

    annotations = _statement_annotation_items(statement, money_symbol)
    if annotations:
        card_y = 458
        card_h = 58
        gap = 14
        card_w = (width - 88 - gap * 2) / 3
        for index, item in enumerate(annotations[:3]):
            card_x = 30 + index * (card_w + gap)
            accent = item.get("color") or "#2563EB"
            note_lines = _wrap_label(_trim_note(str(item.get("note") or ""), 40), 18)[:2]
            body.append(f'<rect x="{card_x:.1f}" y="{card_y:.1f}" width="{card_w:.1f}" height="{card_h}" rx="18" fill="#FFFFFF" stroke="#D9E0E7"/>')
            body.append(f'<rect x="{card_x + 18:.1f}" y="{card_y + 14:.1f}" width="28" height="4" rx="2" fill="{accent}"/>')
            body.append(f'<text x="{card_x + 18:.1f}" y="{card_y + 26:.1f}" font-size="9.4" font-weight="700" fill="{text_soft}">{_escape(_trim_note(str(item.get("title") or ""), 20))}</text>')
            body.append(f'<text x="{card_x + 18:.1f}" y="{card_y + 47:.1f}" font-size="15.0" font-weight="800" fill="#111827">{_escape(str(item.get("value") or ""))}</text>')
            if note_lines:
                body.append(_text_block(card_x + 172, card_y + 24, note_lines, font_size=9.1, fill="#4B5563", weight=600, line_height=10))

    return _svg(width, height, "".join(body))


def render_income_statement_svg(
    statement: dict[str, Any],
    colors: dict[str, str],
    primary: str,
    money_symbol: str = "$",
) -> str:
    width, height = 1180, 540
    business_groups = _condense_statement_sources(
        list(statement.get("business_groups") or statement.get("sources") or []),
        primary,
        max_items=6,
        min_share_pct=6.0,
    )
    if not statement or not business_groups:
        body = [
            f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
            '<text x="32" y="38" font-size="18" font-weight="700" fill="#0F172A">营收与开支可视化图</text>',
            '<rect x="40" y="82" width="1100" height="390" rx="28" fill="#F8FAFC" stroke="#E2E8F0"/>',
            f'<text x="74" y="156" font-size="22" font-weight="700" fill="{primary}">当前缺少可直连的当季利润表拆分</text>',
            '<text x="74" y="196" font-size="14" fill="#334155">该页需要官方财报里的收入、成本、经营费用与税项拆分。当前公司先保留趋势页与结构页，不伪造细项。</text>',
        ]
        return _svg(width, height, "".join(body))
    company_id = str(statement.get("company_id") or "")
    if company_id == "microsoft":
        return _render_microsoft_income_statement_svg(statement, business_groups, money_symbol)

    revenue = float(statement["revenue_bn"])
    gross_profit = float(statement["gross_profit_bn"])
    cost_of_revenue = float(statement["cost_of_revenue_bn"])
    operating_profit = float(statement["operating_profit_bn"])
    operating_expenses = float(statement["operating_expenses_bn"])
    net_profit = float(statement["net_profit_bn"])
    bridge_delta = operating_profit - net_profit
    if abs(bridge_delta) <= max(0.25, revenue * 0.006):
        bridge_delta = 0.0
    positive_adjustments: list[dict[str, Any]] = []
    below_operating: list[dict[str, Any]] = []
    if bridge_delta < 0:
        positive_adjustments = _normalize_flow_items(
            list(statement.get("positive_adjustments") or []),
            abs(bridge_delta),
            fallback_name="Other income",
            fallback_color="#16A34A",
        )
    elif bridge_delta > 0:
        below_operating = _normalize_flow_items(
            list(statement.get("below_operating_items") or []),
            bridge_delta,
            fallback_name="Tax & other",
            fallback_color="#D94B58",
        )
    opex_items = _normalize_flow_items(
        _condense_opex_items(list(statement.get("opex_breakdown") or []), max_items=3, min_share_pct=4.0),
        operating_expenses,
        fallback_name="Other operating items",
        fallback_color="#F97316",
    )

    visual_net_profit = operating_profit if bridge_delta == 0 else net_profit
    revenue_x, gross_x, op_x, net_x, opex_node_x = 388, 634, 846, 948, 996
    node_width = 22
    scale = 226 / max(revenue, 1)
    main_top = 150
    revenue_height = revenue * scale
    gross_height = gross_profit * scale
    cost_height = cost_of_revenue * scale
    op_profit_height = operating_profit * scale
    opex_height = operating_expenses * scale
    net_height = visual_net_profit * scale

    revenue_top = main_top
    revenue_bottom = revenue_top + revenue_height
    gross_top = main_top
    gross_bottom = gross_top + gross_height
    cost_top = gross_bottom
    cost_bottom = revenue_bottom
    op_top = gross_top
    op_bottom = op_top + op_profit_height
    opex_top = op_bottom
    opex_bottom = gross_bottom
    net_top = op_top
    net_bottom = net_top + net_height
    panel_bg = "#F3F5F8"
    text_muted = "#667085"
    revenue_bar = "#6B7280"
    profit_bar = "#2CA74C"
    profit_fill = "#A9D5A9"
    cost_bar = "#E05361"
    cost_fill = "#F4B3BB"
    left_card_x = 46
    left_card_w = 250
    business_top = 154
    business_bottom = 430
    dense_business = len(business_groups) >= 6
    business_palette = _segment_palette([str(item.get("name") or "Business") for item in business_groups], colors, primary)
    business_gap = 6 if dense_business else 8 if len(business_groups) >= 5 else 10 if len(business_groups) >= 4 else 12
    business_card_h = min(
        68.0,
        max(40.0 if dense_business else 44.0 if len(business_groups) >= 5 else 46.0, (business_bottom - business_top - business_gap * max(len(business_groups) - 1, 0)) / max(len(business_groups), 1)),
    )
    business_total = sum(float(item.get("value_bn") or 0.0) for item in business_groups) or revenue
    segment_cursor = revenue_top
    segment_bounds: list[tuple[float, float]] = []
    segment_centers: list[float] = []
    for item in business_groups:
        share_of_revenue = float(item.get("value_bn") or 0.0) / business_total if business_total else 0.0
        seg_height = revenue_height * share_of_revenue
        seg_top = segment_cursor
        seg_bottom = seg_top + seg_height
        segment_bounds.append((seg_top, seg_bottom))
        segment_centers.append((seg_top + seg_bottom) / 2)
        segment_cursor = seg_bottom
    business_positions = _resolve_vertical_positions(segment_centers, business_card_h, business_top, business_bottom, business_gap)
    annotations = _statement_annotation_items(statement, money_symbol)
    company_name = str(statement.get("company_name") or company_id or "Company")
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        f'<rect x="16" y="16" width="{width - 32}" height="{height - 32}" rx="30" fill="{panel_bg}" stroke="#E2E8F0"/>',
        '<text x="44" y="44" font-size="12" letter-spacing="0.18em" fill="#8A94A6">OFFICIAL EARNINGS BRIDGE</text>',
        _company_wordmark_svg(company_id, company_name, 560, 28, primary),
        f'<rect x="960" y="26" width="176" height="64" rx="22" fill="#FFFFFF" stroke="#D8E1EB"/>',
        f'<text x="982" y="56" font-size="22" font-weight="800" fill="{_mix_hex(primary, "#1D4ED8", 0.22)}">{_escape(_compact_fiscal_label(statement.get("fiscal_label")))}</text>',
        f'<text x="982" y="75" font-size="11.5" fill="{text_muted}">{_escape(_statement_period_badge(statement.get("period_end")))}</text>',
        '<text x="44" y="96" font-size="11" letter-spacing="0.12em" fill="#98A2B3">BUSINESS MIX</text>',
        '<text x="1108" y="96" text-anchor="end" font-size="11" letter-spacing="0.12em" fill="#98A2B3">EXPENSE LINES</text>',
    ]

    for index, item in enumerate(business_groups):
        presented = _business_group_presentation(company_id, item)
        value_bn = float(item.get("value_bn") or 0.0)
        seg_top, seg_bottom = segment_bounds[index]
        seg_height = max(seg_bottom - seg_top, 1.0)
        card_y = business_positions[index] if index < len(business_positions) else business_top + index * (business_card_h + business_gap)
        card_center = card_y + business_card_h / 2
        source_x = left_card_x + left_card_w + 10
        source_height = _statement_source_lane_height(business_card_h, dense_business)
        source_top = card_center - source_height / 2
        source_bottom = card_center + source_height / 2
        node_color = _statement_source_color(str(item.get("name") or ""), business_palette, primary)
        card_fill = _mix_hex(node_color, "#FFFFFF", 0.90)
        flow_fill = _mix_hex(node_color, "#FFFFFF", 0.62)
        yoy_pct = item.get("yoy_pct")
        icon_key = str(presented["chips"][0].get("key") or "") if presented.get("chips") else ""
        label_text = _compact_category_label(str(presented.get("display_name") or item.get("name") or "Business"))
        label_lines = _wrap_visual_text_lines(_trim_note(label_text, 24 if dense_business else 28), 10.6 if dense_business else 11.0 if len(business_groups) >= 5 else 12.4, 2)
        label_x = left_card_x + 48
        body.append(f'<path d="{_flow_path(source_x, source_top, source_bottom, revenue_x, seg_top, seg_bottom)}" fill="{flow_fill}" opacity="0.94"/>')
        body.append(
            f'<rect x="{left_card_x:.1f}" y="{card_y:.1f}" width="{left_card_w:.1f}" height="{business_card_h:.1f}" rx="22" fill="{card_fill}" stroke="{_mix_hex(node_color, "#CBD5E1", 0.50)}"/>'
        )
        body.append(f'<rect x="{left_card_x + 12:.1f}" y="{card_y + 10:.1f}" width="4" height="{max(business_card_h - 20, 14):.1f}" rx="2" fill="{node_color}"/>')
        body.append(f'<rect x="{left_card_x + left_card_w - 8:.1f}" y="{card_center - 13:.1f}" width="6" height="26" rx="3" fill="{node_color}"/>')
        if icon_key:
            icon_cy = card_y + max(15.5, min(17.0, business_card_h * 0.34))
            body.append(
                f'<circle cx="{left_card_x + 28:.1f}" cy="{icon_cy:.1f}" r="11" fill="#FFFFFF" stroke="{_mix_hex(node_color, "#CBD5E1", 0.55)}"/>'
            )
            body.append(_logo_icon(left_card_x + 21, icon_cy - 7, 14, icon_key))
        body.append(
            _text_block(
                label_x,
                card_y + (16 if len(label_lines) == 1 else 12.5),
                label_lines,
                font_size=10.2 if dense_business else 10.7 if len(business_groups) >= 5 else 11.2,
                fill="#0F172A",
                weight=700,
                line_height=10.0 if dense_business else 10.4,
            )
        )
        body.append(
            f'<text x="{label_x + 2:.1f}" y="{card_y + business_card_h - 9.8:.1f}" font-size="{14.0 if dense_business else 14.7 if len(business_groups) >= 5 else 15.8}" font-weight="800" fill="#0F172A">{format_money_bn(value_bn, money_symbol)}</text>'
        )
        detail_text = _statement_detail_text(value_bn, revenue, yoy_pct)
        body.append(
            f'<text x="{left_card_x + left_card_w - 18:.1f}" y="{card_y + business_card_h - 9.8:.1f}" text-anchor="end" font-size="{8.4 if dense_business else 9.1}" font-weight="600" fill="{text_muted}">{_escape(_trim_note(detail_text, 24))}</text>'
        )

    body.extend(
        [
            f'<rect x="{revenue_x}" y="{revenue_top:.1f}" width="{node_width}" height="{revenue_height:.1f}" rx="10" fill="{revenue_bar}"/>',
            f'<path d="{_flow_path(revenue_x + node_width, revenue_top, gross_bottom, gross_x, gross_top, gross_bottom)}" fill="{profit_fill}" opacity="0.96"/>',
            f'<path d="{_flow_path(revenue_x + node_width, gross_bottom, revenue_bottom, gross_x, cost_top, cost_bottom)}" fill="{cost_fill}" opacity="0.96"/>',
            f'<rect x="{gross_x}" y="{gross_top:.1f}" width="{node_width}" height="{gross_height:.1f}" rx="10" fill="{profit_bar}"/>',
            f'<rect x="{gross_x}" y="{cost_top:.1f}" width="{node_width}" height="{cost_height:.1f}" rx="10" fill="{cost_bar}"/>',
            f'<path d="{_flow_path(gross_x + node_width, gross_top, op_bottom, op_x, op_top, op_bottom)}" fill="{profit_fill}" opacity="0.96"/>',
            f'<path d="{_flow_path(gross_x + node_width, op_bottom, gross_bottom, op_x, opex_top, opex_bottom)}" fill="{cost_fill}" opacity="0.96"/>',
            f'<rect x="{op_x}" y="{op_top:.1f}" width="{node_width}" height="{op_profit_height:.1f}" rx="10" fill="{profit_bar}"/>',
            f'<rect x="{op_x}" y="{opex_top:.1f}" width="{node_width}" height="{opex_height:.1f}" rx="10" fill="{cost_bar}"/>',
            f'<path d="{_flow_path(op_x + node_width, op_top, op_bottom, net_x, net_top, net_top + op_profit_height)}" fill="{profit_fill}" opacity="0.96"/>',
            f'<rect x="{net_x}" y="{net_top:.1f}" width="{node_width}" height="{net_height:.1f}" rx="10" fill="{profit_bar}"/>',
        ]
    )

    body.append(f'<text x="{revenue_x + node_width / 2:.1f}" y="104" text-anchor="middle" font-size="21" font-weight="800" fill="#556070">Revenue</text>')
    body.append(f'<text x="{revenue_x + node_width / 2:.1f}" y="128" text-anchor="middle" font-size="19" font-weight="800" fill="#556070">{format_money_bn(revenue, money_symbol)}</text>')
    if statement.get("revenue_yoy_pct") is not None:
        body.append(f'<text x="{revenue_x + node_width / 2:.1f}" y="146" text-anchor="middle" font-size="11.2" fill="{text_muted}">{format_pct(statement.get("revenue_yoy_pct"), signed=True)} YoY</text>')

    body.append(f'<text x="{gross_x + node_width / 2:.1f}" y="104" text-anchor="middle" font-size="20.5" font-weight="800" fill="#169248">Gross profit</text>')
    body.append(f'<text x="{gross_x + node_width / 2:.1f}" y="128" text-anchor="middle" font-size="19" font-weight="800" fill="#169248">{format_money_bn(gross_profit, money_symbol)}</text>')
    body.append(f'<text x="{gross_x + node_width / 2:.1f}" y="146" text-anchor="middle" font-size="11.2" fill="{text_muted}">{format_pct(statement.get("gross_margin_pct"))} margin</text>')

    cost_label_y = cost_top + max(34.0, min(cost_height * 0.60, cost_height - 16))
    body.append(f'<text x="{gross_x + node_width / 2:.1f}" y="{cost_label_y:.1f}" text-anchor="middle" font-size="15.8" font-weight="800" fill="#B42318">Cost of revenue</text>')
    body.append(f'<text x="{gross_x + node_width / 2:.1f}" y="{cost_label_y + 20:.1f}" text-anchor="middle" font-size="16.8" font-weight="800" fill="#B42318">({format_money_bn(cost_of_revenue, money_symbol)})</text>')

    body.append(f'<text x="{op_x + node_width / 2:.1f}" y="104" text-anchor="middle" font-size="20.5" font-weight="800" fill="#169248">Operating profit</text>')
    body.append(f'<text x="{op_x + node_width / 2:.1f}" y="128" text-anchor="middle" font-size="19" font-weight="800" fill="#169248">{format_money_bn(operating_profit, money_symbol)}</text>')
    body.append(f'<text x="{op_x + node_width / 2:.1f}" y="146" text-anchor="middle" font-size="11.2" fill="{text_muted}">{format_pct(statement.get("operating_margin_pct"))} margin</text>')

    body.append(f'<text x="{op_x - 20:.1f}" y="{opex_bottom + 28:.1f}" text-anchor="end" font-size="15.2" font-weight="800" fill="#B42318">OpEx ({format_money_bn(operating_expenses, money_symbol)})</text>')
    body.append(f'<text x="{op_x - 20:.1f}" y="{opex_bottom + 47:.1f}" text-anchor="end" font-size="11.0" fill="{text_muted}">{format_pct(operating_expenses / revenue * 100 if revenue else None)} of rev</text>')

    body.append(f'<text x="{net_x + 42:.1f}" y="{net_top + 16:.1f}" font-size="21" font-weight="800" fill="#169248">Net profit</text>')
    body.append(f'<text x="{net_x + 42:.1f}" y="{net_top + 42:.1f}" font-size="20" font-weight="800" fill="#169248">{format_money_bn(net_profit, money_symbol)}</text>')
    body.append(f'<text x="{net_x + 42:.1f}" y="{net_top + 64:.1f}" font-size="11.8" fill="{text_muted}">{format_pct(statement.get("net_margin_pct"))} margin</text>')

    net_label_block_bottom = net_top + 72

    if positive_adjustments:
        positive_total = sum(float(item.get("_flow_value_bn") or 0.0) for item in positive_adjustments)
        positive_cursor = net_top + op_profit_height
        source_cursor = op_bottom - positive_total * scale
        for item in positive_adjustments:
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            item_height = max(flow_value * scale, 10.0)
            target_top = positive_cursor
            target_bottom = target_top + item_height
            source_top = source_cursor
            source_bottom = source_top + item_height
            node_color = item.get("color") or "#16A34A"
            label = _statement_bridge_display_name(item.get("name"))
            body.append(f'<path d="{_flow_path(op_x + node_width, source_top, source_bottom, net_x, target_top, target_bottom)}" fill="{_mix_hex(node_color, "#FFFFFF", 0.64)}" opacity="0.96"/>')
            body.append(f'<rect x="{net_x}" y="{target_top:.1f}" width="{node_width}" height="{item_height:.1f}" rx="10" fill="{node_color}"/>')
            body.append(f'<text x="{net_x + 34:.1f}" y="{target_top + 14:.1f}" font-size="10.2" font-weight="700" fill="#169248">{_escape(_trim_note(label, 20))}</text>')
            body.append(f'<text x="{net_x + 34:.1f}" y="{target_top + 28:.1f}" font-size="9.6" fill="#169248">+{format_money_bn(float(item.get("value_bn") or flow_value), money_symbol)}</text>')
            positive_cursor = target_bottom + 2
            source_cursor = source_bottom

    if below_operating:
        deduction_cursor = max(net_top + net_height + 6, net_label_block_bottom + 6)
        source_cursor = op_bottom - sum(float(item.get("_flow_value_bn") or 0.0) for item in below_operating) * scale
        for item in below_operating:
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            item_height = max(flow_value * scale, 10.0)
            target_top = deduction_cursor
            target_bottom = target_top + item_height
            source_top = source_cursor
            source_bottom = source_top + item_height
            node_color = item.get("color") or cost_bar
            label = _statement_bridge_display_name(item.get("name"))
            body.append(f'<path d="{_flow_path(op_x + node_width, source_top, source_bottom, net_x, target_top, target_bottom)}" fill="{cost_fill}" opacity="0.96"/>')
            body.append(f'<rect x="{net_x}" y="{target_top:.1f}" width="{node_width}" height="{item_height:.1f}" rx="10" fill="{node_color}"/>')
            body.append(f'<text x="{net_x + 34:.1f}" y="{target_top + 14:.1f}" font-size="10.2" font-weight="700" fill="#B42318">{_escape(_trim_note(label, 20))}</text>')
            body.append(f'<text x="{net_x + 34:.1f}" y="{target_top + 28:.1f}" font-size="9.6" fill="#B42318">({format_money_bn(float(item.get("value_bn") or flow_value), money_symbol)})</text>')
            deduction_cursor = target_bottom + 2
            source_cursor = source_bottom

    if opex_items:
        flow_total = sum(float(item.get("_flow_value_bn") or 0.0) for item in opex_items) or 1.0
        source_cursor = opex_top
        expense_sections: list[tuple[float, float]] = []
        for item in opex_items:
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            source_height = max(flow_value / flow_total * opex_height, 4.0)
            source_top = source_cursor
            source_bottom = source_top + source_height
            expense_sections.append((source_top, source_bottom))
            source_cursor = source_bottom
        expense_gap = 10
        expense_top = 248
        expense_bottom = 412
        expense_card_h = min(
            60.0,
            max(44.0, (expense_bottom - expense_top - expense_gap * max(len(opex_items) - 1, 0)) / max(len(opex_items), 1)),
        )
        expense_positions = _resolve_vertical_positions(
            [(section_top + section_bottom) / 2 for section_top, section_bottom in expense_sections],
            expense_card_h,
            expense_top,
            expense_bottom,
            expense_gap,
        )
        expense_card_x = 1012
        expense_card_w = 142
        for index, item in enumerate(opex_items):
            section_top, section_bottom = expense_sections[index]
            card_y = expense_positions[index] if index < len(expense_positions) else expense_top + index * (expense_card_h + expense_gap)
            card_center = card_y + expense_card_h / 2
            flow_value = float(item.get("_flow_value_bn") or 0.0)
            actual_value = float(item.get("value_bn") or 0.0)
            target_height = max(12.0, min(expense_card_h - 18, max(flow_value * scale * 0.84, 12.0)))
            target_top = card_center - target_height / 2
            target_bottom = target_top + target_height
            node_color = item.get("color") or "#F97316"
            label = _statement_bridge_display_name(item.get("name"))
            label_lines = _wrap_label(_trim_note(label, 20), 13)[:2]
            pct_of_revenue = item.get("pct_of_revenue")
            detail = format_pct(pct_of_revenue, signed=False) + " rev" if pct_of_revenue is not None else format_pct(actual_value / revenue * 100 if revenue else None) + " rev"
            body.append(f'<path d="{_flow_path(op_x + node_width, section_top, section_bottom, opex_node_x, target_top, target_bottom)}" fill="{_mix_hex(node_color, "#FFFFFF", 0.68)}" opacity="0.96"/>')
            body.append(f'<rect x="{opex_node_x}" y="{target_top:.1f}" width="12" height="{target_height:.1f}" rx="6" fill="{node_color}"/>')
            body.append(
                f'<rect x="{expense_card_x:.1f}" y="{card_y:.1f}" width="{expense_card_w:.1f}" height="{expense_card_h:.1f}" rx="20" fill="{_mix_hex(node_color, "#FFFFFF", 0.91)}" stroke="{_mix_hex(node_color, "#CBD5E1", 0.48)}"/>'
            )
            body.append(_text_block(expense_card_x + 14, card_y + (16 if len(label_lines) == 1 else 13), label_lines, font_size=10.2, fill="#991B1B", weight=700, line_height=10.2))
            value_y = card_y + (30 if len(label_lines) == 1 else 37)
            body.append(f'<text x="{expense_card_x + expense_card_w - 14:.1f}" y="{value_y:.1f}" text-anchor="end" font-size="14.2" font-weight="800" fill="#991B1B">{format_money_bn(actual_value, money_symbol)}</text>')
            body.append(f'<text x="{expense_card_x + expense_card_w - 14:.1f}" y="{card_y + expense_card_h - 12:.1f}" text-anchor="end" font-size="9.0" font-weight="600" fill="{text_muted}">{_escape(detail)}</text>')

    if annotations:
        card_y = 454
        card_h = 58
        gap = 14
        card_w = (width - 88 - gap * 2) / 3
        for index, item in enumerate(annotations[:3]):
            card_x = 30 + index * (card_w + gap)
            accent = item.get("color") or primary
            note_lines = _wrap_label(_trim_note(str(item.get("note") or ""), 40), 18)[:2]
            body.append(f'<rect x="{card_x:.1f}" y="{card_y:.1f}" width="{card_w:.1f}" height="{card_h}" rx="18" fill="#FFFFFF" stroke="#D9E0E7"/>')
            body.append(f'<rect x="{card_x + 18:.1f}" y="{card_y + 14:.1f}" width="28" height="4" rx="2" fill="{accent}"/>')
            body.append(f'<text x="{card_x + 18:.1f}" y="{card_y + 26:.1f}" font-size="9.4" font-weight="700" fill="{text_muted}">{_escape(_trim_note(str(item.get("title") or ""), 20))}</text>')
            body.append(f'<text x="{card_x + 18:.1f}" y="{card_y + 47:.1f}" font-size="15.0" font-weight="800" fill="#111827">{_escape(str(item.get("value") or ""))}</text>')
            if note_lines:
                body.append(_text_block(card_x + 170, card_y + 24, note_lines, font_size=9.1, fill="#4B5563", weight=600, line_height=10))

    return _svg(width, height, "".join(body))


def render_statement_translation_svg(
    statement: dict[str, Any],
    primary: str,
    accent: str,
    money_symbol: str = "$",
) -> str:
    width, height = 1180, 468
    if not statement or statement.get("revenue_bn") in (None, 0):
        body = [
            f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
            f'<rect x="20" y="20" width="{width - 40}" height="{height - 40}" rx="28" fill="#F8FAFC" stroke="#E2E8F0"/>',
            '<text x="46" y="58" font-size="12" letter-spacing="0.16em" fill="#94A3B8">BILINGUAL STATEMENT MAP</text>',
            '<text x="46" y="98" font-size="28" font-weight="800" fill="#0F172A">财报科目中文直译</text>',
            f'<text x="46" y="136" font-size="14" fill="{_mix_hex(primary, "#0F172A", 0.35)}">当前季度缺少稳定利润表拆分，系统不会伪造收入、费用与利润科目。</text>',
            '<text x="46" y="164" font-size="13" fill="#475569">当官方新闻稿 / 10-Q / 10-K / shareholder deck 接入后，这一页会自动补齐中文直译与原文对照。</text>',
        ]
        return _svg(width, height, "".join(body))

    company_id = str(statement.get("company_id") or "")
    revenue_bn = float(statement.get("revenue_bn") or 0.0)
    gross_profit_bn = statement.get("gross_profit_bn")
    operating_profit_bn = statement.get("operating_profit_bn")
    net_profit_bn = statement.get("net_profit_bn")
    business_groups = _condense_statement_sources(
        list(statement.get("business_groups") or statement.get("sources") or []),
        primary,
        max_items=6,
        min_share_pct=5.0,
    )
    revenue_rows = [
        {
            **item,
            "meta": _statement_meta_text(item, revenue_bn),
        }
        for item in business_groups
        if float(item.get("value_bn") or 0.0) > 0.03
    ]
    if not revenue_rows:
        revenue_rows = [{"name": "Revenue", "value_bn": revenue_bn, "meta": ""}]

    opex_rows = _condense_opex_items(list(statement.get("opex_breakdown") or []), max_items=3, min_share_pct=4.0)
    expense_rows: list[dict[str, Any]] = []
    cost_of_revenue_bn = float(statement.get("cost_of_revenue_bn") or 0.0)
    if cost_of_revenue_bn > 0.03:
        expense_rows.append(
            {
                "name": "Cost of revenue",
                "value_bn": cost_of_revenue_bn,
                "pct_of_revenue": cost_of_revenue_bn / revenue_bn * 100 if revenue_bn else None,
                "color": "#DC2626",
            }
        )
    if opex_rows:
        expense_rows.extend(opex_rows)
    elif float(statement.get("operating_expenses_bn") or 0.0) > 0.03:
        expense_rows.append(
            {
                "name": "Operating expenses",
                "value_bn": float(statement.get("operating_expenses_bn") or 0.0),
                "pct_of_revenue": float(statement.get("operating_expenses_bn") or 0.0) / revenue_bn * 100 if revenue_bn else None,
                "color": "#F97316",
            }
        )
    bridge_item = _aggregate_statement_bridge(
        list(statement.get("below_operating_items") or []),
        fallback_name="Tax & other",
        color="#D92D20",
    )
    positive_item = _aggregate_statement_bridge(
        list(statement.get("positive_adjustments") or []),
        fallback_name="Other income",
        color="#16A34A",
    )
    if bridge_item is not None:
        expense_rows.append(bridge_item)
    elif positive_item is not None:
        expense_rows.append(positive_item)
    expense_rows = [
        {
            **item,
            "meta": _statement_meta_text(item, revenue_bn),
        }
        for item in expense_rows[:4]
        if float(item.get("value_bn") or 0.0) > 0.03
    ]

    title_fill = _mix_hex(primary, "#0F172A", 0.18)
    soft_fill = _mix_hex(primary, "#FFFFFF", 0.93)
    panel_fills = [
        _mix_hex(primary, "#FFFFFF", 0.93),
        _mix_hex("#F97316", "#FFFFFF", 0.94),
        _mix_hex(accent, "#FFFFFF", 0.92),
    ]
    panel_xs = [30, 412, 794]
    panel_width = 356
    panel_y = 74
    panel_h = 340
    row_left = 18
    row_width = panel_width - 36
    period_badge = _statement_period_badge(statement.get("period_end"))

    metrics = [
        {
            "label_cn": "收入",
            "label_en": "Revenue",
            "value": format_money_bn(revenue_bn, money_symbol),
            "note": f"收入同比 {format_pct(statement.get('revenue_yoy_pct'), signed=True)}",
            "accent": primary,
        },
        {
            "label_cn": "毛利润",
            "label_en": "Gross profit",
            "value": format_money_bn(gross_profit_bn, money_symbol),
            "note": f"毛利率 {format_pct(statement.get('gross_margin_pct'))}",
            "accent": _mix_hex(primary, "#16A34A", 0.55),
        },
        {
            "label_cn": "经营利润",
            "label_en": "Operating profit",
            "value": format_money_bn(operating_profit_bn, money_symbol),
            "note": f"经营利润率 {format_pct(statement.get('operating_margin_pct'))}",
            "accent": _mix_hex(accent, "#0F766E", 0.3),
        },
        {
            "label_cn": "净利润",
            "label_en": "Net profit",
            "value": format_money_bn(net_profit_bn, money_symbol),
            "note": f"净利率 {format_pct(statement.get('net_margin_pct'))}",
            "accent": _mix_hex(primary, "#111827", 0.5),
        },
    ]

    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        f'<rect x="16" y="16" width="{width - 32}" height="{height - 32}" rx="30" fill="{soft_fill}" stroke="#E2E8F0"/>',
        '<text x="34" y="42" font-size="11" letter-spacing="0.16em" fill="#94A3B8">BILINGUAL STATEMENT MAP</text>',
        f'<text x="34" y="61" font-size="11.4" fill="{title_fill}">中文为官方口径直译，英文原词保留以便快速回看原文。</text>',
        f'<rect x="924" y="22" width="224" height="46" rx="20" fill="#FFFFFF" stroke="{_mix_hex(primary, "#CBD5E1", 0.55)}"/>',
        f'<text x="944" y="43" font-size="18" font-weight="800" fill="{primary}">{_escape(_compact_fiscal_label(statement.get("fiscal_label")))}</text>',
        f'<text x="944" y="58" font-size="10.2" fill="#64748B">{_escape(period_badge)}</text>',
    ]

    headers = [
        ("业务收入科目", "官方收入结构的中文直译"),
        ("成本与费用科目", "费用项按官方科目顺序压缩展示"),
        ("利润与兑现科目", "总量与利润率一页并读"),
    ]
    for index, panel_x in enumerate(panel_xs):
        body.extend(
            [
                f'<rect x="{panel_x:.1f}" y="{panel_y:.1f}" width="{panel_width}" height="{panel_h}" rx="28" fill="{panel_fills[index]}" stroke="#DCE4EE"/>',
                f'<text x="{panel_x + 18:.1f}" y="{panel_y + 24:.1f}" font-size="11" letter-spacing="0.14em" fill="#94A3B8">OFFICIAL TERMS</text>',
                f'<text x="{panel_x + 18:.1f}" y="{panel_y + 45:.1f}" font-size="16.5" font-weight="700" fill="#0F172A">{_escape(headers[index][0])}</text>',
                f'<text x="{panel_x + 18:.1f}" y="{panel_y + 61:.1f}" font-size="10.4" fill="#64748B">{_escape(_trim_note(headers[index][1], 22))}</text>',
            ]
        )

    if revenue_rows:
        revenue_gap = 6 if len(revenue_rows) >= 6 else 8
        revenue_top = panel_y + 74
        revenue_area = panel_h - 98
        revenue_height = min(58.0, (revenue_area - revenue_gap * max(len(revenue_rows) - 1, 0)) / max(len(revenue_rows), 1))
        revenue_y = revenue_top
        for item in revenue_rows:
            presented = _business_group_presentation(company_id, item)
            icon_key = None
            if presented.get("chips"):
                icon_key = str(presented["chips"][0].get("key") or "")
            if not icon_key:
                icon_key = _keyword_logo_key(str(item.get("name") or "Business"))
            node_color = str(item.get("color") or primary)
            body.append(
                _statement_row_svg(
                    panel_xs[0] + row_left,
                    revenue_y,
                    row_width,
                    revenue_height,
                    label_cn=_translate_statement_label(item.get("name"), company_id),
                    label_en=str(item.get("name") or "-"),
                    value_text=format_money_bn(item.get("value_bn"), money_symbol),
                    meta_text=str(item.get("meta") or ""),
                    accent=node_color,
                    fill=_mix_hex(node_color, "#FFFFFF", 0.9),
                    icon_key=icon_key,
                )
            )
            revenue_y += revenue_height + revenue_gap

    if expense_rows:
        expense_gap = 8
        expense_top = panel_y + 74
        expense_area = panel_h - 98
        expense_height = min(56.0, (expense_area - expense_gap * max(len(expense_rows) - 1, 0)) / max(len(expense_rows), 1))
        expense_y = expense_top
        for item in expense_rows:
            node_color = str(item.get("color") or "#F97316")
            body.append(
                _statement_row_svg(
                    panel_xs[1] + row_left,
                    expense_y,
                    row_width,
                    expense_height,
                    label_cn=_translate_statement_label(item.get("name"), company_id),
                    label_en=str(item.get("name") or "-"),
                    value_text=format_money_bn(item.get("value_bn"), money_symbol),
                    meta_text=str(item.get("meta") or ""),
                    accent=node_color,
                    fill=_mix_hex(node_color, "#FFFFFF", 0.9),
                )
            )
            expense_y += expense_height + expense_gap
    else:
        body.append(
            f'<rect x="{panel_xs[1] + row_left:.1f}" y="{panel_y + 92:.1f}" width="{row_width:.1f}" height="76" rx="20" fill="#FFFFFF" stroke="#DCE4EE"/>'
        )
        body.append(_text_block(panel_xs[1] + 36, panel_y + 118, _wrap_label("当前官方材料未单独披露更细费用项，因此保留成本与总经营费用主科目。", 22), font_size=11.2, fill="#475569", weight=500, line_height=12))

    metric_w = 154
    metric_h = 100
    metric_gap_x = 10
    metric_gap_y = 12
    metric_origin_x = panel_xs[2] + 18
    metric_origin_y = panel_y + 74
    for index, metric in enumerate(metrics):
        row = index // 2
        col = index % 2
        tile_x = metric_origin_x + col * (metric_w + metric_gap_x)
        tile_y = metric_origin_y + row * (metric_h + metric_gap_y)
        body.append(
            _statement_metric_tile_svg(
                tile_x,
                tile_y,
                metric_w,
                metric_h,
                label_cn=str(metric["label_cn"]),
                label_en=str(metric["label_en"]),
                value_text=str(metric["value"]),
                note_text=str(metric["note"]),
                accent=str(metric["accent"]),
            )
        )

    pills = [
        ("毛利率", format_pct(statement.get("gross_margin_pct")), _mix_hex(primary, "#16A34A", 0.55)),
        ("经营利润率", format_pct(statement.get("operating_margin_pct")), _mix_hex(accent, "#0F766E", 0.3)),
        ("净利率", format_pct(statement.get("net_margin_pct")), _mix_hex(primary, "#111827", 0.5)),
    ]
    pill_x = 806
    pill_y = height - 52
    for label_cn, value_text, pill_color in pills:
        pill_w = 112 if len(label_cn) <= 4 else 126
        body.append(f'<rect x="{pill_x:.1f}" y="{pill_y:.1f}" width="{pill_w:.1f}" height="28" rx="14" fill="{_mix_hex(pill_color, "#FFFFFF", 0.86)}"/>')
        body.append(f'<text x="{pill_x + 12:.1f}" y="{pill_y + 18:.1f}" font-size="10.6" font-weight="700" fill="#334155">{_escape(label_cn)}</text>')
        body.append(f'<text x="{pill_x + pill_w - 10:.1f}" y="{pill_y + 18:.1f}" text-anchor="end" font-size="11.0" font-weight="800" fill="{_mix_hex(pill_color, "#0F172A", 0.18)}">{_escape(value_text)}</text>')
        pill_x += pill_w + 8
    body.append(f'<text x="34" y="{height - 18:.1f}" font-size="10.6" fill="#64748B">英文原词保留，便于快速回到新闻稿、10-Q / 10-K 或 shareholder deck 继续核查。</text>')

    return _svg(width, height, "".join(body))


def _trim_note(text: Any, limit: int = 108) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    search_window = normalized[: limit + 1]
    preferred_cutoff = max(
        search_window.rfind(token)
        for token in ("。", "！", "？", "；", ".", "!", "?", ";", "，", ",", " ")
    )
    minimum_boundary = max(10, int(limit * 0.55))
    if preferred_cutoff >= minimum_boundary:
        if search_window[preferred_cutoff] == " ":
            return search_window[:preferred_cutoff].rstrip(" ,.;:，；")
        return search_window[: preferred_cutoff + 1].rstrip(" ,.;:，；")
    return normalized[:limit].rstrip(" ,.;:，；")


def _score_tone(score: float) -> str:
    if score >= 82:
        return "高"
    if score >= 64:
        return "中"
    return "低"


def _signal_topic_card_svg(
    x: float,
    y: float,
    width: float,
    height: float,
    title: str,
    note: str,
    score: float,
    accent: str,
    rank: int,
) -> str:
    score_fill = _mix_hex(accent, "#FFFFFF", 0.86)
    score_text = int(round(score))
    label_lines = _wrap_visual_text_lines(title, 19.4, 2)
    label_font = 12.0 if len("".join(label_lines)) <= 28 else 11.5
    label_y = y + 30
    label_line_height = 11.0
    note_font = 10.1
    note_line_height = 10.3
    note_y = label_y + label_font + max(len(label_lines) - 1, 0) * label_line_height + 7
    footer_y = y + height - 12
    note_line_budget = max(2, min(5, int((footer_y - 10 - note_y) // note_line_height)))
    note_lines = _wrap_visual_text_lines(note, 23.0, note_line_budget)
    if len(note_lines) >= 5:
        note_font = 9.6
        note_line_height = 9.8
        note_line_budget = max(2, min(5, int((footer_y - 10 - note_y) // note_line_height)))
        note_lines = _wrap_visual_text_lines(note, 22.0, note_line_budget)
    return "".join(
        [
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height:.1f}" rx="20" fill="#FFFFFF" stroke="{_mix_hex(accent, "#CBD5E1", 0.55)}"/>',
            f'<text x="{x + 16:.1f}" y="{y + 18:.1f}" font-size="9.0" letter-spacing="0.14em" fill="#94A3B8">TOP {rank:02d}</text>',
            f'<circle cx="{x + width - 22:.1f}" cy="{y + 22:.1f}" r="14.5" fill="{score_fill}"/>',
            f'<text x="{x + width - 22:.1f}" y="{y + 26:.1f}" text-anchor="middle" font-size="10.8" font-weight="700" fill="{accent}">{score_text}</text>',
            _text_block(x + 16, label_y, label_lines, font_size=label_font, fill="#0F172A", weight=700, line_height=label_line_height),
            _text_block(x + 16, note_y, note_lines, font_size=note_font, fill="#475569", line_height=note_line_height),
            f'<text x="{x + width - 16:.1f}" y="{footer_y:.1f}" text-anchor="end" font-size="8.6" fill="#94A3B8">{_score_tone(score)} / {score_text}</text>',
        ]
    )


def _signal_panel_svg(
    x: float,
    y: float,
    width: float,
    height: float,
    title: str,
    items: list[dict[str, Any]],
    color: str,
    subtitle: str,
) -> str:
    subtitle_lines = _wrap_visual_text_lines(subtitle, 30.5, 2)
    body = [
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height:.1f}" rx="28" fill="{_mix_hex(color, "#FFFFFF", 0.95)}" stroke="{_mix_hex(color, "#CBD5E1", 0.72)}"/>',
        f'<text x="{x + 18:.1f}" y="{y + 23:.1f}" font-size="10.6" letter-spacing="0.14em" fill="#94A3B8">SIGNAL BOARD</text>',
        f'<text x="{x + 18:.1f}" y="{y + 45:.1f}" font-size="17.2" font-weight="700" fill="#0F172A">{_escape(title)}</text>',
        _text_block(x + 18, y + 60, subtitle_lines, font_size=10.2, fill="#64748B", line_height=10.6),
    ]
    panel_items = items[:4]
    if not panel_items:
        panel_items = [
            {
                "label": "暂无足够结构化主题",
                "note": "当前页面会继续保留摘要与证据卡片，避免为了图表而硬造主题强度。",
                "score": 0,
            }
        ]
    card_gap_x = 12
    card_gap_y = 10
    card_width = (width - 18 * 2 - card_gap_x) / 2
    grid_top = y + 70 + len(subtitle_lines) * 10.6
    grid_bottom = y + height - 16
    card_height = max(114.0, min(132.0, (grid_bottom - grid_top - card_gap_y) / 2))
    for index, item in enumerate(panel_items):
        row = index // 2
        col = index % 2
        card_x = x + 18 + col * (card_width + card_gap_x)
        card_y = grid_top + row * (card_height + card_gap_y)
        body.append(
            _signal_topic_card_svg(
                card_x,
                card_y,
                card_width,
                card_height,
                str(item.get("label") or "-"),
                str(item.get("note") or "当前主题暂无更长摘要。"),
                float(item.get("score") or 0.0),
                color,
                index + 1,
            )
        )
    if len(panel_items) < 4:
        for index in range(len(panel_items), 4):
            row = index // 2
            col = index % 2
            card_x = x + 18 + col * (card_width + card_gap_x)
            card_y = grid_top + row * (card_height + card_gap_y)
            body.append(
                f'<rect x="{card_x:.1f}" y="{card_y:.1f}" width="{card_width:.1f}" height="{card_height:.1f}" rx="20" fill="rgba(255,255,255,0.5)" stroke="#E2E8F0" stroke-dasharray="5 5"/>'
            )
    return "".join(body)


def render_ranked_bars_svg(title: str, items: list[dict[str, Any]], color: str, subtitle: str) -> str:
    return _svg(
        566,
        306,
        _signal_panel_svg(8, 8, 550, 290, title, items, color, subtitle),
    )


def render_dual_ranked_svg(
    left_title: str,
    left_items: list[dict[str, Any]],
    left_color: str,
    right_title: str,
    right_items: list[dict[str, Any]],
    right_color: str,
    left_subtitle: str = "按管理层讨论密度与重要性打分",
    right_subtitle: str = "按问答热度与研究关注度打分",
) -> str:
    width, height = 1180, 396
    left_panel_x = 16
    panel_y = 16
    panel_width = 566
    panel_height = 364
    right_panel_x = width - 16 - panel_width
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        _signal_panel_svg(left_panel_x, panel_y, panel_width, panel_height, left_title, left_items, left_color, left_subtitle),
        _signal_panel_svg(right_panel_x, panel_y, panel_width, panel_height, right_title, right_items, right_color, right_subtitle),
    ]
    return _svg(width, height, "".join(body))


def _segment_share_map(entry: dict[str, object]) -> dict[str, float]:
    items, _basis = _entry_structure_items(entry)
    total = sum(float(item.get("value_bn") or 0.0) for item in items)
    if total <= 0:
        return {}
    return {
        str(item.get("name") or "Business"): float(item.get("value_bn") or 0.0) / total
        for item in items
        if float(item.get("value_bn") or 0.0) > 0
    }


_GEO_STRUCTURE_MARKERS = (
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
    "pacific",
    "singapore",
    "taiwan",
    "u s",
    "united states",
    "world",
)


def _looks_like_geography_labels(names: list[str]) -> bool:
    cleaned = []
    for name in names:
        token = re.sub(r"[^a-z0-9]+", " ", str(name or "").casefold()).strip()
        if token:
            cleaned.append(token)
    if not cleaned:
        return False
    return all(any(marker in token for marker in _GEO_STRUCTURE_MARKERS) for token in cleaned)


def _entry_structure_items(entry: dict[str, object]) -> tuple[list[dict[str, object]], Optional[str]]:
    segments = list(entry.get("segments") or [])
    geographies = list(entry.get("geographies") or [])
    basis = str(entry.get("structure_basis") or "").lower()
    segment_names = [str(item.get("name") or "") for item in segments]
    segments_geo_like = _looks_like_geography_labels(segment_names)
    if basis == "geography" and geographies:
        return geographies, "geography"
    if segments and not segments_geo_like:
        return segments, "segment"
    if geographies:
        return geographies, "geography"
    if segments:
        return segments, "segment"
    return [], None


def _normalized_entry_structure_items(entry: dict[str, object]) -> tuple[list[dict[str, object]], Optional[str]]:
    items, basis = _entry_structure_items(entry)
    if not items:
        return [], basis
    value_total = sum(float(item.get("value_bn") or 0.0) for item in items if item.get("value_bn") is not None)
    share_total = sum(float(item.get("share_pct") or 0.0) for item in items if item.get("share_pct") is not None)
    normalized: list[dict[str, object]] = []
    if value_total > 0:
        for item in items:
            value = float(item.get("value_bn") or 0.0)
            if value <= 0:
                continue
            payload = dict(item)
            payload["share_pct"] = value / value_total * 100
            normalized.append(payload)
        return normalized, basis
    if share_total > 0:
        for item in items:
            share = float(item.get("share_pct") or 0.0)
            if share <= 0:
                continue
            payload = dict(item)
            payload["share_pct"] = share / share_total * 100
            normalized.append(payload)
        return normalized, basis
    return [], basis


def _dominant_history_structure_basis(entries: list[dict[str, object]]) -> str:
    observed = [
        basis
        for entry in entries
        for _items, basis in [_entry_structure_items(entry)]
        if basis
    ]
    if not observed:
        return "segment"
    return "geography" if observed.count("geography") > observed.count("segment") else "segment"


def _growth_stack_plan(entries: list[dict[str, object]]) -> tuple[list[str], list[dict[str, object]], bool]:
    share_maps = [_segment_share_map(entry) for entry in entries]
    anchor_segments = next((_entry_structure_items(entry)[0] for entry in reversed(entries) if _entry_structure_items(entry)[0]), [])
    segment_names = [str(item.get("name") or "Business") for item in anchor_segments]
    for share_map in share_maps:
        for name in share_map:
            if name not in segment_names:
                segment_names.append(name)
    available_indexes = [index for index, share_map in enumerate(share_maps) if share_map]
    if not available_indexes or not segment_names:
        return ([], [], False)

    plan: list[dict[str, object]] = []
    inferred_any = False
    for index, entry in enumerate(entries):
        share_map = share_maps[index]
        inferred = bool(entry.get("segments_inferred"))
        if not share_map:
            prev_index = next((value for value in reversed(available_indexes) if value < index), None)
            next_index = next((value for value in available_indexes if value > index), None)
            candidate_map: dict[str, float] = {}
            if prev_index is not None and next_index is not None:
                span = max(1, next_index - prev_index)
                weight = (index - prev_index) / span
                prev_map = share_maps[prev_index]
                next_map = share_maps[next_index]
                for name in segment_names:
                    candidate_map[name] = prev_map.get(name, 0.0) * (1 - weight) + next_map.get(name, 0.0) * weight
            elif prev_index is not None:
                candidate_map = {name: share_maps[prev_index].get(name, 0.0) for name in segment_names}
            elif next_index is not None:
                candidate_map = {name: share_maps[next_index].get(name, 0.0) for name in segment_names}
            share_total = sum(candidate_map.values())
            if share_total > 0:
                share_map = {name: value / share_total for name, value in candidate_map.items() if value > 0}
                inferred = True
                inferred_any = True
        if inferred:
            inferred_any = True

        revenue_bn = float(entry.get("revenue_bn") or 0.0)
        stacked_segments = []
        for name in segment_names:
            share = float(share_map.get(name, 0.0))
            if share <= 0:
                continue
            stacked_segments.append({"name": name, "share": share, "value_bn": revenue_bn * share})
        plan.append({"segments": stacked_segments, "inferred": inferred})
    return (segment_names, plan, inferred_any)


def render_growth_overview_svg(
    entries: list[dict[str, object]],
    colors: dict[str, str],
    primary: str,
    money_symbol: str = "$",
    title: str = "近 12 季成长总览",
) -> str:
    width, height = 1180, 428
    chart_left = 74
    chart_top = 74
    chart_width = 1020
    chart_height = 228
    totals = [float(entry.get("revenue_bn") or 0.0) for entry in entries]
    has_total_data = any(entry.get("revenue_bn") is not None for entry in entries)
    max_total = max(totals) if any(totals) else 1
    segment_names, stack_plan, inferred_segments = _growth_stack_plan(entries)
    has_segment_stacks = bool(stack_plan and segment_names)
    structure_basis = _dominant_history_structure_basis(entries)
    structure_label = "业务结构" if structure_basis != "geography" else "地区结构"
    legend_label = "业务类型" if structure_basis != "geography" else "地区类型"
    palette = _segment_palette(segment_names, colors, primary)
    empty_title = title.replace("成长总览", "收入序列")
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        f'<text x="32" y="36" font-size="18" font-weight="700" fill="#0F172A">{_escape(title)}</text>',
        (
            f'<text x="32" y="56" font-size="12" fill="#64748B">每个季度柱体按{structure_label}分段上色；带 * 的季度按邻近官方结构占比补足显示，并保持全报告颜色映射一致。</text>'
            if inferred_segments
            else f'<text x="32" y="56" font-size="12" fill="#64748B">每个季度柱体按{structure_label}分段上色，并保持全报告颜色映射一致。</text>'
        )
        if has_segment_stacks
        else '<text x="32" y="56" font-size="12" fill="#64748B">当前历史结构尚未完整接入时，先展示总收入柱和总量折线，不伪造分段占比。</text>',
    ]
    if not has_total_data:
        body.extend(
            [
                '<rect x="74" y="92" width="1032" height="286" rx="24" fill="#F8FAFC" stroke="#E2E8F0" stroke-dasharray="7 7"/>',
                f'<text x="108" y="154" font-size="20" font-weight="700" fill="{primary}">{_escape(empty_title)} 暂未接入完整数值</text>',
                '<text x="108" y="190" font-size="14" fill="#334155">系统会继续展示当季页、电话会页与指引页；历史成长图在无可靠收入序列时不再强行绘制伪数据。</text>',
                '<text x="108" y="222" font-size="13" fill="#64748B">可用季度：</text>',
                "".join(
                    f'<text x="{108 + (index % 6) * 152}" y="{250 + (index // 6) * 26}" font-size="12.5" fill="#475569">{_escape(str(entry.get("quarter_label") or "-"))}</text>'
                    for index, entry in enumerate(entries[:12])
                ),
            ]
        )
        return _svg(width, height, "".join(body))
    for step in range(5):
        y = chart_top + chart_height - step * chart_height / 4
        label = max_total * step / 4
        body.append(f'<line x1="{chart_left}" y1="{y:.1f}" x2="{chart_left+chart_width}" y2="{y:.1f}" stroke="#E2E8F0"/>')
        body.append(
            f'<text x="{chart_left-12}" y="{y+4:.1f}" text-anchor="end" font-size="12" fill="#64748B">{label:.0f}</text>'
        )
    step_x = chart_width / max(1, len(entries))
    bar_width = step_x * 0.64
    line_points: list[tuple[float, float]] = []
    for index, entry in enumerate(entries):
        x = chart_left + index * step_x + step_x * 0.18
        y_cursor = chart_top + chart_height
        stacked_entry = stack_plan[index] if has_segment_stacks else {"segments": [], "inferred": False}
        if has_segment_stacks and stacked_entry["segments"]:
            segment_map = {
                str(segment["name"]): float(segment["value_bn"])
                for segment in stacked_entry["segments"]  # type: ignore[index]
            }
            for name in segment_names:
                value_bn = segment_map.get(name, 0.0)
                if value_bn <= 0:
                    continue
                bar_height = value_bn / max_total * chart_height
                y_cursor -= bar_height
                body.append(
                    f'<rect x="{x:.1f}" y="{y_cursor:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" rx="6" fill="{palette.get(name, primary)}" stroke="#FFFFFF" stroke-width="1.2"/>'
                )
        else:
            total = float(entry.get("revenue_bn") or 0.0)
            bar_height = total / max_total * chart_height
            y = chart_top + chart_height - bar_height
            body.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" rx="12" fill="{primary}" opacity="0.84"/>')
        total_y = chart_top + chart_height - (float(entry.get("revenue_bn") or 0.0) / max_total * chart_height)
        center_x = x + bar_width / 2
        line_points.append((center_x, total_y))
        quarter_label = str(entry["quarter_label"])
        if has_segment_stacks and bool(stacked_entry.get("inferred")):
            quarter_label += "*"
        body.append(
            f'<text x="{center_x:.1f}" y="{chart_top + chart_height + 27:.1f}" text-anchor="middle" font-size="11" fill="#64748B">{_escape(quarter_label)}</text>'
        )
    if line_points:
        path = "M " + " L ".join(f"{x:.1f} {y:.1f}" for x, y in line_points)
        body.append(f'<path d="{path}" fill="none" stroke="#0F172A" stroke-width="3"/>')
        for x, y in line_points:
            body.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="#0F172A"/>')
        first_x, first_y = line_points[0]
        last_x, last_y = line_points[-1]
        body.append(
            f'<text x="{first_x:.1f}" y="{first_y-12:.1f}" text-anchor="middle" font-size="12" fill="#0F172A">{format_money_bn(entries[0].get("revenue_bn"), money_symbol)}</text>'
        )
        body.append(
            f'<text x="{last_x:.1f}" y="{last_y-12:.1f}" text-anchor="middle" font-size="12" fill="#0F172A">{format_money_bn(entries[-1].get("revenue_bn"), money_symbol)}</text>'
        )
    if has_segment_stacks and segment_names:
        legend_items = segment_names[:6]
        legend_rows = 1 if len(legend_items) <= 3 else 2
        if inferred_segments:
            legend_rows = max(2, legend_rows)
        legend_box_y = 342 if legend_rows == 2 else 352
        legend_box_h = 74 if legend_rows == 2 else 46
        body.append(
            f'<rect x="74" y="{legend_box_y:.1f}" width="1036" height="{legend_box_h:.1f}" rx="18" fill="#F8FAFC" stroke="#E2E8F0"/>'
        )
        body.append(f'<text x="96" y="{legend_box_y + 22:.1f}" font-size="11" letter-spacing="0.12em" fill="#94A3B8">COLOR LEGEND</text>')
        legend_x = 206
        legend_y = legend_box_y + 38
        for index, name in enumerate(legend_items):
            col = index % 3
            row = index // 3
            x = legend_x + col * 278
            y = legend_y + row * 20
            body.extend(
                [
                    f'<rect x="{x:.1f}" y="{y - 9:.1f}" width="12" height="12" rx="4" fill="{palette.get(name, primary)}"/>',
                    f'<text x="{x + 18:.1f}" y="{y + 1:.1f}" font-size="11.5" fill="#334155">{_escape(_compact_category_label(name))}</text>',
                ]
            )
        body.append(f'<text x="1090" y="{legend_box_y + 22:.1f}" text-anchor="end" font-size="11" fill="#94A3B8">颜色对应不同{legend_label}</text>')
        if len(segment_names) > len(legend_items):
            body.append(
                f'<text x="1090" y="{legend_box_y + legend_box_h - 10:.1f}" text-anchor="end" font-size="10.5" fill="#94A3B8">其余细分{legend_label}已并入颜色映射并在后续结构页展开</text>'
            )
        elif inferred_segments:
            body.append(
                f'<text x="1090" y="{legend_box_y + legend_box_h - 10:.1f}" text-anchor="end" font-size="10.5" fill="#94A3B8">* 表示该季度按邻近官方结构占比补足展示</text>'
            )
    return _svg(width, height, "".join(body))


def render_structure_transition_svg(entries: list[dict[str, object]], colors: dict[str, str], primary: str, fallback_note: Optional[str] = None) -> str:
    width, height = 1180, 372
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        '<text x="32" y="36" font-size="18" font-weight="700" fill="#0F172A">结构迁移与集中度变化</text>',
    ]
    normalized_entries = [
        {
            **dict(entry),
            "_normalized_structure_items": _normalized_entry_structure_items(entry)[0],
        }
        for entry in entries
    ]
    complete = all(list(entry.get("_normalized_structure_items") or []) for entry in normalized_entries)
    structure_basis = _dominant_history_structure_basis(entries)
    structure_label = "业务分部" if structure_basis != "geography" else "地区结构"
    if not complete:
        note = fallback_note or "当前历史分部数据不足，结构页自动降级为管理层结构说明。"
        body.extend(
            [
                '<rect x="42" y="76" width="1096" height="228" rx="24" fill="#F8FAFC" stroke="#E2E8F0"/>',
                f'<text x="76" y="128" font-size="18" font-weight="700" fill="{primary}">结构数据自动降级</text>',
                f'<text x="76" y="164" font-size="14" fill="#334155">{_escape(note)}</text>',
                '<text x="76" y="198" font-size="14" fill="#64748B">系统仍保留 12 季总量成长、利润质量与管理层主题分析，以避免页面留白。</text>',
            ]
        )
        return _svg(width, height, "".join(body))

    left = 70
    top = 64
    chart_width = 1030
    chart_height = 190
    step_x = chart_width / max(1, len(entries))
    bar_width = step_x * 0.66
    latest_items = list(normalized_entries[-1].get("_normalized_structure_items") or [])
    segment_names = [str(segment.get("name") or "Business") for segment in latest_items]
    palette = _segment_palette(segment_names, colors, primary)
    for index, entry in enumerate(normalized_entries):
        x = left + index * step_x + step_x * 0.17
        y_cursor = top + chart_height
        for segment in list(entry.get("_normalized_structure_items") or []):
            share_pct = float(segment["share_pct"])
            part_height = share_pct / 100 * chart_height
            y_cursor -= part_height
            body.append(
                f'<rect x="{x:.1f}" y="{y_cursor:.1f}" width="{bar_width:.1f}" height="{part_height:.1f}" rx="10" fill="{palette.get(str(segment["name"]), primary)}"/>'
            )
        body.append(
            f'<text x="{x + bar_width/2:.1f}" y="{top+chart_height+22:.1f}" text-anchor="middle" font-size="11" fill="#64748B">{_escape(str(entry["quarter_label"]))}</text>'
        )
    first = max(list(normalized_entries[0].get("_normalized_structure_items") or []), key=lambda item: float(item.get("share_pct") or 0.0))
    last = max(list(normalized_entries[-1].get("_normalized_structure_items") or []), key=lambda item: float(item.get("share_pct") or 0.0))
    legend_items = segment_names[:6]
    legend_rows = 1 if len(legend_items) <= 3 else 2
    legend_y = 280 if legend_rows == 1 else 266
    legend_h = 78 if legend_rows == 1 else 98
    body.append(f'<rect x="62" y="{legend_y:.1f}" width="1056" height="{legend_h}" rx="18" fill="#F8FAFC" stroke="#E2E8F0"/>')
    body.append(
        f'<text x="84" y="{legend_y + 19:.1f}" font-size="11" letter-spacing="0.12em" fill="#94A3B8">STRUCTURE LEGEND</text>'
    )
    body.append(
        f'<text x="84" y="{legend_y + 39:.1f}" font-size="11.5" fill="#475569">头部{structure_label}占比：{_escape(_compact_category_label(str(first["name"])))} {format_pct(float(first["share_pct"]))} → {_escape(_compact_category_label(str(last["name"])))} {format_pct(float(last["share_pct"]))}</text>'
    )
    for index, name in enumerate(legend_items):
        col = index % 3
        row = index // 3
        x = 620 + col * 166
        y = legend_y + (58 if legend_rows == 2 else 30) + row * 20
        body.append(f'<rect x="{x:.1f}" y="{y - 8:.1f}" width="12" height="12" rx="4" fill="{palette.get(name, primary)}"/>')
        body.append(f'<text x="{x + 18:.1f}" y="{y + 2:.1f}" font-size="{10.2 if legend_rows == 2 else 10.8}" fill="#334155">{_escape(_trim_note(_compact_category_label(name), 22 if legend_rows == 2 else 24))}</text>')
    if len(segment_names) > len(legend_items):
        body.append(f'<text x="1094" y="{legend_y + legend_h - 12:.1f}" text-anchor="end" font-size="10.2" fill="#94A3B8">其余业务已并入统一颜色映射</text>')
    return _svg(width, height, "".join(body))


def _sparkline_markup(values: list[float], color: str, start_label: str, end_label: str) -> str:
    width, height = 240, 78
    path, points = _series_points(values, 12, 14, 216, 38)
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="18" fill="#F8FAFC"/>',
        '<line x1="12" y1="52" x2="228" y2="52" stroke="#E2E8F0"/>',
    ]
    if path:
        body.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>')
        for point_x, point_y in points:
            body.append(f'<circle cx="{point_x:.1f}" cy="{point_y:.1f}" r="3.2" fill="{color}"/>')
    body.append(f'<text x="12" y="70" font-size="10.5" fill="#94A3B8">{_escape(start_label)}</text>')
    body.append(f'<text x="228" y="70" text-anchor="end" font-size="10.5" fill="#94A3B8">{_escape(end_label)}</text>')
    return _svg(width, height, "".join(body))


def render_profitability_svg(
    entries: list[dict[str, object]],
    primary: str,
    secondary: str,
    profit_margin_label: str = "净利率",
) -> str:
    metric_candidates = [
        ("gross_margin_pct", "毛利率", primary),
        ("net_margin_pct", profit_margin_label, secondary),
        ("revenue_yoy_pct", "收入同比", "#0F172A"),
        ("roe_pct", "ROE (TTM)", "#F59E0B"),
        ("net_income_yoy_pct", "净利润同比", "#C2410C"),
        ("ttm_revenue_growth_pct", "TTM 增速", "#7C3AED"),
    ]
    selected: list[tuple[str, str, str, list[float], bool]] = []
    for key, label, color in metric_candidates:
        raw_values = [entry.get(key) for entry in entries]
        actual_values = [float(value) for value in raw_values if value is not None]
        if not actual_values:
            continue
        values: list[float] = []
        carry = actual_values[0]
        for value in raw_values:
            if value is not None:
                carry = float(value)
            values.append(carry)
        selected.append((key, label, color, values, True))
        if len(selected) == 4:
            break
    if len(selected) < 4:
        chosen_keys = {item[0] for item in selected}
        for key, label, color in metric_candidates:
            if key in chosen_keys:
                continue
            selected.append((key, label, color, [], False))
            if len(selected) == 4:
                break

    width, height = 1180, 318
    card_width = 562
    card_height = 132
    start_label = str(entries[0]["quarter_label"])
    end_label = str(entries[-1]["quarter_label"])
    body = [f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>']
    for index, (key, label, color, values, available) in enumerate(selected[:4]):
        col = index % 2
        row = index // 2
        x = 16 + col * (card_width + 24)
        y = 16 + row * (card_height + 18)
        body.append(
            f'<rect x="{x}" y="{y}" width="{card_width}" height="{card_height}" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>'
        )
        body.append(f'<text x="{x + 18}" y="{y + 28}" font-size="12" fill="#64748B">{_escape(label)}</text>')
        if not available or not values:
            body.append(f'<text x="{x + 18}" y="{y + 60}" font-size="22" font-weight="700" fill="#94A3B8">未接入</text>')
            body.append(
                f'<text x="{x + 18}" y="{y + 86}" font-size="11.5" fill="#94A3B8">当前结构化季度序列缺少该指标，系统已自动保留卡位。</text>'
            )
            continue
        latest = values[-1]
        delta = latest - values[0]
        spark_left = x + 18
        spark_top = y + 68
        spark_width = card_width - 36
        spark_height = 32
        path, points = _series_points(values, spark_left, spark_top, spark_width, spark_height)
        body.extend(
            [
                f'<text x="{x + card_width - 18}" y="{y + 36}" text-anchor="end" font-size="28" font-weight="700" fill="#0F172A">{format_pct(latest)}</text>',
                f'<text x="{x + 18}" y="{y + 48}" font-size="11.5" font-weight="700" fill="{color}">12 季变化 {format_pct(delta, signed=True)}</text>',
                f'<text x="{x + card_width - 18}" y="{y + 58}" text-anchor="end" font-size="11.5" fill="#94A3B8">{format_pct(min(values))} - {format_pct(max(values))}</text>',
                f'<rect x="{spark_left}" y="{spark_top - 8}" width="{spark_width}" height="{spark_height + 22}" rx="18" fill="#F8FAFC"/>',
                f'<line x1="{spark_left}" y1="{spark_top + spark_height + 2}" x2="{spark_left + spark_width}" y2="{spark_top + spark_height + 2}" stroke="#E2E8F0"/>',
            ]
        )
        if path:
            body.append(
                f'<path d="{path}" fill="none" stroke="{color}" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round"/>'
            )
            for point_x, point_y in points:
                body.append(f'<circle cx="{point_x:.1f}" cy="{point_y:.1f}" r="3.4" fill="{color}"/>')
        body.append(f'<text x="{spark_left}" y="{y + card_height - 12}" font-size="10.5" fill="#94A3B8">{_escape(start_label)}</text>')
        body.append(
            f'<text x="{spark_left + spark_width}" y="{y + card_height - 12}" text-anchor="end" font-size="10.5" fill="#94A3B8">{_escape(end_label)}</text>'
        )
    return _svg(width, height, "".join(body))


def render_contribution_svg(entries: list[dict[str, object]], colors: dict[str, str], primary: str, fallback_note: Optional[str] = None, money_symbol: str = "$") -> str:
    width, height = 1180, 300
    complete = all(_entry_structure_items(entry)[0] for entry in entries)
    structure_basis = _dominant_history_structure_basis(entries)
    structure_label = "分部" if structure_basis != "geography" else "地区"
    body = [
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>',
        '<text x="32" y="36" font-size="18" font-weight="700" fill="#0F172A">增长拆解与增量来源</text>',
    ]
    if not complete:
        note = fallback_note or "当前无法构建连续 12 季分部增量拆解，系统转而展示总量成长与管理层结构主题。"
        body.extend(
            [
                '<rect x="42" y="74" width="1096" height="176" rx="24" fill="#F8FAFC" stroke="#E2E8F0"/>',
                f'<text x="76" y="122" font-size="18" font-weight="700" fill="{primary}">增量拆解降级</text>',
                f'<text x="76" y="158" font-size="14" fill="#334155">{_escape(note)}</text>',
                '<text x="76" y="190" font-size="14" fill="#64748B">这通常意味着季度分部历史披露不足，而不是报告生成失败。</text>',
            ]
        )
        return _svg(width, height, "".join(body))

    valid_revenue_entries = [entry for entry in entries if entry.get("revenue_bn") is not None]
    if len(valid_revenue_entries) < 2:
        note = fallback_note or "当前无法构建连续 12 季收入增量拆解，系统转而展示结构方向与管理层口径。"
        body.extend(
            [
                '<rect x="42" y="74" width="1096" height="176" rx="24" fill="#F8FAFC" stroke="#E2E8F0"/>',
                f'<text x="76" y="122" font-size="18" font-weight="700" fill="{primary}">增量拆解降级</text>',
                f'<text x="76" y="158" font-size="14" fill="#334155">{_escape(note)}</text>',
                '<text x="76" y="190" font-size="14" fill="#64748B">通常是历史收入序列仍在补齐中，报告会继续保留其他趋势页。</text>',
            ]
        )
        return _svg(width, height, "".join(body))

    first_revenue_entry = valid_revenue_entries[0]
    last_revenue_entry = valid_revenue_entries[-1]
    first_segments = {
        segment["name"]: float(segment["value_bn"])
        for segment in _entry_structure_items(first_revenue_entry)[0]
    }
    last_segments = {
        segment["name"]: float(segment["value_bn"])
        for segment in _entry_structure_items(last_revenue_entry)[0]
    }
    names = []
    for name in list(last_segments.keys()) + list(first_segments.keys()):
        if name not in names:
            names.append(name)
    deltas = [last_segments.get(name, 0.0) - first_segments.get(name, 0.0) for name in names]
    scale = max((abs(delta) for delta in deltas), default=1.0)
    for index, name in enumerate(names):
        y = 82 + index * 38
        width_px = abs(deltas[index]) / scale * 760 if scale else 0
        color = colors.get(name, primary)
        value_x = 860
        delta_label = format_money_bn(deltas[index], money_symbol)
        body.extend(
            [
                f'<text x="42" y="{y}" font-size="12" fill="#334155">{_escape(name)}<tspan fill="#94A3B8"> {structure_label}</tspan></text>',
                f'<rect x="42" y="{y+10}" width="800" height="14" rx="7" fill="#E2E8F0"/>',
                f'<rect x="42" y="{y+10}" width="{width_px:.1f}" height="14" rx="7" fill="{color}"/>',
                f'<text x="{value_x}" y="{y+22}" font-size="12" fill="#0F172A">{delta_label}</text>',
            ]
        )
    total_delta = float(last_revenue_entry["revenue_bn"]) - float(first_revenue_entry["revenue_bn"])
    first_label = _escape(str(first_revenue_entry.get("quarter_label") or "-"))
    last_label = _escape(str(last_revenue_entry.get("quarter_label") or "-"))
    body.append(
        f'<text x="42" y="274" font-size="12" fill="#475569">总收入增量：{format_money_bn(total_delta, money_symbol)} | 观察期：{first_label} → {last_label}</text>'
    )
    return _svg(width, height, "".join(body))


def _coverage_state(score: float) -> str:
    if score >= 88:
        return "覆盖完整"
    if score >= 60:
        return "部分覆盖"
    return "待补强"


def render_materials_svg(materials: Iterable[dict[str, object]], primary: str) -> str:
    width, height = 1180, 286
    card_width = 562
    card_height = 110
    items_list = list(materials)[:4]
    body = [f'<rect x="0" y="0" width="{width}" height="{height}" rx="28" fill="#FFFFFF"/>']
    for index in range(4):
        col = index % 2
        row = index // 2
        x = 16 + col * (card_width + 24)
        y = 16 + row * (card_height + 18)
        body.append(
            f'<rect x="{x}" y="{y}" width="{card_width}" height="{card_height}" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>'
        )
        if index >= len(items_list):
            body.append(
                f'<text x="{x + 18}" y="{y + 50}" font-size="15" font-weight="700" fill="#94A3B8">等待补充</text>'
            )
            body.append(
                f'<text x="{x + 18}" y="{y + 76}" font-size="11.5" fill="#94A3B8">当前公司或季度尚未接入更多可追溯材料。</text>'
            )
            continue
        item = items_list[index]
        score = float(item.get("score") or 0.0)
        body.extend(
            [
                f'<text x="{x + 18}" y="{y + 30}" font-size="12" fill="#64748B">{_escape(str(item.get("label") or "-"))}</text>',
                f'<text x="{x + 18}" y="{y + 70}" font-size="30" font-weight="700" fill="{primary}">{int(round(score))}</text>',
                f'<text x="{x + card_width - 18}" y="{y + 34}" text-anchor="end" font-size="11.5" fill="#94A3B8">覆盖评分</text>',
                f'<text x="{x + 18}" y="{y + 92}" font-size="13" font-weight="700" fill="#334155">{_escape(_coverage_state(score))}</text>',
                f'<text x="{x + 18}" y="{y + 108}" font-size="11.5" fill="#64748B">评分越高，代表原文可追溯性和自动解析稳定度越高。</text>',
            ]
        )
    return _svg(width, height, "".join(body))
