from __future__ import annotations

import re
from collections import Counter
from typing import Any, Optional

from .charts import format_money_bn, format_pct


TEMPLATE_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    (r"当前未抓到完整电话会\s*transcript\s*时", "这季没有完整电话会实录时"),
    (r"当前未获取到当季完整电话会\s*transcript\s*/\s*Q&A", "这季没有完整电话会问答原文"),
    (r"当前未拿到完整 transcript", "这季没有完整 transcript"),
    (r"按官方财报材料构建代理摘要", "改从财报稿、演示材料和 filing 里整理重点"),
    (r"按官方财报材料构建电话会代理摘要", "电话会部分改从财报稿、演示材料和 filing 里整理"),
    (r"按官方材料动态提炼", "从官方材料里整理"),
    (r"动态提炼", "整理"),
    (r"经营基线", "近几季常态"),
    (r"结构化季度财务序列", "历史财务序列"),
    (r"统一研究模板", "通用研究框架"),
    (r"自动切换为", "改为"),
    (r"自动切换", "改看"),
    (r"自动降级为", "改成更保守的表达"),
    (r"自动降级", "改成更保守的表达"),
    (r"下一阶段收入参考", "后续收入参考"),
    (r"推断问答主题", "可能会被追问的问题"),
    (r"研究关注主题", "研究问题清单"),
    (r"结构限制说明", "结构边界说明"),
)

TEMPLATE_MARKERS = (
    "当前未",
    "系统会",
    "动态提炼",
    "经营基线",
    "结构化季度财务序列",
    "统一研究模板",
    "自动切换",
    "自动降级",
    "代理摘要",
)


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _coalesce_number(*values: Any) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _trim_sentence(text: str) -> str:
    cleaned = _clean(text).strip(" ，,；;。")
    if not cleaned:
        return ""
    return cleaned + ("。" if cleaned[-1] not in "。！？!?" else "")


def _line_looks_readable(text: Optional[str]) -> bool:
    cleaned = _clean(text)
    if not cleaned:
        return False
    if any(ord(char) < 32 and char not in "\t\r\n" for char in cleaned):
        return False
    if any(not char.isprintable() and not char.isspace() for char in cleaned):
        return False
    tokens = cleaned.split()
    readable_terms = re.findall(r"[A-Za-z\u4e00-\u9fff]{2,}", cleaned)
    textual_chars = sum(char.isalpha() or ("\u4e00" <= char <= "\u9fff") for char in cleaned)
    weird_tokens = [
        token
        for token in tokens
        if len(token) >= 12 and not re.search(r"[A-Za-z\u4e00-\u9fff]", token)
    ]
    if len(cleaned) >= 60 and textual_chars / max(1, len(cleaned)) < 0.28:
        return False
    if len(tokens) >= 6 and len(readable_terms) < 3:
        return False
    if len(weird_tokens) >= max(2, len(tokens) // 3):
        return False
    return True


def polish_generated_text(text: Optional[str]) -> str:
    cleaned = _clean(text)
    if not cleaned:
        return ""
    for pattern, replacement in TEMPLATE_REPLACEMENTS:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"；+", "；", cleaned)
    cleaned = re.sub(r"。+", "。", cleaned)
    cleaned = re.sub(r"，+", "，", cleaned)
    return cleaned.strip()


def dedupe_lines(lines: list[str], *, limit: Optional[int] = None) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        cleaned = polish_generated_text(line)
        if not cleaned:
            continue
        normalized = re.sub(r"[，。；;,.!?！？\s]+", "", cleaned)
        if normalized in seen:
            continue
        deduped.append(cleaned)
        seen.add(normalized)
        if limit is not None and len(deduped) >= limit:
            break
    return deduped


def _summary_driver(fixture: dict[str, Any], company: dict[str, Any], fiscal_label: str) -> str:
    candidates = [
        str(company.get("card_headline") or ""),
        str(fixture.get("headline") or ""),
    ] + [str(item) for item in list(fixture.get("takeaways") or [])[:3]]
    company_markers = [
        f"{company.get('english_name') or ''} {fiscal_label}",
        str(company.get("english_name") or ""),
        str(company.get("name") or ""),
    ]
    for candidate in candidates:
        cleaned = polish_generated_text(candidate).strip("。")
        if not cleaned:
            continue
        for marker in company_markers:
            if marker and marker in cleaned:
                left, right = cleaned.split(marker, 1)
                cleaned = left.strip(" ，,；;。") or right.strip(" ，,；;。")
        if len(cleaned) < 8:
            continue
        if any(token in cleaned for token in ("收入", "净利润", "经营利润", "同比", "环比", "EPS")):
            continue
        return cleaned
    return ""


def _margin_label(latest_history: dict[str, Any], latest_kpis: dict[str, Any], company: dict[str, Any]) -> str:
    if latest_history.get("gross_margin_pct") is not None or latest_kpis.get("gaap_gross_margin_pct") is not None:
        return "毛利率"
    return str(company.get("historical_profit_margin_label") or "净利率")


def _select_headline_segment(
    fixture: dict[str, Any],
    revenue_bn: Optional[float],
    revenue_yoy_pct: Optional[float],
) -> Optional[dict[str, Any]]:
    segments = [item for item in list(fixture.get("current_segments") or []) if _coalesce_number(item.get("value_bn")) not in (None, 0)]
    if not segments:
        return None
    if revenue_bn in (None, 0) or revenue_yoy_pct is None:
        return max(segments, key=lambda item: float(item.get("value_bn") or 0.0), default=None)

    standout_candidates: list[tuple[float, float, float, dict[str, Any]]] = []
    for item in segments:
        yoy_pct = _coalesce_number(item.get("yoy_pct"))
        value_bn = _coalesce_number(item.get("value_bn"))
        if yoy_pct is None or value_bn is None:
            continue
        share_pct = value_bn / revenue_bn * 100
        growth_gap = yoy_pct - revenue_yoy_pct
        if growth_gap >= 8 and share_pct >= 5:
            standout_candidates.append((growth_gap, share_pct, value_bn, item))

    if standout_candidates:
        _, _, _, standout = max(standout_candidates, key=lambda item: (item[0], item[1], item[2]))
        return standout

    return max(segments, key=lambda item: float(item.get("value_bn") or 0.0), default=None)


def _metric_clause(
    label: str,
    value_bn: Optional[float],
    money_symbol: str,
    *,
    yoy_pct: Optional[float] = None,
    qoq_pct: Optional[float] = None,
    include_qoq: bool = False,
) -> Optional[str]:
    if value_bn is None:
        return None
    bits = [f"{label} {format_money_bn(value_bn, money_symbol)}"]
    if yoy_pct is not None:
        bits.append(f"同比 {format_pct(yoy_pct, signed=True)}")
    if include_qoq and qoq_pct is not None:
        bits.append(f"环比 {format_pct(qoq_pct, signed=True)}")
    return "，".join(bits)


def compose_summary_headline(
    company: dict[str, Any],
    fiscal_label: str,
    latest_kpis: dict[str, Any],
    latest_history: Optional[dict[str, Any]],
    fixture: Optional[dict[str, Any]],
) -> str:
    latest_history = dict(latest_history or {})
    fixture = dict(fixture or {})
    money_symbol = str(company.get("money_symbol") or "$")
    revenue_bn = _coalesce_number(latest_kpis.get("revenue_bn"), latest_history.get("revenue_bn"))
    revenue_yoy_pct = _coalesce_number(latest_kpis.get("revenue_yoy_pct"), latest_history.get("revenue_yoy_pct"))
    net_income_bn = _coalesce_number(latest_kpis.get("net_income_bn"), latest_history.get("net_income_bn"))
    net_income_yoy_pct = _coalesce_number(latest_kpis.get("net_income_yoy_pct"), latest_history.get("net_income_yoy_pct"))
    current_margin = _coalesce_number(latest_kpis.get("gaap_gross_margin_pct"), latest_history.get("gross_margin_pct"), latest_history.get("net_margin_pct"))
    top_segment = _select_headline_segment(fixture, revenue_bn, revenue_yoy_pct)
    top_geo = max(
        list(fixture.get("current_geographies") or []),
        key=lambda item: float(item.get("value_bn") or 0.0),
        default=None,
    )
    if revenue_bn is None:
        return polish_generated_text(str(fixture.get("headline") or company.get("card_headline") or company.get("description") or ""))

    lead = ""
    if top_segment and _coalesce_number(top_segment.get("yoy_pct")) is not None and revenue_yoy_pct is not None and float(top_segment["yoy_pct"]) >= revenue_yoy_pct + 8:
        lead = f"{company['english_name']} {fiscal_label} 这季最值得记住的，不只是收入规模，而是 {top_segment['name']} 明显跑在公司平均增速前面。"
    elif net_income_yoy_pct is not None and revenue_yoy_pct is not None and net_income_yoy_pct >= revenue_yoy_pct + 15:
        lead = f"{company['english_name']} {fiscal_label} 真正改善的不只是规模，更是利润兑现。"
    elif current_margin is not None and current_margin >= 45:
        lead = f"{company['english_name']} {fiscal_label} 这季的关键不是单看收入，而是利润率站得住。"
    else:
        driver = _summary_driver(fixture, company, fiscal_label)
        if driver:
            lead = f"{company['english_name']} {fiscal_label} 这季的主线很清楚：{driver}。"
        elif top_geo:
            lead = f"{company['english_name']} {fiscal_label} 这季表面看是收入扩大，真正要盯的是 {top_geo['name']} 这边的经营韧性。"
        else:
            lead = f"{company['english_name']} {fiscal_label} 这季不是一句“收入增长”就能概括的季度。"

    fact_clauses = [
        _metric_clause("收入", revenue_bn, money_symbol, yoy_pct=revenue_yoy_pct),
        _metric_clause("净利润", net_income_bn, money_symbol, yoy_pct=net_income_yoy_pct),
    ]
    facts = "；".join(clause for clause in fact_clauses if clause) + "。"

    guidance = dict(fixture.get("guidance") or {})
    outlook = ""
    guidance_revenue = _coalesce_number(guidance.get("revenue_bn"))
    if guidance_revenue is not None and revenue_bn not in (None, 0):
        delta = (guidance_revenue / revenue_bn - 1) * 100
        label = str(guidance.get("comparison_label") or ("下一季指引中枢" if guidance.get("mode") == "official" else "后续收入参考"))
        outlook = f"{label}大致落在 {format_money_bn(guidance_revenue, money_symbol)}，相对本季 {format_pct(delta, signed=True)}。"
    elif str(guidance.get("mode") or "") == "official_context" and guidance.get("commentary"):
        outlook = _trim_sentence(str(guidance.get("commentary") or ""))

    return " ".join(part for part in [lead, facts, outlook] if part).strip()


def compose_layered_takeaways(
    company: dict[str, Any],
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> list[dict[str, str]]:
    latest = dict(history[-1] if history else {})
    latest_kpis = dict(fixture.get("latest_kpis") or {})
    guidance = dict(fixture.get("guidance") or {})
    revenue_bn = _coalesce_number(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    revenue_yoy_pct = _coalesce_number(latest_kpis.get("revenue_yoy_pct"), latest.get("revenue_yoy_pct"))
    revenue_qoq_pct = _coalesce_number(latest_kpis.get("revenue_qoq_pct"), latest.get("revenue_qoq_pct"))
    net_income_bn = _coalesce_number(latest_kpis.get("net_income_bn"), latest.get("net_income_bn"))
    net_income_yoy_pct = _coalesce_number(latest_kpis.get("net_income_yoy_pct"), latest.get("net_income_yoy_pct"))
    net_income_qoq_pct = _coalesce_number(latest_kpis.get("net_income_qoq_pct"), latest.get("net_income_qoq_pct"))
    current_margin = _coalesce_number(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct"), latest.get("net_margin_pct"))
    top_segment = max(
        list(fixture.get("current_segments") or []),
        key=lambda item: float(item.get("value_bn") or 0.0),
        default=None,
    )
    top_geo = max(
        list(fixture.get("current_geographies") or []),
        key=lambda item: float(item.get("value_bn") or 0.0),
        default=None,
    )
    margin_label = _margin_label(latest, latest_kpis, company)

    first_title = "先看收入和利润"
    second_title = "变化来自哪里" if top_segment or top_geo else "这季为什么更重要"
    third_title = "下一步盯什么"

    first_clauses = [
        _metric_clause(
            "收入做到",
            revenue_bn,
            money_symbol,
            yoy_pct=revenue_yoy_pct,
            qoq_pct=revenue_qoq_pct,
            include_qoq=True,
        ),
        _metric_clause(
            "净利润",
            net_income_bn,
            money_symbol,
            yoy_pct=net_income_yoy_pct,
            qoq_pct=net_income_qoq_pct,
            include_qoq=True,
        ),
    ]
    first_body = "；".join(clause for clause in first_clauses if clause) + "。"

    second_body = ""
    if top_segment and revenue_bn not in (None, 0):
        share = float(top_segment.get("value_bn") or 0.0) / float(revenue_bn) * 100
        yoy_text = (
            f"，同比 {format_pct(top_segment.get('yoy_pct'), signed=True)}"
            if top_segment.get("yoy_pct") is not None
            else ""
        )
        second_body = (
            f"如果只盯一个业务，先盯 {top_segment['name']}。它这季贡献了约 {format_pct(share)} 的收入{yoy_text}；"
            f"这比单看总收入更能解释市场为什么重新定价。"
        )
    elif top_geo:
        second_body = f"地区层面最重的暴露仍在 {top_geo['name']}，这决定了公司这季的经营节奏更受哪里拖动。"
    elif current_margin is not None:
        second_body = f"真正重要的不是单看收入，而是 {margin_label} 大致站在 {format_pct(current_margin)} 附近，说明增长有没有顺利转成利润质量。"
    else:
        second_body = "真正重要的是把规模、结构和利润放在一起看，而不是只记住一个收入数字。"

    third_body = ""
    guidance_revenue = _coalesce_number(guidance.get("revenue_bn"))
    if guidance_revenue is not None and revenue_bn not in (None, 0):
        delta = (guidance_revenue / revenue_bn - 1) * 100
        label = str(guidance.get("comparison_label") or ("下一季收入指引" if guidance.get("mode") == "official" else "后续收入参考"))
        third_body = f"管理层给出的 {label} 在 {format_money_bn(guidance_revenue, money_symbol)} 左右，和本季相比 {format_pct(delta, signed=True)}；接下来重点看这个方向会不会继续兑现。"
    elif guidance.get("commentary"):
        third_body = polish_generated_text(str(guidance.get("commentary") or ""))
    elif top_segment:
        third_body = f"后面别只看总收入，先看 {top_segment['name']} 的增速还能不能继续快过公司整体。"
    else:
        third_body = "后面真正要盯的是收入方向、利润率和管理层语气能不能继续站在同一边。"

    return [
        {"title": first_title, "body": _trim_sentence(first_body)},
        {"title": second_title, "body": _trim_sentence(second_body)},
        {"title": third_title, "body": _trim_sentence(third_body)},
    ]


def normalize_takeaways(
    takeaways: list[str],
    latest_kpis: dict[str, Any],
    latest_history: dict[str, Any],
    money_symbol: str,
) -> list[str]:
    del latest_kpis, latest_history, money_symbol
    return dedupe_lines([_trim_sentence(item) for item in list(takeaways or [])], limit=4)


def build_expectation_panel(
    fixture: dict[str, Any],
    history: list[dict[str, Any]],
    money_symbol: str,
) -> dict[str, Any]:
    latest = dict(history[-1] if history else {})
    latest_kpis = dict(fixture.get("latest_kpis") or {})
    guidance = dict(fixture.get("guidance") or {})
    current_revenue = _coalesce_number(latest_kpis.get("revenue_bn"), latest.get("revenue_bn"))
    current_margin = _coalesce_number(latest_kpis.get("gaap_gross_margin_pct"), latest.get("gross_margin_pct"), latest.get("net_margin_pct"))
    guidance_revenue = _coalesce_number(guidance.get("revenue_bn"))
    title = (
        "管理层怎么想下一季"
        if str(guidance.get("mode") or "") == "official"
        else "管理层怎么描绘下一步"
        if str(guidance.get("mode") or "") == "official_context"
        else "下一步先拿什么做参照"
    )
    bullets: list[str] = []
    chips: list[dict[str, str]] = []
    if current_revenue is not None:
        bullets.append(f"先把本季收入 {format_money_bn(current_revenue, money_symbol)} 当成出发点，再去判断下一步是不是在继续上修。")
    if current_margin is not None:
        bullets.append(f"利润这边先看 {format_pct(current_margin)} 左右的位置是否站得住，别只盯收入。")
        chips.append({"label": "利润位置", "value": format_pct(current_margin), "note": "先看利润率能否站稳"})
    if guidance_revenue is not None and current_revenue not in (None, 0):
        delta = (guidance_revenue / current_revenue - 1) * 100
        label = str(guidance.get("comparison_label") or ("下一季收入指引" if guidance.get("mode") == "official" else "后续收入参考"))
        bullets.append(f"{label}在 {format_money_bn(guidance_revenue, money_symbol)} 左右，和本季相比 {format_pct(delta, signed=True)}。")
        chips.append({"label": "后续方向", "value": format_pct(delta, signed=True), "note": label})
    commentary = polish_generated_text(str(guidance.get("commentary") or ""))
    if commentary:
        bullets.append(_trim_sentence(commentary))
    if not chips and current_revenue is not None:
        chips.append({"label": "本季基准", "value": format_money_bn(current_revenue, money_symbol), "note": "先从这个位置往后看"})
    return {"title": title, "bullets": dedupe_lines(bullets, limit=4), "chips": chips[:3]}


def build_call_panel(
    transcript_summary: Optional[dict[str, Any]],
    qna_topics: list[dict[str, Any]],
    narrative_provenance: dict[str, Any],
) -> dict[str, Any]:
    qna_status = str(((narrative_provenance or {}).get("qna") or {}).get("status") or "")
    meta_lines = dedupe_lines([str(item) for item in list((narrative_provenance or {}).get("call_panel_meta_lines") or [])], limit=2)
    fallback_bullets = dedupe_lines(
        [
            str(item.get("note") or "")
            for item in list(qna_topics or [])
            if _line_looks_readable(str(item.get("note") or ""))
        ],
        limit=2,
    )
    if transcript_summary:
        title = "电话会摘录" if transcript_summary.get("source_type") == "manual_transcript" else "电话会要点"
        bullets = dedupe_lines(
            [str(item) for item in list(transcript_summary.get("highlights") or []) if _line_looks_readable(str(item))],
            limit=2,
        )
        if not bullets:
            bullets = fallback_bullets
        return {"title": title, "meta_lines": meta_lines, "bullets": bullets}
    if qna_status in {"manual_transcript", "official_call_material"}:
        title = "电话会里最值得追的两件事"
    elif qna_status == "official_material_proxy":
        title = "电话会缺席时，先看这些问题"
    else:
        title = "管理层接下来最可能被追问什么"
    return {"title": title, "meta_lines": meta_lines, "bullets": fallback_bullets}


def build_institutional_digest(institutional_views: list[dict[str, Any]]) -> dict[str, Any]:
    if not institutional_views:
        return {
            "title": "卖方视角",
            "bullets": [
                "这季公开可追踪的卖方条目还不够多，所以这一页宁可留白，也不硬凑观点。",
                "如果后面新增高质量公开转述，再看市场是不是开始往同一个问题上收敛。",
            ],
        }

    stance_counts = Counter(str(item.get("stance_label") or "参考") for item in institutional_views)
    positive_count = stance_counts.get("偏积极", 0)
    cautious_count = stance_counts.get("偏谨慎", 0)
    neutral_count = stance_counts.get("中性", 0)
    reference_count = stance_counts.get("参考", 0)
    bullets = [
        f"这季我们抓到 {len(institutional_views)} 条公开卖方转述：偏积极 {positive_count} 条，偏谨慎 {cautious_count} 条，中性 {neutral_count} 条，事件跟踪型 {reference_count} 条。"
    ]
    if positive_count >= max(2, len(institutional_views) - 1):
        bullets.append("整体口径偏正面，说明这份财报更多是在强化原有多头逻辑，而不是只靠估值弹性。")
    elif cautious_count >= 2:
        bullets.append("反馈偏谨慎，市场更像是在财报后重新校准后续增速和利润持续性。")
    else:
        bullets.append("分歧还在，机构条目更适合拿来当问题清单，而不是直接当结论。")
    return {"title": "卖方视角", "bullets": dedupe_lines(bullets, limit=3)}


def _provenance_entry(status: str) -> dict[str, Any]:
    mapping = {
        "manual_transcript": {
            "label": "电话会原文",
            "detail": "这一页主要依据手动上传的 transcript。",
            "is_inferred": False,
        },
        "official_call_material": {
            "label": "电话会材料",
            "detail": "这一页直接依据官方电话会材料整理。",
            "is_inferred": False,
        },
        "official_material_proxy": {
            "label": "财报稿、演示材料和 filing",
            "detail": "这里主要把财报稿、演示材料和 filing 交叉起来看，整理管理层最在意的问题。",
            "is_inferred": True,
        },
        "official_material_inferred": {
            "label": "财报稿与 filing",
            "detail": "这里的主题根据当季披露整理，更适合当后续问题清单来读。",
            "is_inferred": True,
        },
        "structured_fallback": {
            "label": "历史财务序列",
            "detail": "原始材料不足时，这里只保留最核心的研究问题，不硬凑细节。",
            "is_inferred": True,
        },
        "official_quote_excerpt": {
            "label": "管理层原话",
            "detail": "卡片直接引用官方材料里的原句。",
            "is_inferred": False,
        },
        "synthesized_quote": {
            "label": "管理层语境",
            "detail": "没有合适原话时，卡片只保留最接近管理层表达的意思。",
            "is_inferred": True,
        },
        "no_quote_excerpt": {
            "label": "证据卡片",
            "detail": "没有合适原话时，改用证据卡片支撑判断。",
            "is_inferred": True,
        },
    }
    return {"status": status, **mapping[status]}


def build_narrative_provenance(
    *,
    transcript_source_type: str,
    has_official_material: bool,
    has_quote_cards: bool,
    synthesized_quotes: bool,
) -> dict[str, Any]:
    if transcript_source_type == "manual_transcript":
        qna = _provenance_entry("manual_transcript")
    elif transcript_source_type == "official_call_material":
        qna = _provenance_entry("official_call_material")
    elif transcript_source_type == "official_material_proxy":
        qna = _provenance_entry("official_material_proxy")
    elif has_official_material:
        qna = _provenance_entry("official_material_inferred")
    else:
        qna = _provenance_entry("structured_fallback")

    if transcript_source_type == "official_call_material":
        narrative_status = "official_call_material"
    elif transcript_source_type == "official_material_proxy":
        narrative_status = "official_material_proxy"
    else:
        narrative_status = "official_material_inferred" if has_official_material else "structured_fallback"
    management = _provenance_entry(narrative_status)
    risks = _provenance_entry(narrative_status)
    catalysts = _provenance_entry(narrative_status)

    if has_quote_cards and not synthesized_quotes:
        quotes = _provenance_entry("official_quote_excerpt")
    elif has_quote_cards:
        quotes = _provenance_entry("synthesized_quote")
    else:
        quotes = _provenance_entry("no_quote_excerpt")

    call_meta_lines = []
    if qna["status"] in {"manual_transcript", "official_call_material"}:
        call_meta_lines.append(f"这页主要依据 {qna['label']}。")
    elif qna["status"] == "official_material_proxy":
        call_meta_lines.append("这页主要把财报稿、演示材料和 filing 交叉起来看。")
    elif qna["status"] == "official_material_inferred":
        call_meta_lines.append("这页基于财报稿和 filing 整理，更适合当问题清单来读。")
    else:
        call_meta_lines.append("原始材料还不够完整，这页先保留最核心的研究问题。")
    call_meta_lines.append(f"锚点部分采用 {quotes['label']}。")

    return {
        "qna": qna,
        "management": management,
        "risks": risks,
        "catalysts": catalysts,
        "quotes": quotes,
        "call_panel_meta_lines": dedupe_lines(call_meta_lines, limit=2),
        "risk_meta_lines": dedupe_lines(
            [
                f"风险判断主要依据 {risks['label']}。",
                f"催化判断主要依据 {catalysts['label']}。",
            ],
            limit=2,
        ),
        "quote_panel_title": "管理层原话" if quotes["status"] == "official_quote_excerpt" else "管理层语境",
        "qna_chart_title": "电话会里最重要的问题" if not qna["is_inferred"] else "市场下一步会追问什么",
        "qna_chart_subtitle": (
            "直接按电话会原文整理"
            if qna["status"] in {"manual_transcript", "official_call_material"}
            else "没有完整 transcript 时，改从官方材料里反推"
        ),
        "management_chart_subtitle": (
            "按管理层原文和财报披露整理"
            if management["status"] == "official_call_material"
            else "按财报稿、演示材料和 filing 归纳"
            if management["status"] == "official_material_proxy"
            else "结合当季披露和经营数据整理"
        ),
    }


def humanize_support_lines(lines: list[str]) -> list[str]:
    rewritten: list[str] = []
    for line in list(lines or []):
        cleaned = polish_generated_text(line)
        if not cleaned:
            continue
        if cleaned.startswith("已应用手动上传 transcript："):
            rewritten.append(cleaned.replace("已应用手动上传 transcript：", "这份报告已接入手动上传的 transcript："))
            continue
        if cleaned.startswith("已自动提取官方电话会材料："):
            rewritten.append(cleaned.replace("已自动提取官方电话会材料：", "这次补入了官方电话会材料："))
            continue
        if cleaned.startswith("已基于官方财报材料构建电话会代理摘要："):
            rewritten.append(cleaned.replace("已基于官方财报材料构建电话会代理摘要：", "电话会页主要参考财报稿、演示材料和 filing："))
            continue
        if re.search(r"本次已自动抓取\s*(\d+)\s*份官方材料", cleaned):
            count = re.search(r"本次已自动抓取\s*(\d+)\s*份官方材料", cleaned)
            rewritten.append(f"这次拿到了 {count.group(1)} 份官方材料，正文判断都基于这些原文展开。")
            continue
        if re.search(r"本次已复用\s*(\d+)\s*份官方材料缓存", cleaned):
            count = re.search(r"本次已复用\s*(\d+)\s*份官方材料缓存", cleaned)
            rewritten.append(f"这次沿用了 {count.group(1)} 份已缓存的官方材料，核心判断仍然能回到原文。")
            continue
        if "本次新抓取" in cleaned and "复用缓存" in cleaned and "官方材料" in cleaned:
            rewritten.append("这次一部分材料新抓取，一部分沿用缓存，最终判断都能回到官方原文。")
            continue
        if "当前环境关闭了官方源材料自动抓取" in cleaned:
            rewritten.append("这次没有额外联网抓取，主要依据现有缓存和内置数据整理。")
            continue
        if "优先依据官方电话会材料整理" in cleaned:
            rewritten.append("问答主题主要按官方电话会材料整理。")
            continue
        if "问答主题基于官方财报材料动态推断" in cleaned:
            rewritten.append("这页的问题清单主要根据财报稿和 filing 整理。")
            continue
        if "统一研究 fallback" in cleaned or "统一研究模板" in cleaned:
            rewritten.append("原始材料还不够完整，这页只保留最核心的问题。")
            continue
        if "公开可追踪的卖方条目" in cleaned or "未稳定抓到可追溯的机构观点条目" in cleaned:
            rewritten.append("公开可追踪的卖方条目还不够，这一页宁可少说，也不硬凑观点。")
            continue
        if "电话会代理摘要" in cleaned or "代理电话会摘要" in cleaned:
            rewritten.append("电话会页重点来自财报稿、演示材料和 filing 的交叉整理。")
            continue
        if "地区结构" in cleaned and "切换" in cleaned:
            rewritten.append("因为分部披露不连续，这一页改从地区结构去看迁移。")
            continue
        if "优先根据官方原文动态解析" in cleaned or "优先解析自动发现的官方" in cleaned:
            rewritten.append("这份报告优先按当季官方原文动态解析，未披露字段才回到历史口径。")
            continue
        if "动态补入地区营收结构" in cleaned or "优先采用官方披露原文中的地理口径" in cleaned:
            rewritten.append("地区拆分这次直接按官方披露原文整理，没有再沿用旧口径。")
            continue
        if "地区结构已改为优先解析官方 10-Q" in cleaned:
            rewritten.append("地区结构这次直接回到 10-Q 里的地区拆分表来做。")
            continue
        rewritten.append(cleaned)
    return dedupe_lines(rewritten, limit=5)


def narrative_template_markers() -> tuple[str, ...]:
    return TEMPLATE_MARKERS
