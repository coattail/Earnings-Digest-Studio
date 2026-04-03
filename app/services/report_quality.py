from __future__ import annotations

import re
from typing import Any, Optional

from .narrative_writer import narrative_template_markers


FALLBACK_MARKERS = (
    "fallback",
    "回退",
    "降级",
    "回溯",
    "限制说明",
    "内置口径",
    "没有完整电话会",
    "原始材料不足",
    "近几季常态",
    "替代阅读",
)
FULL_COVERAGE_CORE_MARKERS = (
    "结构",
    "分部",
    "地区",
    "geograph",
    "segment",
    "kpi",
    "收入",
    "净利润",
    "管理层",
    "问答",
    "电话会",
    "guidance",
    "指引",
)
FALLBACK_WARNING_EXEMPT_SNIPPETS = (
    "未披露字段才回退到内置口径",
    "若部分字段仍未披露，再回退到结构化季度财务序列与统一研究模板",
)
NARRATIVE_TEMPLATE_MARKERS = narrative_template_markers()
NARRATIVE_PROVENANCE_MARKERS = (
    "来源：",
    "依据：",
    "核对：",
    "这页主要依据",
    "原始材料",
    "财报稿",
    "filling",
    "filing",
    "transcript",
)


def _topic_note_looks_placeholder(note: Any) -> bool:
    cleaned = re.sub(r"\s+", " ", str(note or "")).strip()
    if not cleaned:
        return True
    if any(marker in cleaned for marker in ("关键词聚类", "来自自动抓取的官方电话会材料", "来自官方补充材料", "来自手动上传 transcript")):
        return True
    if "..." in cleaned or "…" in cleaned:
        return True
    if re.search(r"(?:同比|环比)\s*[-–—]", cleaned):
        return True
    if cleaned.endswith(("...", "…", "-", "—", "–", ":", "：", ",", "，")):
        return True
    readable_terms = re.findall(r"[A-Za-z\u4e00-\u9fff]{2,}", cleaned)
    return len(readable_terms) < 3


def _placeholder_topic_count(items: list[dict[str, Any]]) -> int:
    return sum(1 for item in items if _topic_note_looks_placeholder(item.get("note")))


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _non_empty_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            normalized.append(item)
    return normalized


def _coverage_ratio(items: list[dict[str, Any]], key: str) -> float:
    if not items:
        return 0.0
    covered = sum(1 for item in items if _non_empty_items(item.get(key)))
    return covered / len(items)


def _structure_ratio(items: list[dict[str, Any]], revenue_bn: Optional[float]) -> Optional[float]:
    if not items or revenue_bn in (None, 0):
        return None
    total = sum(max(_safe_float(item.get("value_bn")) or 0.0, 0.0) for item in items)
    if total <= 0:
        return None
    return total / float(revenue_bn)


def _issue(severity: str, code: str, message: str) -> dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
    }


def _has_any_marker(text: str, markers: tuple[str, ...]) -> bool:
    lowered = str(text or "").casefold()
    return any(marker.casefold() in lowered for marker in markers)


def _collect_narrative_lines(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    lines.append(str(payload.get("headline") or ""))
    lines.extend(str(item) for item in list(payload.get("takeaways") or []))
    for item in _non_empty_items(payload.get("layered_takeaways")):
        lines.append(str(item.get("title") or ""))
        lines.append(str(item.get("body") or ""))
    call_panel = dict(payload.get("call_panel") or {})
    lines.append(str(call_panel.get("title") or ""))
    lines.extend(str(item) for item in list(call_panel.get("meta_lines") or []))
    lines.extend(str(item) for item in list(call_panel.get("bullets") or []))
    institutional_digest = dict(payload.get("institutional_digest") or {})
    lines.append(str(institutional_digest.get("title") or ""))
    lines.extend(str(item) for item in list(institutional_digest.get("bullets") or []))
    for item in _non_empty_items(payload.get("management_themes")):
        lines.append(str(item.get("label") or ""))
        lines.append(str(item.get("note") or ""))
    for item in _non_empty_items(payload.get("qna_themes")):
        lines.append(str(item.get("label") or ""))
        lines.append(str(item.get("note") or ""))
    return [re.sub(r"\s+", " ", line).strip() for line in lines if str(line or "").strip()]


def _template_phrase_hits(lines: list[str]) -> int:
    hits = 0
    for line in lines:
        if _has_any_marker(line, NARRATIVE_TEMPLATE_MARKERS):
            hits += 1
    return hits


def _repeated_ngram_hits(lines: list[str], *, n: int = 6) -> int:
    counts: dict[str, set[int]] = {}
    for index, line in enumerate(lines):
        compact = re.sub(r"[^\w\u4e00-\u9fff]+", "", line.casefold())
        if len(compact) < n * 2:
            continue
        grams = {compact[offset : offset + n] for offset in range(len(compact) - n + 1)}
        for gram in grams:
            counts.setdefault(gram, set()).add(index)
    return sum(1 for seen_in_lines in counts.values() if len(seen_in_lines) >= 3)


def _provenance_heavy_ratio(lines: list[str]) -> float:
    if not lines:
        return 0.0
    provenance_lines = sum(1 for line in lines if _has_any_marker(line, NARRATIVE_PROVENANCE_MARKERS))
    return provenance_lines / len(lines)


def evaluate_report_payload(
    payload: dict[str, Any],
    *,
    history_window: Optional[int] = None,
    require_full_coverage: bool = False,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    latest_kpis = dict(normalized.get("latest_kpis") or {})
    history = _non_empty_items(normalized.get("historical_cube"))
    segments = _non_empty_items(normalized.get("current_segments"))
    geographies = _non_empty_items(normalized.get("current_geographies"))
    qna_themes = _non_empty_items(normalized.get("qna_themes"))
    management_themes = _non_empty_items(normalized.get("management_themes"))
    evidence_cards = _non_empty_items(normalized.get("evidence_cards"))
    call_quote_cards = _non_empty_items(normalized.get("call_quote_cards"))
    source_materials = _non_empty_items(normalized.get("source_materials"))
    coverage_warnings = [str(item).strip() for item in list(normalized.get("coverage_warnings") or []) if str(item).strip()]
    structure_dimension = str(normalized.get("structure_dimension_used") or "management")

    issues: list[dict[str, str]] = []
    revenue_bn = _safe_float(latest_kpis.get("revenue_bn"))
    net_income_bn = _safe_float(latest_kpis.get("net_income_bn"))
    gross_margin_pct = _safe_float(latest_kpis.get("gaap_gross_margin_pct"))
    net_margin_pct = _safe_float((history[-1] if history else {}).get("net_margin_pct"))

    if revenue_bn is None:
        issues.append(_issue("critical", "latest_kpi_missing_revenue", "最新季度缺少 revenue_bn，报告不满足可交付标准。"))
    if net_income_bn is None:
        issues.append(_issue("critical", "latest_kpi_missing_net_income", "最新季度缺少 net_income_bn，利润质量页无法可靠生成。"))
    if gross_margin_pct is None and net_margin_pct is None:
        issues.append(_issue("major", "latest_margin_missing", "毛利率和净利率均缺失，盈利质量判断会明显降级。"))

    structure_severity_default = "major"
    if structure_dimension == "management":
        structure_severity_default = "minor"
    if not segments and not geographies:
        issues.append(_issue(structure_severity_default, "current_structure_missing", "当前季度缺少业务和地区结构拆分。"))
    if structure_dimension == "segment" and not segments:
        issues.append(_issue("major", "segment_mode_without_segments", "报告声明使用分部结构，但当前季度未给出可用分部数据。"))
    if structure_dimension == "geography" and not geographies:
        issues.append(_issue("major", "geography_mode_without_geographies", "报告声明使用地区结构，但当前季度未给出可用地区数据。"))

    segment_ratio = _structure_ratio(segments, revenue_bn)
    geo_ratio = _structure_ratio(geographies, revenue_bn)
    if segment_ratio is not None and not (0.35 <= segment_ratio <= 1.65):
        issues.append(_issue("major", "segment_ratio_outlier", "当季分部收入合计与总收入偏差较大，建议核对分部口径。"))
    if geo_ratio is not None and not (0.35 <= geo_ratio <= 1.65):
        issues.append(_issue("major", "geography_ratio_outlier", "当季地区收入合计与总收入偏差较大，建议核对地区口径。"))

    if not history:
        issues.append(_issue("critical", "history_missing", "缺少历史季度序列，12 季模块无法使用。"))
    else:
        expected_window = int(history_window or len(history))
        if expected_window >= 4 and len(history) < expected_window:
            issues.append(_issue("major", "history_window_short", f"历史窗口仅 {len(history)} 季，低于目标 {expected_window} 季。"))
        missing_revenue_count = sum(1 for item in history if _safe_float(item.get("revenue_bn")) is None)
        missing_net_income_count = sum(1 for item in history if _safe_float(item.get("net_income_bn")) is None)
        if missing_revenue_count > max(1, len(history) // 5):
            severity = "critical" if missing_revenue_count > max(2, len(history) // 3) else "major"
            issues.append(_issue(severity, "history_revenue_missing", f"历史序列中有 {missing_revenue_count} 季缺少收入。"))
        if missing_net_income_count > max(1, len(history) // 5):
            severity = "critical" if missing_net_income_count > max(2, len(history) // 3) else "major"
            issues.append(_issue(severity, "history_net_income_missing", f"历史序列中有 {missing_net_income_count} 季缺少净利润。"))

        segment_coverage = _coverage_ratio(history, "segments")
        geo_coverage = _coverage_ratio(history, "geographies")
        if max(segment_coverage, geo_coverage) <= 0:
            issues.append(
                _issue(
                    "minor" if structure_dimension == "management" else "major",
                    "history_structure_missing",
                    "历史季度缺少结构数据，迁移与增量分析不可用。",
                )
            )
        elif max(segment_coverage, geo_coverage) < 0.35:
            issues.append(
                _issue(
                    "minor" if structure_dimension == "management" else "major",
                    "history_structure_sparse",
                    "历史结构覆盖率过低，结构迁移结论可靠性有限。",
                )
            )
        if structure_dimension == "segment" and segment_coverage < 0.6:
            issues.append(_issue("major", "segment_history_sparse", "分部模式下历史分部覆盖率不足 60%。"))
        if structure_dimension == "geography" and geo_coverage < 0.6:
            issues.append(_issue("major", "geography_history_sparse", "地区模式下历史地区覆盖率不足 60%。"))

    if len(qna_themes) == 0:
        issues.append(_issue("major", "qna_missing", "问答主题缺失，电话会页缺少核心内容。"))
    elif len(qna_themes) < 2:
        issues.append(_issue("major", "qna_sparse", "问答主题过少，电话会页信息密度不足。"))
    if len(management_themes) < 2:
        issues.append(_issue("minor", "management_theme_sparse", "管理层主题数量偏少。"))
    qna_placeholder_count = _placeholder_topic_count(qna_themes)
    management_placeholder_count = _placeholder_topic_count(management_themes)
    if qna_placeholder_count > 0:
        issues.append(
            _issue(
                "major" if qna_placeholder_count >= min(2, len(qna_themes)) else "minor",
                "qna_placeholder_topics",
                f"问答主题里仍有 {qna_placeholder_count} 条占位/模板化摘要，电话会页可读性不足。",
            )
        )
    if management_placeholder_count > 0:
        issues.append(
            _issue(
                "major" if management_placeholder_count >= 2 else "minor",
                "management_placeholder_topics",
                f"管理层主题里仍有 {management_placeholder_count} 条占位/模板化摘要，执行层信号不够完整。",
            )
        )
    if len(evidence_cards) < 2:
        issues.append(_issue("major", "evidence_sparse", "证据卡片不足，回溯原文的可验证性下降。"))

    extracted_materials = [
        item
        for item in source_materials
        if str(item.get("status") or "") in {"fetched", "cached"} and int(item.get("text_length") or 0) > 0
    ]
    failed_materials = [item for item in source_materials if str(item.get("status") or "") == "error"]
    disabled_materials = [item for item in source_materials if str(item.get("status") or "") == "disabled"]
    if source_materials and not extracted_materials and not disabled_materials:
        issues.append(_issue("critical", "source_material_not_extracted", "官方源材料已定位但未提取出可解析文本。"))
    if source_materials and failed_materials and len(failed_materials) >= max(2, len(source_materials) // 2):
        issues.append(_issue("major", "source_material_fail_ratio_high", "官方材料抓取失败比例较高，建议重跑并检查源站解析策略。"))

    narrative_lines = _collect_narrative_lines(normalized)
    template_phrase_hits = _template_phrase_hits(narrative_lines)
    repeated_ngram_hits = _repeated_ngram_hits(narrative_lines)
    provenance_heavy_ratio = _provenance_heavy_ratio(narrative_lines)
    if template_phrase_hits >= 3:
        issues.append(_issue("major", "narrative_template_language", "报告文案里仍有较明显的模板化或系统提示式表达。"))
    elif template_phrase_hits >= 1:
        issues.append(_issue("minor", "narrative_template_language", "报告文案里还有少量模板化表达。"))
    if repeated_ngram_hits >= 18:
        issues.append(_issue("major", "narrative_repetition_high", "报告文案重复度偏高，读起来仍像自动拼接。"))
    elif repeated_ngram_hits >= 10:
        issues.append(_issue("minor", "narrative_repetition_high", "报告文案存在可感知的重复句型。"))
    if provenance_heavy_ratio >= 0.33:
        issues.append(_issue("minor", "narrative_provenance_too_dense", "正文里来源/方法说明占比偏高，影响阅读流畅度。"))

    if require_full_coverage:
        if structure_dimension == "management":
            issues.append(_issue("critical", "full_coverage_structure_mode_management", "Full coverage 模式不允许管理层降级结构，必须具备可用分部或地区结构。"))
        has_segments = len(segments) >= 2
        has_geographies = len(geographies) >= 2
        has_geo_proxy_segments = any(str(item.get("scope") or "").casefold() == "geo_proxy" for item in segments)
        if not has_segments:
            issues.append(
                _issue(
                    "critical" if not has_geographies else "major",
                    "full_coverage_segment_missing",
                    "Full coverage 模式要求结构页具备业务分部深度；当前季度分部不足 2 项。",
                )
            )
        if not has_geographies:
            issues.append(
                _issue(
                    "critical" if not has_segments else "major",
                    "full_coverage_geography_missing",
                    "Full coverage 模式要求结构页具备地区维度深度；当前季度地区不足 2 项。",
                )
            )

        non_quarterly_geographies = [
            item
            for item in geographies
            if str(item.get("scope") or "").casefold() in {"annual_filing"}
        ]
        if non_quarterly_geographies:
            issues.append(
                _issue(
                    "critical",
                    "full_coverage_geography_non_quarterly",
                    "当前地区结构仍包含年报口径，未达到季度级完整披露标准。",
                )
            )

        segment_history_coverage = _coverage_ratio(history, "segments")
        geography_history_coverage = _coverage_ratio(history, "geographies")
        if max(segment_history_coverage, geography_history_coverage) < 0.95:
            issues.append(_issue("critical", "full_coverage_structure_history_incomplete", "历史结构覆盖率不足（分部和地区均低于 95%）。"))
        if segment_history_coverage < 0.95:
            if not (geography_history_coverage >= 0.95 and has_geo_proxy_segments):
                issues.append(
                    _issue(
                        "major" if geography_history_coverage >= 0.95 else "critical",
                        "full_coverage_segment_history_incomplete",
                        "历史分部覆盖率低于 95%，分部结构连续性不足。",
                    )
                )
        if geography_history_coverage < 0.95:
            issues.append(
                _issue(
                    "major" if segment_history_coverage >= 0.95 else "critical",
                    "full_coverage_geography_history_incomplete",
                    "历史地区覆盖率低于 95%，地区结构连续性不足。",
                )
            )

        if len(qna_themes) < 3:
            issues.append(_issue("critical", "full_coverage_qna_sparse", "Full coverage 模式要求至少 3 条电话会/问答主题。"))
        if len(management_themes) < 3:
            issues.append(_issue("critical", "full_coverage_management_sparse", "Full coverage 模式要求至少 3 条管理层主题。"))
        if qna_placeholder_count > 0:
            issues.append(_issue("critical", "full_coverage_qna_placeholder_topics", "Full coverage 模式不允许问答主题保留占位/模板化摘要。"))
        if management_placeholder_count > 0:
            issues.append(_issue("critical", "full_coverage_management_placeholder_topics", "Full coverage 模式不允许管理层主题保留占位/模板化摘要。"))
        if len(evidence_cards) < 3:
            issues.append(_issue("critical", "full_coverage_evidence_sparse", "Full coverage 模式要求至少 3 张证据卡片。"))

        extracted_roles = {
            str(item.get("role") or item.get("kind") or "").casefold()
            for item in extracted_materials
        }
        has_structured_financials = "structured_financials" in extracted_roles
        has_release_surrogate = any(role in extracted_roles for role in {"sec_filing", "investor_relations"})
        has_dense_context = len(qna_themes) >= 3 and len(management_themes) >= 3 and len(evidence_cards) >= 3
        for role in ("earnings_release", "sec_filing"):
            if role == "sec_filing" and has_structured_financials:
                continue
            if role not in extracted_roles:
                if role == "earnings_release" and has_release_surrogate and has_dense_context:
                    continue
                severity = "critical"
                issues.append(_issue(severity, f"full_coverage_source_role_missing_{role}", f"缺少可解析的官方材料角色：{role}。"))
        has_call_source = any(role in extracted_roles for role in ("earnings_commentary", "earnings_call", "earnings_presentation"))
        if not has_call_source and (len(qna_themes) < 3 and len(call_quote_cards) < 2):
            issues.append(
                _issue(
                    "critical",
                    "full_coverage_source_role_missing_call_context",
                    "缺少可解析的电话会/评论材料，且问答与引语密度不足以支撑电话会详情页。",
                )
            )

        fallback_lines = [
            line
            for line in coverage_warnings
            if _has_any_marker(line, FALLBACK_MARKERS)
            and _has_any_marker(line, FULL_COVERAGE_CORE_MARKERS)
            and not _has_any_marker(line, FALLBACK_WARNING_EXEMPT_SNIPPETS)
        ]
        if fallback_lines:
            issues.append(
                _issue(
                    "critical",
                    "full_coverage_fallback_detected",
                    f"检测到回退/降级痕迹：{fallback_lines[0]}",
                )
            )

    severity_weight = {"critical": 25, "major": 10, "minor": 4}
    deduction = sum(severity_weight.get(item["severity"], 0) for item in issues)
    score = max(0, 100 - deduction)
    counts = {
        "critical": sum(1 for item in issues if item["severity"] == "critical"),
        "major": sum(1 for item in issues if item["severity"] == "major"),
        "minor": sum(1 for item in issues if item["severity"] == "minor"),
    }
    if counts["critical"] > 0:
        status = "fail"
    elif counts["major"] >= 3 or score < 82:
        status = "review"
    else:
        status = "pass"

    metrics = {
        "history_quarter_count": len(history),
        "current_segment_count": len(segments),
        "current_geography_count": len(geographies),
        "qna_theme_count": len(qna_themes),
        "evidence_card_count": len(evidence_cards),
        "source_material_count": len(source_materials),
        "source_extracted_count": len(extracted_materials),
        "source_error_count": len(failed_materials),
        "segment_to_revenue_ratio": segment_ratio,
        "geography_to_revenue_ratio": geo_ratio,
        "segment_history_coverage": _coverage_ratio(history, "segments"),
        "geography_history_coverage": _coverage_ratio(history, "geographies"),
        "narrative_template_phrase_hits": template_phrase_hits,
        "narrative_repeated_ngram_hits": repeated_ngram_hits,
        "narrative_provenance_heavy_ratio": provenance_heavy_ratio,
        "qna_placeholder_topic_count": qna_placeholder_count,
        "management_placeholder_topic_count": management_placeholder_count,
    }

    return {
        "status": status,
        "score": score,
        "counts": counts,
        "issues": issues,
        "metrics": metrics,
        "summary": f"{status.upper()} | score {score} | critical {counts['critical']} | major {counts['major']} | minor {counts['minor']}",
    }


def quality_warnings_for_payload(quality_report: dict[str, Any], max_issue_lines: int = 2) -> list[str]:
    status = str((quality_report or {}).get("status") or "pass")
    if status == "pass":
        return []
    score = int((quality_report or {}).get("score") or 0)
    counts = dict((quality_report or {}).get("counts") or {})
    issues = _non_empty_items((quality_report or {}).get("issues"))
    warnings = [
        (
            "自动质量门禁提示："
            f"{status.upper()}（score={score}，critical={int(counts.get('critical') or 0)}，major={int(counts.get('major') or 0)}）。"
        )
    ]
    for issue in issues[: max(0, int(max_issue_lines))]:
        message = str(issue.get("message") or "").strip()
        if not message:
            continue
        warnings.append(f"质量问题：{message}")
    return warnings
