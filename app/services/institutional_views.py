from __future__ import annotations

import html
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional
from xml.etree import ElementTree

import httpx

from ..config import INSTITUTIONAL_VIEWS_DIR, ensure_directories

RSS_ENDPOINT = "https://news.google.com/rss/search"
RSS_HEADERS = {
    "user-agent": "EarningsDigestStudio/0.1 (+local-user)",
    "accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
}

INSTITUTION_ALIASES: dict[str, tuple[str, ...]] = {
    "Goldman Sachs": ("goldman sachs", "goldman"),
    "Morgan Stanley": ("morgan stanley",),
    "JPMorgan": ("jpmorgan", "jp morgan", "jpm"),
    "UBS": ("ubs",),
    "Bank of America": ("bank of america", "bofa", "bofa securities"),
    "Citi": ("citi", "citigroup"),
    "Bernstein": ("bernstein", "sanford c. bernstein"),
    "Barclays": ("barclays",),
    "Evercore ISI": ("evercore", "evercore isi"),
    "Jefferies": ("jefferies",),
    "Wells Fargo": ("wells fargo",),
    "Deutsche Bank": ("deutsche bank",),
    "Mizuho": ("mizuho",),
    "RBC Capital": ("rbc", "royal bank of canada"),
    "Raymond James": ("raymond james",),
    "Oppenheimer": ("oppenheimer",),
    "Wedbush": ("wedbush",),
    "Needham": ("needham",),
    "Piper Sandler": ("piper sandler",),
    "Baird": ("baird",),
    "HSBC": ("hsbc",),
    "BMO Capital": ("bmo", "bmo capital"),
    "KeyBanc": ("keybanc",),
    "Wolfe Research": ("wolfe", "wolfe research"),
    "Cowen": ("cowen",),
    "Stifel": ("stifel",),
    "Susquehanna": ("susquehanna",),
    "Canaccord": ("canaccord", "canaccord genuity"),
    "Credit Suisse": ("credit suisse",),
    "Cantor": ("cantor", "cantor fitzgerald"),
}

STANCE_PATTERNS: list[tuple[re.Pattern[str], tuple[str, str]]] = [
    (
        re.compile(
            r"(\bupgrade[sd]?\b|\boutperform\b|\boverweight\b|\bbuy\b|\bbullish\b|\b(raise[sd]?|lifts?)\b.{0,30}\btarget\b|\bprice target (raised|lifted)\b)",
            re.I,
        ),
        ("positive", "偏积极"),
    ),
    (
        re.compile(
            r"(\bdowngrade[sd]?\b|\bunderperform\b|\bunderweight\b|\bsell\b|\bbearish\b|\b(cuts?|lowers?)\b.{0,30}\btarget\b|\bprice target lowered\b)",
            re.I,
        ),
        ("negative", "偏谨慎"),
    ),
    (re.compile(r"\b(neutral|equal weight|hold|market perform)\b", re.I), ("neutral", "中性")),
]
InstitutionalProgressCallback = Callable[[float, str], None]


def _cache_path(company_id: str, calendar_quarter: str) -> Path:
    ensure_directories()
    return INSTITUTIONAL_VIEWS_DIR / f"{company_id}-{calendar_quarter}.json"


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _quarter_anchor_date(calendar_quarter: str) -> date:
    year = int(calendar_quarter[:4])
    quarter = int(calendar_quarter[-1])
    month = quarter * 3
    if month == 12:
        return date(year + 1, 1, 31)
    return date(year, month + 1, 28)


def _fetch_window(calendar_quarter: str, release_date: Optional[str]) -> tuple[date, date]:
    anchor = _parse_iso_date(release_date) or _quarter_anchor_date(calendar_quarter)
    return (anchor - timedelta(days=70), anchor + timedelta(days=100))


def _google_rss_queries(company: dict[str, Any], start_date: date, end_date: date) -> list[str]:
    english_name = str(company.get("english_name") or company.get("name") or "")
    ticker = str(company.get("ticker") or "")
    date_filter = f"after:{start_date.isoformat()} before:{end_date.isoformat()}"
    return [
        f'"{english_name}" {ticker} analyst rating price target {date_filter}',
        f'"{english_name}" {ticker} brokerage note target {date_filter}',
        f'"{english_name}" {ticker} analyst {date_filter}',
        f'"{english_name}" {ticker} Morgan Stanley Goldman Sachs JPMorgan UBS Barclays {date_filter}',
    ]


def _strip_html(text: Optional[str]) -> str:
    raw = html.unescape(str(text or ""))
    return re.sub(r"<[^>]+>", " ", raw).replace("\xa0", " ").strip()


def _detect_institution(text: str) -> Optional[str]:
    lowered = text.lower()
    for name, aliases in INSTITUTION_ALIASES.items():
        if any(alias in lowered for alias in aliases):
            return name
    return None


def _detect_stance(text: str) -> tuple[str, str]:
    for pattern, stance in STANCE_PATTERNS:
        if pattern.search(text):
            return stance
    return ("reference", "参考")


def _extract_price_target(text: str) -> Optional[str]:
    match = re.search(r"\$([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return None
    return f"${match.group(1)}"


def _compact_headline(title: str) -> str:
    compact = re.sub(r"\s+", " ", title).strip()
    return re.sub(r"\s*-\s*[^-]+$", "", compact)


def _rating_tokens(text: str) -> list[str]:
    labels = [
        ("outperform", "Outperform"),
        ("overweight", "Overweight"),
        ("buy", "Buy"),
        ("neutral", "Neutral"),
        ("equal weight", "Equal Weight"),
        ("hold", "Hold"),
        ("underweight", "Underweight"),
        ("underperform", "Underperform"),
        ("sell", "Sell"),
    ]
    lowered = text.lower()
    return [label for needle, label in labels if needle in lowered]


def _view_action_point(firm: str, combined_text: str, stance_label: str) -> str:
    lowered = combined_text.lower()
    price_target = _extract_price_target(combined_text)
    ratings = _rating_tokens(combined_text)
    rating_text = " / ".join(ratings[:2]) if ratings else ""
    if "upgrade" in lowered:
        return f"{firm} 给出上调评级动作{f'，当前口径偏向 {rating_text}' if rating_text else ''}。"
    if "downgrade" in lowered:
        return f"{firm} 给出下调评级动作{f'，当前口径偏向 {rating_text}' if rating_text else ''}。"
    if price_target and any(token in lowered for token in ("lower", "cut", "cuts", "cuts", "lowers")) and "target" in lowered:
        return f"{firm} 下调目标价至 {price_target}{f'，但仍维持 {rating_text}' if rating_text else ''}。"
    if price_target and any(token in lowered for token in ("raise", "raised", "lift", "lifted")) and "target" in lowered:
        return f"{firm} 上调目标价至 {price_target}{f'，观点仍偏向 {rating_text}' if rating_text else ''}。"
    if price_target and "target" in lowered:
        return f"{firm} 围绕目标价 {price_target} 重新校准财报前后预期。"
    if any(token in lowered for token in ("maintain", "maintains", "keep", "keeps", "reiterate", "reiterates")) and rating_text:
        return f"{firm} 维持 {rating_text} 观点，说明核心判断没有明显转向。"
    if "pessimistic forecast" in lowered or stance_label == "偏谨慎":
        return f"{firm} 给出的财报后判断偏谨慎，更关注预期下修风险。"
    if stance_label == "偏积极":
        return f"{firm} 的财报后判断偏积极，更看重上修空间与目标价弹性。"
    return f"{firm} 当前把这家公司列为重点跟踪标的，观点倾向为“{stance_label}”。"


def _view_focus_point(company: dict[str, Any], title: str, description: str) -> str:
    combined = f"{title} {description}".lower()
    english_name = str(company.get("english_name") or "")
    if any(token in combined for token in ("azure", "cloud", "aws", "copilot")):
        return "关注点集中在云业务增速、AI 需求兑现和相关产品货币化节奏。"
    if any(token in combined for token in ("margin", "profit", "earnings", "cash flow")):
        return "更看重利润率、盈利兑现和现金流质量是否能持续强化。"
    if any(token in combined for token in ("guidance", "outlook", "forecast")):
        return "核心在于财报后的指引强弱，以及管理层给出的后续经营边界。"
    if any(token in combined for token in ("price target", "target")):
        return "这类观点更像估值框架调整，重点是目标价和预期中枢的变化。"
    compact = _compact_headline(description or title)
    if english_name:
        compact = compact.replace(english_name, "").strip(" -:")
    compact = re.sub(r"\s+", " ", compact).strip(" -:")
    if compact:
        return f"媒体转述的核心表述是：{compact[:88]}。"
    return "媒体条目没有给出更多细节，因此这里只保留机构动作和公开转述范围。"


def _view_points(company: dict[str, Any], firm: str, title: str, description: str, stance_label: str) -> list[str]:
    combined_text = " ".join(part for part in (title, description) if part)
    return [
        _view_action_point(firm, combined_text, stance_label),
        _view_focus_point(company, title, description),
    ]


def _item_summary(company: dict[str, Any], firm: str, title: str, description: str, stance_label: str) -> str:
    points = _view_points(company, firm, title, description, stance_label)
    return " ".join(points)


def _hydrate_cached_item(company: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    title = str(item.get("headline") or item.get("title") or "")
    description = str(item.get("description") or "")
    firm = str(item.get("firm") or "机构")
    stance_label = str(item.get("stance_label") or "参考")
    enriched = dict(item)
    enriched["headline"] = title
    enriched["description"] = description
    enriched["view_points"] = list(item.get("view_points") or _view_points(company, firm, title, description, stance_label))
    enriched["summary"] = str(item.get("summary") or _item_summary(company, firm, title, description, stance_label))
    return enriched


def _parse_rss_items(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []
    items: list[dict[str, Any]] = []
    for item in root.findall("./channel/item"):
        title = _strip_html(item.findtext("title"))
        description = _strip_html(item.findtext("description"))
        link = _strip_html(item.findtext("link"))
        published = _strip_html(item.findtext("pubDate"))
        if not title or not link:
            continue
        items.append(
            {
                "title": title,
                "description": description,
                "link": link,
                "published": published,
            }
        )
    return items


def _fetch_rss(query: str) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    with httpx.Client(headers=RSS_HEADERS, follow_redirects=True, timeout=8.0) as client:
        response = client.get(RSS_ENDPOINT, params=params)
        response.raise_for_status()
    return _parse_rss_items(response.text)


def _normalize_item(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    combined_text = " ".join(str(item.get(key) or "") for key in ("title", "description"))
    firm = _detect_institution(combined_text)
    if not firm:
        return None
    stance, stance_label = _detect_stance(combined_text)
    title = str(item.get("title") or "")
    source = title.rsplit(" - ", 1)[-1] if " - " in title else "News relay"
    published = str(item.get("published") or "")
    return {
        "firm": firm,
        "stance": stance,
        "stance_label": stance_label,
        "headline": title,
        "description": str(item.get("description") or ""),
        "source": source,
        "published_at": _parse_iso_date(published).isoformat() if _parse_iso_date(published) else published[:16],
        "link": str(item.get("link") or ""),
    }


def _read_cache(path: Path, company: dict[str, Any]) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return [_hydrate_cached_item(company, item) for item in list(payload.get("items") or [])]


def _write_cache(path: Path, items: list[dict[str, Any]]) -> None:
    payload = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "items": items,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_institutional_views(
    company: dict[str, Any],
    calendar_quarter: str,
    release_date: Optional[str],
    refresh: bool = False,
    progress_callback: Optional[InstitutionalProgressCallback] = None,
) -> list[dict[str, Any]]:
    def notify(progress: float, message: str) -> None:
        if progress_callback is None:
            return
        progress_callback(max(0.0, min(1.0, float(progress))), message)

    path = _cache_path(str(company["id"]), calendar_quarter)
    if not refresh:
        cached = _read_cache(path, company)
        if cached:
            notify(1.0, "已复用机构观点缓存。")
            return cached
    if os.environ.get("EARNINGS_DIGEST_DISABLE_SOURCE_FETCH") == "1":
        notify(1.0, "机构观点在线抓取已关闭。")
        return _read_cache(path, company)

    start_date, end_date = _fetch_window(calendar_quarter, release_date)
    normalized: list[dict[str, Any]] = []
    seen_links: set[str] = set()
    seen_firms: set[str] = set()
    queries = _google_rss_queries(company, start_date, end_date)
    total_queries = max(len(queries), 1)
    for query_index, query in enumerate(queries, start=1):
        query_start = (query_index - 1) / total_queries
        query_end = query_index / total_queries
        notify(query_start * 0.7, f"正在准备搜索机构观点来源 {query_index}/{total_queries}...")
        try:
            notify(min(0.68, (query_start + (query_end - query_start) * 0.45) * 0.7), f"正在请求 RSS 源 {query_index}/{total_queries}...")
            items = _fetch_rss(query)
        except Exception:
            notify(min(0.7, query_end * 0.7), f"第 {query_index}/{total_queries} 个机构观点源响应失败，继续下一个...")
            continue
        for item in items:
            normalized_item = _normalize_item(item)
            if normalized_item is None:
                continue
            normalized_item = _hydrate_cached_item(company, normalized_item)
            link = str(normalized_item["link"])
            firm = str(normalized_item["firm"])
            if not link or link in seen_links or firm in seen_firms:
                continue
            normalized.append(normalized_item)
            seen_links.add(link)
            seen_firms.add(firm)
            notify(
                min(0.9, 0.7 + len(normalized) * 0.08),
                f"已整理 {len(normalized)} 条机构观点：{firm}",
            )
            if len(normalized) >= 4:
                break
        if len(normalized) >= 4:
            break
        notify(min(0.7, query_end * 0.7), f"已完成机构观点源 {query_index}/{total_queries} 的检索。")
    if normalized:
        _write_cache(path, normalized)
        notify(1.0, f"机构观点整理完成，共 {len(normalized)} 条。")
    else:
        notify(1.0, "未抓到可追溯的机构观点条目。")
    return normalized
