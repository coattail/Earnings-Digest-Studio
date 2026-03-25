from __future__ import annotations

import copy
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from ..config import CACHE_DIR, ensure_directories
from .official_materials import DISABLE_FETCH_ENV, REQUEST_HEADERS
from .local_data import get_company_series


OFFICIAL_SOURCES_DIR = CACHE_DIR / "official-sources"
RECENT_SUBMISSIONS_TTL_SECONDS = 12 * 60 * 60
HISTORICAL_SUBMISSIONS_TTL_SECONDS = 30 * 24 * 60 * 60
ARCHIVE_SUBMISSIONS_TTL_SECONDS = 90 * 24 * 60 * 60
IR_DISCOVERY_TTL_SECONDS = 14 * 24 * 60 * 60
SITEMAP_DISCOVERY_TTL_SECONDS = 14 * 24 * 60 * 60
DEFAULT_RELEASE_HINTS = (
    "press release",
    "earnings release",
    "earnings result",
    "financial results",
    "quarterly results",
    "result release",
    "99.1",
    "99_1",
    "ex99",
    "exhibit99",
    "exhibit991",
    "earningsrelease",
)
DEFAULT_RELEASE_EXCLUDES = ("presentation", "slides", "deck", "commentary", "cfo", "supplement", "webcast", "transcript")
DEFAULT_COMMENTARY_HINTS = (
    "commentary",
    "cfo",
    "supplement",
    "financial supplement",
    "99.2",
    "99_2",
    "exhibit992",
)
DEFAULT_COMMENTARY_EXCLUDES = ("webcast", "script", "deck", "slides", "presentation")
DEFAULT_PRESENTATION_HINTS = (
    "presentation",
    "slides",
    "deck",
    "99.2",
    "99_2",
    "exhibit992",
)
DEFAULT_PRESENTATION_EXCLUDES = ("webcast", "script", "commentary", "cfo", "supplement")
DEFAULT_CALL_HINTS = (
    "transcript",
    "webcast",
    "conference call",
    "earnings call",
    "prepared remarks",
    "script",
)
DEFAULT_CALL_EXCLUDES = ("presentation", "slides", "deck", "supplement")
ATTACHMENT_BINARY_SUFFIXES = {
    ".csv",
    ".gif",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".png",
    ".svg",
    ".xbrl",
    ".xml",
    ".xsd",
    ".xls",
    ".xlsx",
    ".zip",
}
SourceProgressCallback = Callable[[float, str], None]
RESOLVED_SOURCES_MEMORY_CACHE: dict[tuple[str, str, str, bool, tuple[tuple[str, str, str, str, str], ...]], list[dict[str, Any]]] = {}
SITEMAP_LINKS_MEMORY_CACHE: dict[str, list[dict[str, str]]] = {}


def _cache_path(company_id: str) -> Path:
    ensure_directories()
    OFFICIAL_SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    return OFFICIAL_SOURCES_DIR / f"{company_id}-sec-submissions.json"


def _historical_cache_path(company_id: str, filename: str) -> Path:
    ensure_directories()
    OFFICIAL_SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    stem = re.sub(r"[^A-Za-z0-9_-]+", "-", Path(filename).stem).strip("-") or "historical"
    return OFFICIAL_SOURCES_DIR / f"{company_id}-{stem}.json"


def _ir_cache_path(company_id: str, calendar_quarter: str) -> Path:
    ensure_directories()
    OFFICIAL_SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    return OFFICIAL_SOURCES_DIR / f"{company_id}-{calendar_quarter}-ir-discovery.json"


def _sitemap_cache_path(company_id: str, sitemap_url: str) -> Path:
    ensure_directories()
    OFFICIAL_SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    stem = re.sub(r"[^A-Za-z0-9_-]+", "-", sitemap_url).strip("-")[:96] or "sitemap"
    return OFFICIAL_SOURCES_DIR / f"{company_id}-{stem}-sitemap-links.json"


def _parse_date(value: str) -> Optional[date]:
    try:
        return date.fromisoformat(str(value or ""))
    except ValueError:
        return None


def _read_cached_submissions(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _write_cached_submissions(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _cache_file_is_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists():
        return False
    age_seconds = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime)
    return age_seconds <= ttl_seconds


def _submissions_ttl_seconds(period_end: Optional[str]) -> int:
    target_date = _parse_date(str(period_end or ""))
    if target_date is not None and abs((date.today() - target_date).days) <= 180:
        return RECENT_SUBMISSIONS_TTL_SECONDS
    return HISTORICAL_SUBMISSIONS_TTL_SECONDS


def _fetch_json(url: str) -> dict[str, Any]:
    with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def _fetch_text(url: str) -> str:
    with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def _load_submissions(company_id: str, sec_cik: str, *, period_end: Optional[str] = None, refresh: bool = False) -> Optional[dict[str, Any]]:
    path = _cache_path(company_id)
    cached = _read_cached_submissions(path)
    if cached and not refresh and _cache_file_is_fresh(path, _submissions_ttl_seconds(period_end)):
        return cached
    try:
        payload = _fetch_json(f"https://data.sec.gov/submissions/CIK{sec_cik}.json")
    except Exception:
        return cached
    return _write_cached_submissions(path, payload)


def _load_historical_submissions(company_id: str, filename: str, *, refresh: bool = False) -> Optional[dict[str, Any]]:
    path = _historical_cache_path(company_id, filename)
    cached = _read_cached_submissions(path)
    if cached and not refresh and _cache_file_is_fresh(path, ARCHIVE_SUBMISSIONS_TTL_SECONDS):
        return cached
    try:
        payload = _fetch_json(f"https://data.sec.gov/submissions/{filename}")
    except Exception:
        return cached
    return _write_cached_submissions(path, payload)


def _archive_url(sec_cik: str, accession_number: str, primary_document: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(sec_cik)}/"
        f"{str(accession_number).replace('-', '')}/{primary_document}"
    )


def _archive_txt_url(sec_cik: str, accession_number: str) -> str:
    normalized_accession = str(accession_number or "").strip()
    compact = normalized_accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{int(sec_cik)}/{compact}/{normalized_accession}.txt"


def _recent_row(recent: dict[str, list[Any]], index: int) -> dict[str, Any]:
    return {key: values[index] if index < len(values) else "" for key, values in recent.items()}


def _submission_rows(payload: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    if not payload:
        return []
    filings = payload.get("filings")
    if isinstance(filings, dict):
        recent = filings.get("recent", {})
        rows = recent.get("form") or []
        return [_recent_row(recent, index) for index in range(len(rows))]
    rows = payload.get("form") or []
    return [_recent_row(payload, index) for index in range(len(rows))]


def _submissions_window_rows(
    company_id: str,
    submissions: dict[str, Any],
    *,
    period_end: str,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    combined = _submission_rows(submissions)
    target_date = _parse_date(period_end)
    if target_date is None:
        return combined

    lower_bound = target_date - timedelta(days=400)
    upper_bound = target_date + timedelta(days=150)
    historical_files = submissions.get("filings", {}).get("files") or []
    seen_accessions = {
        str(row.get("accessionNumber") or "")
        for row in combined
        if str(row.get("accessionNumber") or "")
    }
    historical_rows: list[dict[str, Any]] = []
    for item in historical_files:
        filing_from = _parse_date(str(item.get("filingFrom") or ""))
        filing_to = _parse_date(str(item.get("filingTo") or ""))
        if filing_from and filing_from > upper_bound:
            continue
        if filing_to and filing_to < lower_bound:
            continue
        filename = str(item.get("name") or "").strip()
        if not filename:
            continue
        historical_payload = _load_historical_submissions(company_id, filename, refresh=refresh)
        for row in _submission_rows(historical_payload):
            accession = str(row.get("accessionNumber") or "")
            if accession and accession in seen_accessions:
                continue
            seen_accessions.add(accession)
            historical_rows.append(row)
    if not historical_rows:
        return combined
    ordered = combined + historical_rows
    ordered.sort(key=lambda row: str(row.get("filingDate") or ""), reverse=True)
    return ordered


def _document_tokens(*values: str) -> str:
    tokens: list[str] = []
    for value in values:
        lowered = str(value or "").lower().strip()
        if not lowered:
            continue
        tokens.append(lowered)
        normalized = re.sub(r"(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)", " ", lowered)
        normalized = re.sub(r"[-_/]+", " ", normalized)
        normalized = re.sub(r"[^a-z0-9.% ]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if normalized and normalized != lowered:
            tokens.append(normalized)
    return " ".join(tokens)


def _url_slug_text(url: str) -> str:
    path = urlparse(str(url or "")).path
    slug = path.rstrip("/").split("/")[-1]
    slug = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", slug)
    slug = re.sub(r"[-_]+", " ", slug)
    slug = re.sub(r"\s+", " ", slug)
    return slug.strip()


def _score_document_tokens(
    tokens: str,
    *,
    hints: tuple[str, ...],
    excludes: tuple[str, ...],
) -> int:
    score = 0
    if any(token in tokens for token in excludes):
        score -= 80
    for hint in hints:
        if hint in tokens:
            score += 35
    return score


def _filing_score(
    row: dict[str, Any],
    *,
    target_date: Optional[date],
    forms: list[str],
    release_mode: bool,
    document_hints: tuple[str, ...],
    document_excludes: tuple[str, ...],
) -> int:
    score = 0
    form = str(row.get("form") or "")
    if form not in forms:
        return -10_000
    document_tokens = _document_tokens(row.get("primaryDocument"), row.get("primaryDocDescription"))
    score += max(0, 200 - forms.index(form) * 30)
    filing_date = _parse_date(str(row.get("filingDate") or ""))
    if filing_date and target_date:
        delta = abs((filing_date - target_date).days)
        score += max(0, 160 - delta)
        if filing_date >= target_date:
            score += 12
    report_date = _parse_date(str(row.get("reportDate") or ""))
    if report_date and target_date:
        report_delta = abs((report_date - target_date).days)
        score += max(0, 120 - report_delta * 2)
        if report_delta == 0:
            score += 80
    if release_mode and form == "8-K" and "2.02" not in str(row.get("items") or ""):
        return -10_000
    score += _score_document_tokens(
        document_tokens,
        hints=document_hints,
        excludes=document_excludes,
    )
    if document_hints and not any(hint in document_tokens for hint in document_hints):
        score -= 120
    if document_excludes and any(token in document_tokens for token in document_excludes):
        score -= 160
    return score


def _select_filing(
    submissions: dict[str, Any],
    *,
    company_id: str,
    period_end: str,
    forms: list[str],
    release_mode: bool,
    document_hints: tuple[str, ...] = (),
    document_excludes: tuple[str, ...] = (),
    refresh: bool = False,
) -> Optional[dict[str, Any]]:
    rows = _submissions_window_rows(
        company_id,
        submissions,
        period_end=period_end,
        refresh=refresh,
    )
    if not rows or not forms:
        return None
    target_date = _parse_date(period_end)
    lower_bound = target_date - timedelta(days=45) if target_date else None
    upper_bound = target_date + timedelta(days=130) if target_date else None
    best_row: Optional[dict[str, Any]] = None
    best_score = -10_000
    for row in rows:
        filing_date = _parse_date(str(row.get("filingDate") or ""))
        if upper_bound and filing_date and filing_date > upper_bound:
            continue
        if lower_bound and filing_date and filing_date < lower_bound:
            continue
        score = _filing_score(
            row,
            target_date=target_date,
            forms=forms,
            release_mode=release_mode,
            document_hints=document_hints,
            document_excludes=document_excludes,
        )
        if score > best_score:
            best_score = score
            best_row = row
    return best_row if best_score > -1_000 else None


def _append_attachment_text(existing: str, text: str) -> str:
    cleaned = " ".join(str(text or "").split())
    if not cleaned:
        return existing
    if not existing:
        return cleaned
    if cleaned in existing:
        return existing
    return f"{existing} {cleaned}".strip()


def _attachment_rows(wrapper_html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(wrapper_html, "html.parser")
    aggregated: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        aggregated[href] = _append_attachment_text(aggregated.get(href, ""), anchor.get_text(" ", strip=True))
    return [{"href": href, "text": text} for href, text in aggregated.items()]


def _filing_index_json_url(wrapper_url: str) -> str:
    parsed = urlparse(wrapper_url)
    base = parsed.path.rsplit("/", 1)[0]
    return f"{parsed.scheme}://{parsed.netloc}{base}/index.json"


def _directory_attachment_rows(wrapper_url: str) -> list[dict[str, str]]:
    try:
        payload = _fetch_json(_filing_index_json_url(wrapper_url))
    except Exception:
        return []
    items = payload.get("directory", {}).get("item") or []
    rows: list[dict[str, str]] = []
    for item in items:
        name = str(item.get("name") or "").strip()
        if not name or "/" in name:
            continue
        rows.append(
            {
                "href": name,
                "text": name,
                "type": str(item.get("type") or ""),
            }
        )
    return rows


def _attachment_score(
    attachment: dict[str, str],
    *,
    wrapper_url: str,
    hints: tuple[str, ...],
    excludes: tuple[str, ...],
) -> int:
    href = attachment["href"]
    tokens = _document_tokens(href, attachment.get("text", ""), attachment.get("type", ""))
    score = _score_document_tokens(tokens, hints=hints, excludes=excludes)
    suffix = Path(urljoin(wrapper_url, href)).suffix.lower()
    lower_name = Path(href).name.lower()
    if suffix in ATTACHMENT_BINARY_SUFFIXES:
        score -= 220
    if suffix in {".htm", ".html", ".pdf", ".txt"}:
        score += 8
    if "form6k" in lower_name:
        score -= 18
    if lower_name.endswith("-index.html") or lower_name.endswith("-index-headers.html"):
        score -= 220
    if lower_name in {"index.html", "index.htm", "index.json"}:
        score -= 220
    if any(token in lower_name for token in ("header", "xbrl", "schema", "instance", "ex101", "graphic", "image")):
        score -= 120
    if re.fullmatch(r"\d{10}-\d{2}-\d{6}\.txt", lower_name):
        score -= 220
    if suffix == ".txt" and "exhibit99" not in lower_name and "ex99" not in lower_name:
        score -= 120
    if any(token in lower_name for token in ("pressrelease", "earningsrelease", "financialresults", "results", "withguidance")):
        score += 28
    if any(token in lower_name for token in ("quarterlyresul", "q1results", "q2results", "q3results", "q4results")):
        score += 24
    if any(token in lower_name for token in ("exhibit991", "exhibit99", "99-1", "99_1", "ex99")):
        score += 24
    if any(token in lower_name for token in ("presentation", "slides", "deck")):
        score += 22
    if any(token in lower_name for token in ("presentat", "presq", "presq1", "presq2", "presq3", "presq4")):
        score += 22
    if any(token in lower_name for token in ("commentary", "cfo", "supplement")):
        score += 26
    if any(token in lower_name for token in ("exhibit992", "99-2", "99_2")):
        score += 18
    if re.search(r"d[a-z0-9]+dex99(?:1|01)\b", lower_name):
        score += 24
    if re.search(r"d[a-z0-9]+dex99(?:2|02)\b", lower_name):
        score += 18
    return score


def _best_attachment(
    attachments: list[dict[str, str]],
    *,
    wrapper_url: str,
    hints: tuple[str, ...],
    excludes: tuple[str, ...],
) -> tuple[str, str, int]:
    best_url = wrapper_url
    best_text = ""
    best_score = -10_000
    for attachment in attachments:
        score = _attachment_score(attachment, wrapper_url=wrapper_url, hints=hints, excludes=excludes)
        if score <= best_score:
            continue
        best_score = score
        best_url = urljoin(wrapper_url, attachment["href"])
        best_text = attachment.get("text", "")
    return (best_url, best_text, best_score)


def _wrapper_document_score(
    wrapper_url: str,
    *,
    hints: tuple[str, ...],
    excludes: tuple[str, ...],
) -> int:
    parsed = urlparse(wrapper_url)
    name = Path(parsed.path).name.lower()
    tokens = _document_tokens(wrapper_url, _url_slug_text(wrapper_url), name)
    score = _score_document_tokens(tokens, hints=hints, excludes=excludes)
    if name.endswith((".htm", ".html", ".pdf", ".txt")):
        score += 8
    if name.endswith(("-index.html", "-index-headers.html", "index.html", "index.htm", "index.json")):
        score -= 220
    if any(token in name for token in ("header", "xbrl", "schema", "instance", "ex101", "graphic", "image")):
        score -= 120
    return score


def _discover_attachment_url(
    wrapper_url: str,
    *,
    hints: tuple[str, ...],
    excludes: tuple[str, ...],
) -> tuple[str, str]:
    def _prefer_attachment_over_wrapper(candidate_url: str, candidate_score: int) -> bool:
        wrapper_name = Path(urlparse(wrapper_url).path).name.lower()
        candidate_name = Path(urlparse(candidate_url).path).name.lower()
        return (
            candidate_url != wrapper_url
            and candidate_score >= wrapper_score
            and any(token in candidate_name for token in ("pressreleasequarterlyresul", "financialstatements", "presentat", "presq"))
            and any(token in wrapper_name for token in ("form6k", "quarterlyfilings"))
        )

    wrapper_score = _wrapper_document_score(
        wrapper_url,
        hints=hints,
        excludes=excludes,
    )
    directory_choice = _best_attachment(
        _directory_attachment_rows(wrapper_url),
        wrapper_url=wrapper_url,
        hints=hints,
        excludes=excludes,
    )
    if _prefer_attachment_over_wrapper(directory_choice[0], directory_choice[2]):
        return (directory_choice[0], directory_choice[1])
    if directory_choice[2] > wrapper_score + 6 and directory_choice[0] != wrapper_url:
        return (directory_choice[0], directory_choice[1])
    try:
        wrapper_html = _fetch_text(wrapper_url)
    except Exception:
        return (wrapper_url, "")
    best_url, best_text, best_score = _best_attachment(
        _attachment_rows(wrapper_html),
        wrapper_url=wrapper_url,
        hints=hints,
        excludes=excludes,
    )
    attachment_name = Path(urlparse(best_url).path).name.lower()
    attachment_tokens = _document_tokens(best_url, best_text, _url_slug_text(best_url))
    generic_exhibit_without_quarter_signal = (
        wrapper_score >= 35
        and re.search(r"d[a-z0-9]+dex99(?:1|01|2|02)\b", attachment_name) is not None
        and not any(
            token in attachment_tokens
            for token in (
                "quarter",
                "result",
                "financial",
                "q1",
                "q2",
                "q3",
                "q4",
                "presentation",
                "commentary",
                "supplement",
                "webcast",
                "transcript",
            )
        )
    )
    if generic_exhibit_without_quarter_signal:
        return (wrapper_url, "")
    if _prefer_attachment_over_wrapper(best_url, best_score):
        return (best_url, best_text)
    if best_score > wrapper_score + 6:
        return (best_url, best_text)
    return (wrapper_url, "")


def _release_label(company: dict[str, Any], attachment_text: str) -> str:
    cleaned = " ".join(str(attachment_text or "").split())
    if cleaned:
        return f"{company['english_name']} {cleaned}"
    return f"{company['english_name']} earnings release"


def _commentary_label(company: dict[str, Any], attachment_text: str) -> str:
    cleaned = " ".join(str(attachment_text or "").split())
    if cleaned:
        return f"{company['english_name']} {cleaned}"
    return f"{company['english_name']} earnings commentary"


def _presentation_label(company: dict[str, Any], attachment_text: str) -> str:
    cleaned = " ".join(str(attachment_text or "").split())
    if cleaned:
        return f"{company['english_name']} {cleaned}"
    return f"{company['english_name']} earnings presentation"


def _attachment_profile_label(company: dict[str, Any], role: str, attachment_text: str) -> str:
    if role == "earnings_commentary":
        return _commentary_label(company, attachment_text)
    if role == "earnings_presentation":
        return _presentation_label(company, attachment_text)
    return _release_label(company, attachment_text)


def _attachment_profiles(company: dict[str, Any], source_config: dict[str, Any]) -> list[dict[str, Any]]:
    configured = list(source_config.get("attachment_profiles") or [])
    if configured:
        profiles = []
        for profile in configured:
            profiles.append(
                {
                    "kind": str(profile.get("kind") or "presentation"),
                    "role": str(profile.get("role") or "earnings_presentation"),
                    "hints": tuple(str(item).lower() for item in list(profile.get("hints") or [])),
                    "excludes": tuple(str(item).lower() for item in list(profile.get("excludes") or [])),
                }
            )
        return profiles
    return [
        {
            "kind": "official_release",
            "role": "earnings_release",
            "hints": DEFAULT_RELEASE_HINTS,
            "excludes": DEFAULT_RELEASE_EXCLUDES,
        },
        {
            "kind": "presentation",
            "role": "earnings_commentary",
            "hints": DEFAULT_COMMENTARY_HINTS,
            "excludes": DEFAULT_COMMENTARY_EXCLUDES,
        },
        {
            "kind": "call_summary",
            "role": "earnings_call",
            "hints": DEFAULT_CALL_HINTS,
            "excludes": DEFAULT_CALL_EXCLUDES,
        },
        {
            "kind": "presentation",
            "role": "earnings_presentation",
            "hints": DEFAULT_PRESENTATION_HINTS,
            "excludes": DEFAULT_PRESENTATION_EXCLUDES,
        },
    ]


def _merge_sources(existing_sources: list[dict[str, Any]], resolved_sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    has_official = any(
        source.get("kind") in {"official_release", "sec_filing", "presentation"} and source.get("url")
        for source in resolved_sources
    )
    has_resolved_release = any(str(source.get("kind") or "") == "official_release" for source in resolved_sources)
    has_resolved_filing = any(str(source.get("kind") or "") == "sec_filing" for source in resolved_sources)
    ordered = list(resolved_sources)
    for source in existing_sources:
        if has_official and source.get("kind") == "investor_relations":
            continue
        if has_resolved_release and source.get("kind") == "official_release":
            continue
        if has_resolved_filing and source.get("kind") == "sec_filing":
            continue
        ordered.append(source)
    def _source_priority(source: dict[str, Any]) -> tuple[int, int]:
        kind = str(source.get("kind") or "")
        role = str(source.get("role") or "")
        kind_rank = {
            "official_release": 5,
            "sec_filing": 4,
            "call_summary": 3,
            "presentation": 2,
            "structured_financials": 1,
        }.get(kind, 0)
        role_rank = {
            "earnings_release": 5,
            "sec_filing": 4,
            "earnings_call": 3,
            "earnings_commentary": 2,
            "earnings_presentation": 1,
        }.get(role, 0)
        return (kind_rank, role_rank)

    ordered.sort(key=_source_priority, reverse=True)
    for source in ordered:
        url = str(source.get("url") or "")
        if not url or url in seen:
            continue
        merged.append(source)
        seen.add(url)
    return merged


def _quarter_sort_key(calendar_quarter: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(calendar_quarter or ""))
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))


def _sec_cik_for_calendar_quarter(source_config: dict[str, Any], calendar_quarter: str) -> str:
    default_cik = str(source_config.get("sec_cik") or "").strip()
    quarter_key = _quarter_sort_key(calendar_quarter)
    if quarter_key == (0, 0):
        return default_cik
    for item in list(source_config.get("historical_sec_ciks") or []):
        if not isinstance(item, dict):
            continue
        override_cik = str(item.get("sec_cik") or "").strip()
        through_quarter = str(item.get("through") or "").strip()
        if not override_cik or not through_quarter:
            continue
        if quarter_key <= _quarter_sort_key(through_quarter):
            return override_cik
    return default_cik


def _normalize_href(base_url: str, href: str) -> Optional[str]:
    candidate = str(href or "").strip()
    if not candidate or candidate.startswith(("#", "mailto:", "javascript:", "tel:")):
        return None
    absolute = urljoin(base_url, candidate)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return None
    return absolute


def _same_domain(url_a: str, url_b: str) -> bool:
    host_a = urlparse(url_a).netloc.lower().split(":")[0]
    host_b = urlparse(url_b).netloc.lower().split(":")[0]
    return bool(host_a and host_b and host_a == host_b)


def _page_links(page_url: str, html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        absolute = _normalize_href(page_url, str(anchor.get("href") or ""))
        if not absolute or absolute in seen:
            continue
        text = " ".join(anchor.get_text(" ", strip=True).split())
        links.append({"url": absolute, "text": text})
        seen.add(absolute)
    return links


def _fiscal_period_reference(company: dict[str, Any], period_end: str) -> tuple[Optional[int], Optional[int]]:
    target_date = _parse_date(period_end)
    if target_date is None:
        return (None, None)
    source_config = dict(company.get("official_source") or {})
    fiscal_year_end_month = int(source_config.get("fiscal_year_end_month") or 12)
    fiscal_year_end_month = min(12, max(1, fiscal_year_end_month))
    fiscal_year = target_date.year + (1 if target_date.month > fiscal_year_end_month else 0)
    fiscal_year_start_month = fiscal_year_end_month % 12 + 1
    month_offset = (target_date.month - fiscal_year_start_month) % 12
    fiscal_quarter = month_offset // 3 + 1
    return (fiscal_year, fiscal_quarter)


def _quarter_reference_terms(
    company: dict[str, Any],
    calendar_quarter: str,
    period_end: str,
) -> tuple[set[str], set[str], set[str], set[int]]:
    source_config = dict(company.get("official_source") or {})
    fiscal_year_end_month = int(source_config.get("fiscal_year_end_month") or 12)
    uses_calendar_quarter_terms = fiscal_year_end_month == 12
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(calendar_quarter or ""))
    calendar_terms: set[str] = set()
    fiscal_terms: set[str] = set()
    target_years: set[str] = set()
    allowed_quarters: set[int] = set()
    if match:
        year = int(match.group(1))
        quarter = int(match.group(2))
        if uses_calendar_quarter_terms:
            target_years.add(str(year))
            allowed_quarters.add(quarter)
            ordinal = {1: "first", 2: "second", 3: "third", 4: "fourth"}[quarter]
            calendar_terms.update(
                {
                    f"q{quarter}",
                    f"{year} q{quarter}",
                    f"q{quarter} {year}",
                    f"{year}q{quarter}",
                    f"{ordinal} quarter",
                    f"{ordinal} quarter {year}",
                    f"{year} {ordinal} quarter",
                    f"{quarter}q {year}",
                }
            )
    target_date = _parse_date(period_end)
    fiscal_year, fiscal_quarter = _fiscal_period_reference(company, period_end)
    if target_date is not None and fiscal_year is not None and fiscal_quarter is not None:
        target_years.add(str(target_date.year))
        target_years.add(str(fiscal_year))
        allowed_quarters.add(fiscal_quarter)
        ordinal = {1: "first", 2: "second", 3: "third", 4: "fourth"}[fiscal_quarter]
        month_name = target_date.strftime("%B").lower()
        short_month_name = target_date.strftime("%b").lower()
        fiscal_terms.update(
            {
                f"q{fiscal_quarter} fy{fiscal_year}",
                f"fy{fiscal_year} q{fiscal_quarter}",
                f"fy{fiscal_year}q{fiscal_quarter}",
                f"q{fiscal_quarter} fiscal year {fiscal_year}",
                f"fiscal year {fiscal_year} q{fiscal_quarter}",
                f"{ordinal} quarter fiscal year {fiscal_year}",
                f"fiscal year {fiscal_year} {ordinal} quarter",
                f"fy{str(fiscal_year)[-2:]} q{fiscal_quarter}",
                f"quarter ended {month_name} {target_date.day}, {fiscal_year}",
                f"quarter ended {month_name} {target_date.day} {fiscal_year}",
                f"quarter ended {month_name} {target_date.year}",
                f"quarter ended {short_month_name} {target_date.day} {target_date.year}",
                f"ended {month_name} {target_date.day}, {target_date.year}",
                f"ended {month_name} {target_date.day} {target_date.year}",
                f"{month_name} quarter {target_date.year}",
                f"{short_month_name} quarter {target_date.year}",
            }
        )
        calendar_terms.add(str(fiscal_year))
        calendar_terms.add(str(target_date.year))
    return calendar_terms, fiscal_terms, target_years, allowed_quarters


def _listing_page_score(link: dict[str, str]) -> int:
    tokens = _document_tokens(link.get("url", ""), link.get("text", ""))
    score = 0
    for keyword in (
        "quarterly results",
        "financial news releases",
        "news releases",
        "events and presentations",
        "events + presentations",
        "events presentations",
        "past events",
        "upcoming events",
        "earnings",
        "results",
        "financial reports",
        "quarterly",
        "presentations",
        "events",
    ):
        if keyword in tokens:
            score += 20
    if any(keyword in tokens for keyword in ("annual report", "proxy", "tax", "stock information", "governance", "esg", "sec filings archive")):
        score -= 20
    if "/news" in tokens or "/events" in tokens or "/financial-information" in tokens:
        score += 8
    return score


def _ir_source_score(
    link: dict[str, str],
    *,
    role: str,
    calendar_terms: set[str],
    fiscal_terms: set[str],
    target_years: set[str],
    allowed_quarters: set[int],
    company: dict[str, Any],
) -> int:
    tokens = _document_tokens(link.get("url", ""), link.get("text", ""))
    path = urlparse(str(link.get("url") or "")).path.lower()
    score = 0
    if role == "earnings_release":
        for keyword in ("earnings", "financial results", "quarterly results", "results", "press release"):
            if keyword in tokens:
                score += 22
        if "news-release-details" in path:
            score += 18
    elif role == "earnings_call":
        for keyword in ("webcast", "transcript", "earnings call", "conference call", "prepared remarks", "event"):
            if keyword in tokens:
                score += 24
        if "/events/" in path:
            score += 30
    elif role == "earnings_presentation":
        for keyword in ("presentation", "slides", "deck", "supplement", "pdf"):
            if keyword in tokens:
                score += 22
        if "/presentations" in path or "/quarterly-results" in path:
            score += 12
    elif role == "earnings_commentary":
        for keyword in ("commentary", "supplement", "prepared remarks"):
            if keyword in tokens:
                score += 20

    for token in calendar_terms:
        if token and token in tokens:
            score += 9
    for token in fiscal_terms:
        if token and token in tokens:
            score += 12

    mentioned_years = set(re.findall(r"\b(20\d{2})\b", tokens))
    if mentioned_years:
        if mentioned_years & target_years:
            score += 14
        elif target_years:
            score -= 24

    mentioned_quarters: set[int] = set()
    for quarter in range(1, 5):
        if f"q{quarter}" in tokens or f"{quarter}q" in tokens:
            mentioned_quarters.add(quarter)
    for quarter, ordinal in {1: "first", 2: "second", 3: "third", 4: "fourth"}.items():
        if f"{ordinal} quarter" in tokens:
            mentioned_quarters.add(quarter)
    if mentioned_quarters:
        if mentioned_quarters & allowed_quarters:
            score += 12
        else:
            score -= 18

    ticker = str(company.get("ticker") or "").lower()
    english_name = str(company.get("english_name") or "").lower()
    if ticker and ticker in tokens:
        score += 3
    if english_name and english_name.lower().split()[0] in tokens:
        score += 3

    if any(keyword in tokens for keyword in ("annual report", "proxy", "dividend", "esg", "sustainability", "stock information")):
        score -= 26
    return score


def _ir_temporal_alignment(
    link: dict[str, str],
    *,
    target_years: set[str],
    allowed_quarters: set[int],
) -> bool:
    tokens = _document_tokens(link.get("url", ""), link.get("text", ""))
    mentioned_years = set(re.findall(r"\b(20\d{2})\b", tokens))
    if mentioned_years and target_years and not (mentioned_years & target_years):
        return False
    mentioned_quarters: set[int] = set()
    for quarter in range(1, 5):
        if f"q{quarter}" in tokens or f"{quarter}q" in tokens:
            mentioned_quarters.add(quarter)
    for quarter, ordinal in {1: "first", 2: "second", 3: "third", 4: "fourth"}.items():
        if f"{ordinal} quarter" in tokens:
            mentioned_quarters.add(quarter)
    if mentioned_quarters and allowed_quarters and not (mentioned_quarters & allowed_quarters):
        return False
    return True


def _ir_role_keywords_match(link: dict[str, str], role: str) -> bool:
    tokens = _document_tokens(link.get("url", ""), link.get("text", ""))
    looks_like_upcoming_event = any(
        keyword in tokens
        for keyword in (
            "to announce",
            "will host",
            "will hold",
            "scheduled to discuss",
            "scheduled for",
            "register here",
            "register now",
            "upcoming event",
        )
    )
    has_archived_call_signal = any(keyword in tokens for keyword in ("transcript", "prepared remarks", "replay", "archive"))
    if role == "earnings_call":
        if looks_like_upcoming_event and not has_archived_call_signal:
            return False
        return any(keyword in tokens for keyword in ("webcast", "transcript", "earnings call", "conference call", "prepared remarks", "call replay"))
    if role == "earnings_presentation":
        if any(keyword in tokens for keyword in ("events and presentations", "past events", "upcoming events", "quarterly results")):
            return False
        if any(keyword in tokens for keyword in ("how to", "tutorial", "template", "training", "pitch deck")):
            return False
        has_asset_hint = any(keyword in tokens for keyword in ("presentation", "slides", "deck", "supplement", ".pdf", "download"))
        has_earnings_context = any(
            keyword in tokens
            for keyword in ("earnings", "financial results", "quarterly results", "fiscal", "q1", "q2", "q3", "q4", "investor")
        )
        return has_asset_hint and has_earnings_context
    if role == "earnings_commentary":
        return any(keyword in tokens for keyword in ("commentary", "supplement", "prepared remarks"))
    if role == "earnings_release":
        if "to announce" in tokens:
            return False
        if any(keyword in tokens for keyword in ("earnings call", "conference call", "webcast", "transcript", "prepared remarks", "presentation", "slides", "deck", "/events/")):
            return False
        return any(keyword in tokens for keyword in ("earnings", "financial results", "quarterly results", "press release", "results"))
    return True


def _ir_related_link_keywords_match(link: dict[str, str], role: str, page_role: str) -> bool:
    tokens = _document_tokens(link.get("url", ""), link.get("text", ""))
    if role == "earnings_release" and page_role == "earnings_release":
        return any(keyword in tokens for keyword in ("pdf", "download", "print"))
    return _ir_role_keywords_match(link, role)


def _ir_label(company: dict[str, Any], role: str, text: str) -> str:
    cleaned = " ".join(str(text or "").split())
    if cleaned:
        return f"{company['english_name']} {cleaned}"
    if role == "earnings_call":
        return f"{company['english_name']} earnings call"
    if role == "earnings_presentation":
        return f"{company['english_name']} earnings presentation"
    if role == "earnings_commentary":
        return f"{company['english_name']} earnings commentary"
    return f"{company['english_name']} earnings release"


def _ir_kind_for_role(role: str) -> str:
    if role == "earnings_call":
        return "call_summary"
    if role in {"earnings_presentation", "earnings_commentary"}:
        return "presentation"
    return "official_release"


def _discover_related_ir_sources(
    page_source: dict[str, Any],
    *,
    company: dict[str, Any],
    calendar_terms: set[str],
    fiscal_terms: set[str],
    target_years: set[str],
    allowed_quarters: set[int],
) -> list[dict[str, Any]]:
    page_url = str(page_source.get("url") or "").strip()
    if not page_url or urlparse(page_url).scheme not in {"http", "https"}:
        return []
    try:
        html = _fetch_text(page_url)
    except Exception:
        return []
    related: list[dict[str, Any]] = []
    page_label = str(page_source.get("label") or "")
    page_role = str(page_source.get("role") or "")
    page_links = _page_links(page_url, html)
    candidate_roles = {"earnings_call", "earnings_presentation"}
    if page_role == "earnings_release":
        candidate_roles = {"earnings_release", "earnings_call", "earnings_presentation"}
    elif page_role == "earnings_call":
        candidate_roles = {"earnings_call"}
    elif page_role == "earnings_presentation":
        candidate_roles = {"earnings_presentation"}
    for role in candidate_roles:
        best_link: Optional[dict[str, str]] = None
        best_score = 0
        for link in page_links:
            raw_link = {
                "url": link["url"],
                "text": str(link.get("text", "")).strip(),
            }
            if not _ir_related_link_keywords_match(raw_link, role, page_role):
                continue
            contextual_link = {
                "url": link["url"],
                "text": f"{page_label} {link.get('text', '')}".strip(),
            }
            if not _ir_temporal_alignment(contextual_link, target_years=target_years, allowed_quarters=allowed_quarters):
                continue
            score = _ir_source_score(
                contextual_link,
                role=role,
                calendar_terms=calendar_terms,
                fiscal_terms=fiscal_terms,
                target_years=target_years,
                allowed_quarters=allowed_quarters,
                company=company,
            )
            if score > best_score:
                best_score = score
                best_link = contextual_link
        if best_link is None or best_score < 38:
            continue
        if str(best_link["url"]) == page_url:
            continue
        related.append(
            {
                "label": _ir_label(company, role, best_link.get("text", "")),
                "url": str(best_link["url"]),
                "kind": _ir_kind_for_role(role),
                "role": role,
                "date": str(page_source.get("date") or ""),
            }
        )
    return related


def _discover_ir_sources(
    company: dict[str, Any],
    calendar_quarter: str,
    period_end: str,
    *,
    refresh: bool = False,
    required_roles: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    ir_url = str(company.get("ir_url") or "").strip()
    if not ir_url:
        return []
    cache_path = _ir_cache_path(str(company["id"]), calendar_quarter)
    cached = _read_cached_submissions(cache_path)
    if cached and not refresh and _cache_file_is_fresh(cache_path, IR_DISCOVERY_TTL_SECONDS):
        payload_sources = list(cached.get("sources") or [])
        if required_roles:
            payload_sources = [item for item in payload_sources if str(item.get("role") or "") in required_roles]
        return payload_sources

    try:
        home_html = _fetch_text(ir_url)
    except Exception:
        return list(cached.get("sources") or []) if cached else []

    fetched_pages: dict[str, str] = {ir_url: home_html}
    queue: list[tuple[str, int, int]] = [(ir_url, 0, 100)]
    visited: set[str] = set()
    max_pages = 10
    while queue and len(fetched_pages) <= max_pages:
        page_url, depth, _score = queue.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)
        html = fetched_pages.get(page_url)
        if html is None:
            try:
                html = _fetch_text(page_url)
            except Exception:
                continue
            fetched_pages[page_url] = html
        if depth >= 2:
            continue
        child_candidates: list[tuple[int, str]] = []
        for link in _page_links(page_url, html):
            if not _same_domain(ir_url, link["url"]):
                continue
            score = _listing_page_score(link)
            if score <= 0:
                continue
            child_candidates.append((score, link["url"]))
        for score, child_url in sorted(child_candidates, reverse=True)[:5]:
            if child_url in visited or child_url in fetched_pages:
                continue
            queue.append((child_url, depth + 1, score))
            if len(queue) + len(fetched_pages) > max_pages:
                break

    all_links: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for page_url, html in fetched_pages.items():
        for link in _page_links(page_url, html):
            if not _same_domain(ir_url, link["url"]) or link["url"] in seen_urls:
                continue
            all_links.append(link)
            seen_urls.add(link["url"])

    calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(company, calendar_quarter, period_end)
    roles = required_roles or {"earnings_release", "earnings_call", "earnings_presentation", "earnings_commentary"}
    discovered: list[dict[str, Any]] = []
    for role in roles:
        best_link: Optional[dict[str, str]] = None
        best_score = 0
        for link in all_links:
            if not _ir_role_keywords_match(link, role):
                continue
            if not _ir_temporal_alignment(link, target_years=target_years, allowed_quarters=allowed_quarters):
                continue
            score = _ir_source_score(
                link,
                role=role,
                calendar_terms=calendar_terms,
                fiscal_terms=fiscal_terms,
                target_years=target_years,
                allowed_quarters=allowed_quarters,
                company=company,
            )
            if score > best_score:
                best_score = score
                best_link = link
        if best_link is None or best_score < 32:
            continue
        discovered.append(
            {
                "label": _ir_label(company, role, best_link.get("text", "")),
                "url": str(best_link["url"]),
                "kind": _ir_kind_for_role(role),
                "role": role,
                "date": "",
            }
        )

    _write_cached_submissions(
        cache_path,
        {
            "company_id": company["id"],
            "calendar_quarter": calendar_quarter,
            "period_end": period_end,
            "sources": discovered,
        },
    )
    return discovered


def _discover_sitemap_sources(
    company: dict[str, Any],
    calendar_quarter: str,
    period_end: str,
    sitemap_url: str,
    *,
    refresh: bool = False,
    required_roles: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    def _dedupe_urls(urls: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for item in urls:
            normalized = str(item or "").strip()
            if not normalized or normalized in seen:
                continue
            deduped.append(normalized)
            seen.add(normalized)
        return deduped

    def _sitemap_discovery_score(url: str) -> int:
        tokens = _document_tokens(url)
        score = 0
        for keyword in (
            "investor",
            "earnings",
            "results",
            "quarter",
            "financial",
            "press",
            "news",
            "presentation",
            "events",
        ):
            if keyword in tokens:
                score += 8
        if tokens.endswith(".xml"):
            score += 3
        if any(keyword in tokens for keyword in ("image", "video", "product", "support", "store")):
            score -= 6
        return score

    def _cached_sitemap_links(target_url: str) -> list[dict[str, str]]:
        cache_key = f"{company['id']}::{target_url}"
        if not refresh and cache_key in SITEMAP_LINKS_MEMORY_CACHE:
            return [dict(item) for item in SITEMAP_LINKS_MEMORY_CACHE[cache_key]]
        cache_path = _sitemap_cache_path(str(company["id"]), target_url)
        if not refresh and _cache_file_is_fresh(cache_path, SITEMAP_DISCOVERY_TTL_SECONDS):
            cached = _read_cached_submissions(cache_path)
            links = list(cached.get("links") or []) if isinstance(cached, dict) else []
            normalized = [
                {"url": str(item.get("url") or ""), "text": str(item.get("text") or "")}
                for item in links
                if str(item.get("url") or "")
            ]
            SITEMAP_LINKS_MEMORY_CACHE[cache_key] = normalized
            return [dict(item) for item in normalized]
        links = _sitemap_links_from_url(target_url)
        normalized = [
            {"url": str(item.get("url") or ""), "text": str(item.get("text") or "")}
            for item in links
            if str(item.get("url") or "")
        ]
        _write_cached_submissions(
            cache_path,
            {
                "company_id": company["id"],
                "sitemap_url": target_url,
                "links": normalized,
            },
        )
        SITEMAP_LINKS_MEMORY_CACHE[cache_key] = normalized
        return [dict(item) for item in normalized]

    def _sitemap_links_from_url(target_url: str, depth: int = 0) -> list[dict[str, str]]:
        try:
            payload = _fetch_text(target_url)
        except Exception:
            return []
        try:
            root = ET.fromstring(payload)
        except ET.ParseError:
            return []

        namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        loc_nodes = root.findall(".//sm:loc", namespace) or root.findall(".//loc")
        if not loc_nodes:
            return []

        root_tag = str(root.tag or "").lower()
        loc_values = _dedupe_urls([str(node.text or "").strip() for node in loc_nodes if str(node.text or "").strip()])
        if "sitemapindex" in root_tag and depth < 1:
            child_urls = sorted(loc_values, key=_sitemap_discovery_score, reverse=True)[:8]
            nested_links: list[dict[str, str]] = []
            seen_nested: set[str] = set()
            for child_url in child_urls:
                for link in _sitemap_links_from_url(child_url, depth + 1):
                    link_url = str(link.get("url") or "")
                    if not link_url or link_url in seen_nested:
                        continue
                    nested_links.append(link)
                    seen_nested.add(link_url)
            return nested_links

        return [{"url": loc, "text": _url_slug_text(loc)} for loc in loc_values]

    calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(company, calendar_quarter, period_end)
    roles = required_roles or {"earnings_release", "earnings_call", "earnings_presentation", "earnings_commentary"}
    sitemap_links = _cached_sitemap_links(sitemap_url)
    if not sitemap_links:
        return []
    discovered: list[dict[str, Any]] = []
    for role in roles:
        best_link: Optional[dict[str, str]] = None
        best_score = 0
        for link in sitemap_links:
            if not _ir_role_keywords_match(link, role):
                continue
            if not _ir_temporal_alignment(link, target_years=target_years, allowed_quarters=allowed_quarters):
                continue
            score = _ir_source_score(
                link,
                role=role,
                calendar_terms=calendar_terms,
                fiscal_terms=fiscal_terms,
                target_years=target_years,
                allowed_quarters=allowed_quarters,
                company=company,
            )
            if score > best_score:
                best_score = score
                best_link = link
        if best_link is None or best_score < 34:
            continue
        discovered.append(
            {
                "label": _ir_label(company, role, best_link.get("text", "")),
                "url": str(best_link["url"]),
                "kind": _ir_kind_for_role(role),
                "role": role,
                "date": "",
            }
        )
    return discovered


def _discover_default_sitemap_urls(ir_url: str) -> list[str]:
    parsed = urlparse(str(ir_url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return []

    origin = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.rstrip("/")
    base_urls = [origin]
    if path:
        base_urls.append(f"{origin}{path}")

    candidates: list[str] = []
    for base_url in base_urls:
        candidates.extend(
            [
                f"{base_url}/sitemap.xml",
                f"{base_url}/sitemap_index.xml",
                f"{base_url}/sitemap-index.xml",
            ]
        )

    try:
        robots_text = _fetch_text(f"{origin}/robots.txt")
    except Exception:
        robots_text = ""
    if robots_text:
        for line in robots_text.splitlines():
            if not line.lower().startswith("sitemap:"):
                continue
            robots_sitemap_url = str(line.split(":", 1)[1] or "").strip()
            if robots_sitemap_url:
                candidates.append(robots_sitemap_url)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _series_fallback_source(company: dict[str, Any], calendar_quarter: str) -> Optional[dict[str, Any]]:
    source_config = dict(company.get("official_source") or {})
    sec_cik = _sec_cik_for_calendar_quarter(source_config, calendar_quarter)
    if not sec_cik:
        return None
    try:
        _, series = get_company_series(str(company["id"]))
    except Exception:
        return None
    period_meta = dict(series.get("periodMeta") or {}).get(calendar_quarter)
    if not isinstance(period_meta, dict):
        return None
    accession = str(period_meta.get("accn") or "").strip()
    if not accession:
        return None
    filing_form = str(period_meta.get("form") or "SEC filing").strip()
    filed = str(period_meta.get("filed") or "")
    return {
        "label": f"{company['english_name']} Form {filing_form} (companyfacts fallback)",
        "url": _archive_txt_url(sec_cik, accession),
        "kind": "sec_filing",
        "role": "sec_filing",
        "date": filed,
    }


def resolve_official_sources(
    company: dict[str, Any],
    calendar_quarter: str,
    period_end: str,
    existing_sources: Optional[list[dict[str, Any]]] = None,
    *,
    refresh: bool = False,
    prefer_sec_only: bool = False,
    progress_callback: Optional[SourceProgressCallback] = None,
) -> list[dict[str, Any]]:
    def notify(progress: float, message: str) -> None:
        if progress_callback is None:
            return
        progress_callback(max(0.0, min(1.0, float(progress))), message)

    source_config = dict(company.get("official_source") or {})
    existing_sources = list(existing_sources or [])
    cache_key = (
        str(company.get("id") or ""),
        str(calendar_quarter or ""),
        str(period_end or ""),
        bool(prefer_sec_only),
        tuple(str(item).upper() for item in list(source_config.get("release_forms") or [])),
        tuple(str(item).upper() for item in list(source_config.get("filing_forms") or [])),
        tuple(str(item).lower() for item in list(source_config.get("release_document_hints") or [])),
        tuple(str(item).lower() for item in list(source_config.get("release_document_excludes") or [])),
        tuple(str(item).lower() for item in list(source_config.get("filing_document_hints") or [])),
        tuple(str(item).lower() for item in list(source_config.get("filing_document_excludes") or [])),
        tuple(
            (
                str(item.get("url") or ""),
                str(item.get("label") or ""),
                str(item.get("kind") or ""),
                str(item.get("role") or ""),
                str(item.get("date") or ""),
            )
            for item in existing_sources
        ),
    )
    if not refresh and cache_key in RESOLVED_SOURCES_MEMORY_CACHE:
        notify(1.0, "已复用进程内官方源发现缓存。")
        return copy.deepcopy(RESOLVED_SOURCES_MEMORY_CACHE[cache_key])
    sec_cik = _sec_cik_for_calendar_quarter(source_config, calendar_quarter)
    sitemap_urls = [str(item).strip() for item in list(source_config.get("discovery_sitemaps") or []) if str(item).strip()]
    if os.environ.get(DISABLE_FETCH_ENV) == "1":
        notify(1.0, "当前公司未启用在线官方源解析，直接沿用现有来源。")
        return existing_sources

    submissions: Optional[dict[str, Any]] = None
    if sec_cik:
        notify(0.12, f"正在读取 {company['ticker']} 的 SEC submissions...")
        submissions = _load_submissions(company["id"], sec_cik, period_end=period_end, refresh=refresh)

    release_forms = list(source_config.get("release_forms") or [])
    filing_forms = list(source_config.get("filing_forms") or [])
    release_document_hints = tuple(str(item).lower() for item in source_config.get("release_document_hints") or [])
    release_document_excludes = tuple(str(item).lower() for item in source_config.get("release_document_excludes") or [])
    filing_document_hints = tuple(str(item).lower() for item in source_config.get("filing_document_hints") or [])
    filing_document_excludes = tuple(str(item).lower() for item in source_config.get("filing_document_excludes") or [])
    attachment_profiles = _attachment_profiles(company, source_config)

    notify(0.34, "正在匹配最相关的 earnings release / filing...")
    release_row = None
    filing_row = None
    if submissions:
        release_row = _select_filing(
            submissions,
            company_id=company["id"],
            period_end=period_end,
            forms=release_forms,
            release_mode=True,
            document_hints=release_document_hints,
            document_excludes=release_document_excludes,
            refresh=refresh,
        )
        filing_row = _select_filing(
            submissions,
            company_id=company["id"],
            period_end=period_end,
            forms=filing_forms,
            release_mode=False,
            document_hints=filing_document_hints,
            document_excludes=filing_document_excludes,
            refresh=refresh,
        )

    resolved: list[dict[str, Any]] = []

    if release_row:
        wrapper_url = _archive_url(sec_cik, str(release_row["accessionNumber"]), str(release_row["primaryDocument"]))
        if prefer_sec_only and not filing_row:
            resolved.append(
                {
                    "label": f"{company['english_name']} earnings release (SEC submission)",
                    "url": _archive_txt_url(sec_cik, str(release_row["accessionNumber"])),
                    "kind": "official_release",
                    "role": "earnings_release",
                    "date": str(release_row.get("filingDate") or ""),
                }
            )
        else:
            discovered_urls: set[str] = set()
            for index, profile in enumerate(attachment_profiles, start=1):
                notify(0.46 + index * 0.1, f"正在定位 {profile['role']} 附件...")
                hints = tuple(profile["hints"])
                excludes = tuple(profile["excludes"])
                if profile["role"] == "earnings_release":
                    hints = hints + release_document_hints
                    excludes = excludes + release_document_excludes
                attachment_url, attachment_text = _discover_attachment_url(
                    wrapper_url,
                    hints=hints,
                    excludes=excludes,
                )
                if attachment_url == wrapper_url and not attachment_text:
                    continue
                if attachment_url in discovered_urls:
                    continue
                discovered_urls.add(attachment_url)
                resolved.append(
                    {
                        "label": _attachment_profile_label(company, str(profile["role"]), attachment_text),
                        "url": attachment_url,
                        "kind": str(profile["kind"]),
                        "role": str(profile["role"]),
                        "date": str(release_row.get("filingDate") or ""),
                    }
                )

    if filing_row:
        filing_url = _archive_url(sec_cik, str(filing_row["accessionNumber"]), str(filing_row["primaryDocument"]))
        if str(filing_row.get("form") or "").upper() == "6-K":
            filing_attachment_url, _filing_attachment_text = _discover_attachment_url(
                filing_url,
                hints=(
                    "99.3",
                    "99_3",
                    "summary",
                    "financial statements",
                    "press release",
                    "presentation",
                    "quarterly results",
                    "earnings",
                    "99.1",
                    "99.2",
                ),
                excludes=filing_document_excludes,
            )
            if filing_attachment_url and filing_attachment_url != filing_url:
                filing_url = filing_attachment_url
        if not any(str(source.get("url") or "") == filing_url for source in resolved):
            resolved.append(
                {
                    "label": f"{company['english_name']} Form {filing_row['form']}",
                    "url": filing_url,
                    "kind": "sec_filing",
                    "role": "sec_filing",
                    "date": str(filing_row.get("filingDate") or ""),
                }
            )
    if not resolved and sec_cik:
        fallback_source = _series_fallback_source(company, calendar_quarter)
        if fallback_source is not None:
            resolved.append(fallback_source)

    ir_url = str(company.get("ir_url") or "").strip()
    resolved_roles = {str(item.get("role") or "") for item in resolved}
    has_ir_release = any(
        str(item.get("role") or "") == "earnings_release"
        and ir_url
        and _same_domain(ir_url, str(item.get("url") or ""))
        for item in resolved
    )
    missing_roles: set[str] = set()
    if not prefer_sec_only:
        missing_roles = {
            role
            for role in {"earnings_release", "earnings_call", "earnings_presentation"}
            if role not in resolved_roles or (role == "earnings_release" and ir_url and not has_ir_release)
        }
    auto_sitemap_urls: list[str] = []
    if missing_roles and ir_url:
        auto_sitemap_urls = _discover_default_sitemap_urls(ir_url)
    sitemap_candidates = list(dict.fromkeys(sitemap_urls + auto_sitemap_urls))
    if missing_roles and sitemap_candidates:
        notify(0.78, "正在扫描公司官方 sitemap，定位历史财报与电话会页面...")
        sitemap_sources: list[dict[str, Any]] = []
        for sitemap_url in sitemap_candidates:
            sitemap_sources.extend(
                _discover_sitemap_sources(
                    company,
                    calendar_quarter,
                    period_end,
                    sitemap_url,
                    refresh=refresh,
                    required_roles=missing_roles,
                )
            )
        resolved.extend(sitemap_sources)
        calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(company, calendar_quarter, period_end)
        related_sources: list[dict[str, Any]] = []
        for source in list(sitemap_sources):
            if str(source.get("role") or "") not in {"earnings_release", "earnings_call", "earnings_presentation"}:
                continue
            related_sources.extend(
                _discover_related_ir_sources(
                    source,
                    company=company,
                    calendar_terms=calendar_terms,
                    fiscal_terms=fiscal_terms,
                    target_years=target_years,
                    allowed_quarters=allowed_quarters,
                )
            )
        resolved.extend(related_sources)
        resolved_roles = {str(item.get("role") or "") for item in resolved}
        has_ir_release = any(
            str(item.get("role") or "") == "earnings_release"
            and ir_url
            and _same_domain(ir_url, str(item.get("url") or ""))
            for item in resolved
        )
        missing_roles = {
            role
            for role in {"earnings_release", "earnings_call", "earnings_presentation"}
            if role not in resolved_roles or (role == "earnings_release" and ir_url and not has_ir_release)
        }
    if missing_roles and ir_url and not prefer_sec_only:
        notify(0.86, "正在补充扫描 IR 站点中的历史财报、电话会与演示材料...")
        ir_sources = _discover_ir_sources(
            company,
            calendar_quarter,
            period_end,
            refresh=refresh,
            required_roles=missing_roles,
        )
        resolved.extend(ir_sources)
        calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(company, calendar_quarter, period_end)
        related_sources: list[dict[str, Any]] = []
        for source in list(ir_sources):
            if str(source.get("role") or "") not in {"earnings_release", "earnings_call", "earnings_presentation"}:
                continue
            related_sources.extend(
                _discover_related_ir_sources(
                    source,
                    company=company,
                    calendar_terms=calendar_terms,
                    fiscal_terms=fiscal_terms,
                    target_years=target_years,
                    allowed_quarters=allowed_quarters,
                )
            )
        resolved.extend(related_sources)

    merged = _merge_sources(existing_sources, resolved)
    if not refresh:
        RESOLVED_SOURCES_MEMORY_CACHE[cache_key] = copy.deepcopy(merged)
    notify(1.0, f"官方源解析完成，共整理 {len(merged)} 个来源。")
    return merged
