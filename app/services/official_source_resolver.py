from __future__ import annotations

import copy
import html
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from ..config import CACHE_DIR, ensure_directories
from .official_materials import DISABLE_FETCH_ENV, OFFICIAL_MATERIALS_DIR, REQUEST_HEADERS
from .local_data import get_company_series


OFFICIAL_SOURCES_DIR = CACHE_DIR / "official-sources"
RECENT_SUBMISSIONS_TTL_SECONDS = 12 * 60 * 60
HISTORICAL_SUBMISSIONS_TTL_SECONDS = 30 * 24 * 60 * 60
ARCHIVE_SUBMISSIONS_TTL_SECONDS = 90 * 24 * 60 * 60
IR_DISCOVERY_TTL_SECONDS = 14 * 24 * 60 * 60
SITEMAP_DISCOVERY_TTL_SECONDS = 14 * 24 * 60 * 60
IR_DISCOVERY_CACHE_VERSION = 4
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
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
    "replay",
    "archive",
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


def _historical_sec_coverage_is_sufficient(
    resolved_sources: list[dict[str, Any]],
    period_end: str,
    *,
    prefer_sec_only: bool,
    filing_forms: list[str],
) -> bool:
    if prefer_sec_only:
        return True
    target_date = _parse_date(str(period_end or ""))
    if target_date is None or abs((date.today() - target_date).days) <= 365:
        return False
    roles = {str(item.get("role") or "") for item in resolved_sources}
    has_release = "earnings_release" in roles
    has_filing = "sec_filing" in roles or not filing_forms
    return has_release and has_filing


def _fetch_json(url: str) -> dict[str, Any]:
    with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def _fetch_json_response(url: str, *, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
        response = client.get(url, params=params)
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


def _call_label(company: dict[str, Any], attachment_text: str) -> str:
    cleaned = " ".join(str(attachment_text or "").split())
    if cleaned:
        return f"{company['english_name']} {cleaned}"
    return f"{company['english_name']} earnings call"


def _attachment_profile_label(company: dict[str, Any], role: str, attachment_text: str) -> str:
    if role == "earnings_call":
        return _call_label(company, attachment_text)
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
            "kind": "call_summary",
            "role": "earnings_call",
            "hints": DEFAULT_CALL_HINTS,
            "excludes": DEFAULT_CALL_EXCLUDES,
        },
        {
            "kind": "presentation",
            "role": "earnings_commentary",
            "hints": DEFAULT_COMMENTARY_HINTS,
            "excludes": DEFAULT_COMMENTARY_EXCLUDES,
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
    seen_indices: dict[str, int] = {}
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
        if not url:
            continue
        existing_index = seen_indices.get(url)
        if existing_index is not None:
            existing = dict(merged[existing_index])
            updated = dict(existing)
            for key, value in source.items():
                if updated.get(key) in (None, "", []) and value not in (None, "", []):
                    updated[key] = value
            if not updated.get("fetch_url") and source.get("fetch_url"):
                updated["fetch_url"] = source.get("fetch_url")
            merged[existing_index] = updated
            continue
        seen_indices[url] = len(merged)
        merged.append(source)
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


def _unwrap_document_service_url(url: str) -> str:
    candidate = str(url or "").strip()
    parsed = urlparse(candidate)
    host = parsed.netloc.lower().split(":")[0]
    if host in {"view.officeapps.live.com", "view.officeapps-df.live.com"}:
        source_url = str(parse_qs(parsed.query).get("src", [""])[0] or "").strip()
        if source_url:
            return source_url
    return candidate


def _normalize_href(base_url: str, href: str) -> Optional[str]:
    candidate = str(href or "").strip()
    if not candidate or candidate.startswith(("#", "mailto:", "javascript:", "tel:")):
        return None
    absolute = _unwrap_document_service_url(urljoin(base_url, candidate))
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return None
    return absolute


def _same_domain(url_a: str, url_b: str) -> bool:
    host_a = urlparse(url_a).netloc.lower().split(":")[0]
    host_b = urlparse(url_b).netloc.lower().split(":")[0]
    return bool(host_a and host_b and host_a == host_b)


def _documentish_page_link(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    path = parsed.path.lower()
    host = parsed.netloc.lower().split(":")[0]
    if not path:
        return False
    name = Path(path).name.lower()
    if name in {"blank.html", "blank.htm", "session-error.htm", "session-error.html", "error.htm", "error.html"}:
        return False
    if name == "webcast.htm" and not str(parsed.query or "").strip():
        return False
    if any(
        social_host in host
        for social_host in ("facebook.com", "linkedin.com", "twitter.com", "x.com", "reddit.com", "pinterest.com")
    ):
        return False
    if any(token in path for token in ("/onerfstatics/", "/_scrf/", "/etc.clientlibs/", "/uhf/")):
        return False
    if any(path.endswith(suffix) for suffix in (".css", ".js", ".svg", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".ttf", ".map")):
        return False
    return True


def _string_looks_like_link_candidate(value: str) -> bool:
    candidate = str(value or "").strip()
    if not candidate or len(candidate) > 2048:
        return False
    lowered = candidate.lower()
    if lowered.startswith(("http://", "https://", "/")):
        return True
    return any(
        lowered.endswith(suffix)
        for suffix in (".pdf", ".htm", ".html", ".txt", ".xml", ".json", ".mp3", ".mp4", ".m3u8", ".aspx")
    )


def _extract_script_links(page_url: str, script_text: str) -> list[dict[str, str]]:
    extracted: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_candidate(raw_value: str, context: str = "") -> None:
        normalized_value = html.unescape(str(raw_value or "").strip()).replace("\\/", "/")
        absolute = _normalize_href(page_url, normalized_value)
        if not absolute or absolute in seen or not _documentish_page_link(absolute):
            return
        context_text = " ".join(str(context or "").split())
        lowered_context = context_text.lower()
        if (
            not context_text
            or len(context_text) > 180
            or any(token in lowered_context for token in ("{", "}", "function", "defaultlocale", "global variables", "//"))
        ):
            context_text = _url_slug_text(absolute)
        text = " ".join(str(context_text or _url_slug_text(absolute)).split())
        extracted.append({"url": absolute, "text": text})
        seen.add(absolute)

    payload = str(script_text or "").strip()
    if not payload:
        return []
    if payload[:1] in {"{", "["}:
        try:
            json_payload = json.loads(payload)
        except json.JSONDecodeError:
            json_payload = None
        if json_payload is not None:
            stack: list[Any] = [json_payload]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)
                elif isinstance(node, str) and _string_looks_like_link_candidate(node):
                    add_candidate(node, payload[:120])

    absolute_pattern = re.compile(r"https?://[^\s\"'<>\\]+", flags=re.IGNORECASE)
    relative_pattern = re.compile(
        r"(?:(?<=['\"])|(?<=[:\s]))((?:/|\.\./)[^\"'<>\\]+?\.(?:aspx|pdf|htm|html|txt|xml|json|mp3|mp4|m3u8)(?:\?[^\"'<>\\]*)?)",
        flags=re.IGNORECASE,
    )
    for match in absolute_pattern.finditer(payload):
        add_candidate(match.group(0), payload[:120])
    for match in relative_pattern.finditer(payload):
        add_candidate(match.group(1), payload[:120])
    return extracted


def _page_links(page_url: str, html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    attribute_candidates = {
        "href",
        "link",
        "src",
        "data-url",
        "data-href",
        "data-link",
        "data-download-url",
        "data-file",
        "data-pdf",
    }

    def add_link(raw_href: str, text: str = "") -> None:
        absolute = _normalize_href(page_url, raw_href)
        if not absolute or absolute in seen or not _documentish_page_link(absolute):
            return
        normalized_text = " ".join(str(text or _url_slug_text(absolute)).split())
        links.append({"url": absolute, "text": normalized_text})
        seen.add(absolute)

    for anchor in soup.find_all("a", href=True):
        add_link(str(anchor.get("href") or ""), anchor.get_text(" ", strip=True))
    for element in soup.find_all(True):
        element_text = " ".join(element.get_text(" ", strip=True).split())
        for attribute, value in dict(element.attrs).items():
            attribute_name = str(attribute or "").lower()
            if attribute_name not in attribute_candidates and not attribute_name.endswith(("href", "url")):
                continue
            values = value if isinstance(value, list) else [value]
            for item in values:
                if not _string_looks_like_link_candidate(str(item or "")):
                    continue
                add_link(str(item or ""), element_text)
    for script in soup.find_all("script"):
        for candidate in _extract_script_links(page_url, script.get_text(" ", strip=True)):
            add_link(candidate["url"], candidate.get("text", ""))
    return links


def _q4_public_feed_links(page_url: str, page_html: str) -> list[dict[str, str]]:
    lowered = str(page_html or "").lower()
    has_events_module = ".events(" in lowered or "module-event" in lowered
    has_presentations_module = ".presentations(" in lowered or "module-presentation" in lowered
    has_financial_reports_module = any(
        token in lowered
        for token in (
            "q4financials(",
            "financialreport.svc/getfinancialreportlist",
            "doccategories:",
            '"doccategories"',
        )
    )
    if "widgets.q4app.com/widgets/q4.api" not in lowered and not has_events_module and not has_presentations_module and not has_financial_reports_module:
        return []

    origin = urlparse(page_url)
    if origin.scheme not in {"http", "https"} or not origin.netloc:
        return []
    path = origin.path.lower()
    if any(token in path for token in ("/event-details/", "/news-details/", "/presentation-details/", "/sec-filings-details/")):
        return []
    base_url = f"{origin.scheme}://{origin.netloc}"
    links: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_link(raw_href: str, text: str) -> None:
        absolute = _normalize_href(page_url, html.unescape(str(raw_href or "")).replace("\\/", "/"))
        if not absolute or absolute in seen or not _documentish_page_link(absolute):
            return
        links.append({"url": absolute, "text": " ".join(str(text or _url_slug_text(absolute)).split())})
        seen.add(absolute)

    if has_events_module:
        try:
            event_payload = _fetch_json_response(
                f"{base_url}/feed/Event.svc/GetEventList",
                params={
                    "pageSize": -1,
                    "pageNumber": 0,
                    "tagList": "",
                    "includeTags": "true",
                    "year": -1,
                    "excludeSelection": 1,
                    "eventSelection": 3,
                    "eventDateFilter": 3,
                    "includeFinancialReports": "true",
                    "includePresentations": "true",
                    "includePressReleases": "true",
                    "sortOperator": 1,
                },
            )
        except Exception:
            event_payload = {}
        for item in list(event_payload.get("GetEventListResult") or []):
            title = str(item.get("Title") or "").strip()
            add_link(str(item.get("LinkToDetailPage") or ""), title)
            add_link(str(item.get("WebCastLink") or ""), f"{title} webcast")
            for attachment in list(item.get("Attachments") or []):
                attachment_title = str(attachment.get("Title") or attachment.get("Type") or "").strip()
                add_link(str(attachment.get("Url") or ""), f"{title} {attachment_title}".strip())
            for presentation in list(item.get("EventPresentation") or []):
                presentation_title = str(presentation.get("Title") or "").strip()
                add_link(str(presentation.get("LinkToDetailPage") or ""), presentation_title or title)
                add_link(str(presentation.get("DocumentPath") or ""), f"{presentation_title or title} presentation")
            for release in list(item.get("EventPressRelease") or []):
                release_title = str(release.get("Headline") or release.get("Title") or title).strip()
                add_link(str(release.get("LinkToDetailPage") or ""), release_title)
                add_link(str(release.get("DocumentPath") or ""), f"{release_title} release")

    if has_presentations_module:
        try:
            presentation_payload = _fetch_json_response(
                f"{base_url}/feed/Presentation.svc/GetPresentationList",
                params={
                    "pageSize": -1,
                    "pageNumber": 0,
                    "tagList": "",
                    "includeTags": "true",
                    "year": -1,
                    "excludeSelection": 1,
                    "presentationDateFilter": 3,
                },
            )
        except Exception:
            presentation_payload = {}
        for item in list(presentation_payload.get("GetPresentationListResult") or []):
            title = str(item.get("Title") or "").strip()
            add_link(str(item.get("LinkToDetailPage") or ""), title)
            add_link(str(item.get("DocumentPath") or ""), f"{title} presentation")
            add_link(str(item.get("AudioFile") or ""), f"{title} audio")
            add_link(str(item.get("VideoFile") or ""), f"{title} video")

    if has_financial_reports_module:
        try:
            financial_report_payload = _fetch_json_response(
                f"{base_url}/feed/FinancialReport.svc/GetFinancialReportList",
                params={
                    "pageSize": -1,
                    "pageNumber": 0,
                    "reportTypeId": "",
                    "languageId": 1,
                    "categoryId": "",
                    "year": -1,
                    "excludeSelection": 1,
                },
            )
        except Exception:
            financial_report_payload = {}

        def financial_document_text(report_title: str, category: str, document_title: str) -> str:
            category_hint = {
                "news": "earnings release",
                "webcast": "webcast transcript",
                "presentation": "earnings presentation",
                "annual": "annual report",
                "tenk": "form 10-k",
                "tenq": "form 10-q",
            }.get(category, "")
            generic_titles = {"pdf", "html", "online", "file"}
            cleaned_document_title = " ".join(document_title.split())
            if cleaned_document_title.lower() in generic_titles:
                cleaned_document_title = ""
            preferred_suffix = cleaned_document_title or category_hint
            return " ".join(part for part in (report_title, preferred_suffix) if part).strip()

        for item in list(financial_report_payload.get("GetFinancialReportListResult") or []):
            report_title = " ".join(
                str(part or "").strip()
                for part in (item.get("ReportTitle"), item.get("ReportSubType"))
                if str(part or "").strip()
            ).strip()
            for document in list(item.get("Documents") or []):
                category = str(document.get("DocumentCategory") or "").strip().lower()
                document_title = str(document.get("DocumentTitle") or document.get("Title") or "").strip()
                add_link(
                    str(document.get("DocumentPath") or ""),
                    financial_document_text(report_title, category, document_title),
                )
    return links


def _discover_cached_material_sources(
    company_id: str,
    calendar_quarter: str,
    *,
    required_roles: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    root = OFFICIAL_MATERIALS_DIR / company_id / calendar_quarter
    if not root.exists():
        return []
    roles = required_roles or {"earnings_release", "earnings_call", "earnings_presentation", "earnings_commentary"}
    discovered: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for metadata_path in sorted(root.glob("*.json")):
        cached = _read_cached_submissions(metadata_path)
        if not isinstance(cached, dict):
            continue
        role = str(cached.get("role") or "").strip()
        kind = str(cached.get("kind") or "").strip()
        url = str(cached.get("url") or "").strip()
        if role not in roles or kind not in {"official_release", "sec_filing", "call_summary", "presentation"} or not url:
            continue
        if str(cached.get("status") or "").strip() not in {"cached", "fetched"}:
            continue
        text_path = Path(str(cached.get("text_path") or "").strip())
        if not str(text_path) or not text_path.exists():
            continue
        if url in seen_urls:
            continue
        discovered.append(
            {
                "label": str(cached.get("label") or ""),
                "url": url,
                "fetch_url": str(cached.get("fetch_url") or ""),
                "kind": kind,
                "role": role,
                "date": str(cached.get("date") or ""),
            }
        )
        seen_urls.add(url)
    return discovered


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
        for keyword in ("transcript", "prepared remarks", "webcast replay", "call replay", "conference call", "earnings call", "webcast"):
            if keyword in tokens:
                score += 24
        if "event" in tokens:
            score += 4
        if "/events/" in path:
            if any(keyword in tokens for keyword in ("transcript", "prepared remarks", "webcast replay", "call replay", "replay")):
                score += 12
            else:
                score -= 16
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
    if role == "earnings_release" and any(
        keyword in tokens for keyword in ("segment revenue", "segment revenues", "segment result", "segment results")
    ):
        score -= 30
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
    path = urlparse(str(link.get("url") or "")).path.lower()
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
        return any(
            keyword in tokens
            for keyword in ("webcast", "transcript", "earnings call", "conference call", "prepared remarks", "call replay", "webcast replay", "replay")
        )
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
        if any(keyword in tokens for keyword in ("segment revenue", "segment revenues", "segment result", "segment results")):
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


def _ir_related_link_tiebreak(link: dict[str, str], role: str) -> int:
    if role != "earnings_call":
        return 0
    tokens = _document_tokens(link.get("url", ""), link.get("text", ""))
    path = urlparse(str(link.get("url") or "")).path.lower()
    bonus = 0
    if any(token in tokens for token in ("transcript", "prepared remarks", "script")):
        bonus += 30
    if any(token in tokens for token in ("webcast replay", "call replay", "replay")):
        bonus += 16
    if path.endswith(".pdf") and any(token in tokens for token in ("transcript", "prepared remarks")):
        bonus += 10
    if "/event-details/" in path:
        bonus -= 8
    if any(
        token in tokens
        for token in ("already registered", "log in now", "register now", "complete this form to enter the webcast")
    ):
        bonus -= 22
    return bonus


def _ir_label(company: dict[str, Any], role: str, text: str) -> str:
    cleaned = " ".join(str(text or "").split())
    cleaned = re.sub(r"\(opens in new window\)", "", cleaned, flags=re.IGNORECASE).strip(" -–—")
    english_name = str(company.get("english_name") or "").strip()
    lowered_cleaned = cleaned.lower()
    lowered_name = english_name.lower()
    while lowered_name and lowered_cleaned.startswith(f"{lowered_name} "):
        cleaned = cleaned[len(english_name) :].strip()
        lowered_cleaned = cleaned.lower()
    if (
        len(cleaned) > 180
        or any(token in lowered_cleaned for token in ("{", "}", "defaultlocale", "global variables", "//"))
    ):
        cleaned = ""
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


def _wayback_snapshot_url(timestamp: str, original_url: str) -> str:
    return f"https://web.archive.org/web/{timestamp}if_/{original_url}"


def _wayback_cdx_rows(payload: Any) -> list[dict[str, str]]:
    if isinstance(payload, list) and payload:
        header = payload[0] if isinstance(payload[0], list) else []
        rows = payload[1:] if isinstance(header, list) else payload
        if isinstance(header, list) and {"timestamp", "original"} <= {str(item) for item in header}:
            normalized: list[dict[str, str]] = []
            for row in rows:
                if not isinstance(row, list):
                    continue
                item = {
                    str(header[index]): str(row[index] if index < len(row) else "")
                    for index in range(len(header))
                }
                if item.get("timestamp") and item.get("original"):
                    normalized.append(item)
            return normalized
    return []


def _generic_investor_landing_path(url: str) -> bool:
    path = urlparse(str(url or "")).path.lower().rstrip("/")
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


def _wayback_query_patterns(
    ir_url: str,
    *,
    role: str,
    target_years: set[str],
    allowed_quarters: set[int],
) -> list[str]:
    parsed = urlparse(str(ir_url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return []
    origin = f"{parsed.scheme}://{parsed.netloc}"
    trimmed_path = parsed.path.strip("/")
    path_variants = [trimmed_path] if trimmed_path else []
    path_variants.extend(
        [
            f"assets/{trimmed_path}" if trimmed_path else "",
            f"files/{trimmed_path}" if trimmed_path else "",
            f"static-files/{trimmed_path}" if trimmed_path else "",
            f"static/{trimmed_path}" if trimmed_path else "",
            f"media/{trimmed_path}" if trimmed_path else "",
            f"uploads/{trimmed_path}" if trimmed_path else "",
        ]
    )
    if role in {"earnings_call", "earnings_presentation", "earnings_release"}:
        path_variants.extend(
            [
                "assets/investor",
                "assets/investors",
                "files/investor",
                "media/investor",
                "uploads/investor",
            ]
        )
    patterns: list[str] = []
    seen: set[str] = set()
    for path_variant in path_variants or [""]:
        normalized = f"{origin}/{path_variant.strip('/')}/" if path_variant else f"{origin}/"
        if normalized not in seen:
            patterns.append(normalized)
            seen.add(normalized)
    return patterns


def _discover_wayback_sources(
    company: dict[str, Any],
    calendar_quarter: str,
    period_end: str,
    *,
    required_roles: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    ir_url = str(company.get("ir_url") or "").strip()
    if not ir_url:
        return []
    calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(company, calendar_quarter, period_end)
    roles = required_roles or {"earnings_release", "earnings_call", "earnings_presentation"}
    discovered_rows: dict[str, dict[str, str]] = {}
    for role in roles:
        for pattern in _wayback_query_patterns(
            ir_url,
            role=role,
            target_years=target_years,
            allowed_quarters=allowed_quarters,
        ):
            try:
                payload = _fetch_json_response(
                    WAYBACK_CDX_URL,
                    params={
                        "url": pattern,
                        "matchType": "prefix",
                        "output": "json",
                        "fl": "timestamp,original,statuscode,mimetype",
                        "filter": "statuscode:200",
                        "limit": 2000,
                    },
                )
            except Exception:
                continue
            for row in _wayback_cdx_rows(payload):
                original_url = str(row.get("original") or "").strip()
                if not original_url or _generic_investor_landing_path(original_url):
                    continue
                if not _same_domain(ir_url, original_url):
                    continue
                existing = discovered_rows.get(original_url)
                if existing is None or str(row.get("timestamp") or "") > str(existing.get("timestamp") or ""):
                    discovered_rows[original_url] = row

    selected: list[dict[str, Any]] = []
    for role in roles:
        best_source: Optional[dict[str, Any]] = None
        best_key: tuple[int, int, int] = (-10_000, -10_000, -10_000)
        for row in discovered_rows.values():
            original_url = str(row.get("original") or "").strip()
            timestamp = str(row.get("timestamp") or "").strip()
            mimetype = str(row.get("mimetype") or "").lower()
            link = {"url": original_url, "text": _url_slug_text(original_url)}
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
            score += _ir_related_link_tiebreak(link, role)
            if "pdf" in mimetype:
                score += 8
            if role == "earnings_call" and any(token in _document_tokens(original_url) for token in ("transcript", "prepared remarks", "prepared", "script")):
                score += 18
            if role == "earnings_release" and any(token in _document_tokens(original_url) for token in ("annual", "10-k", "20-f")):
                score -= 30
            candidate_key = (
                score,
                1 if "pdf" in mimetype else 0,
                int(timestamp or "0"),
            )
            if candidate_key > best_key:
                best_key = candidate_key
                best_source = {
                    "label": _ir_label(company, role, _url_slug_text(original_url)),
                    "url": original_url,
                    "fetch_url": _wayback_snapshot_url(timestamp, original_url),
                    "kind": _ir_kind_for_role(role),
                    "role": role,
                    "date": f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}" if len(timestamp) >= 8 else "",
                }
        if best_source is not None and best_key[0] >= 34:
            selected.append(best_source)
    return selected


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
    for candidate in _q4_public_feed_links(page_url, html):
        if not any(str(existing.get("url") or "") == str(candidate.get("url") or "") for existing in page_links):
            page_links.append(candidate)
    candidate_roles: list[str] = ["earnings_call", "earnings_presentation"]
    if page_role == "earnings_release":
        candidate_roles = ["earnings_release", "earnings_call", "earnings_presentation"]
    elif page_role == "earnings_call":
        candidate_roles = ["earnings_call", "earnings_presentation"]
    elif page_role == "earnings_presentation":
        candidate_roles = ["earnings_call", "earnings_presentation"]
    for role in candidate_roles:
        best_link: Optional[dict[str, str]] = None
        best_key: tuple[int, int] = (-10_000, -10_000)
        for link in page_links:
            raw_link = {
                "url": link["url"],
                "text": str(link.get("text", "")).strip(),
            }
            if not _ir_related_link_keywords_match(raw_link, role, page_role):
                continue
            link_text = str(link.get("text", "")).strip()
            link_tokens = _document_tokens(link_text, link.get("url", ""))
            descriptive_link_text = (
                len(link_text) >= 32
                or any(
                    token in link_tokens
                    for token in ("transcript", "prepared remarks", "conference call", "earnings call", "presentation", "webcast", "financial results")
                )
            )
            contextual_link = {
                "url": link["url"],
                "text": link_text if descriptive_link_text else f"{page_label} {link_text}".strip(),
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
            tie_break = _ir_related_link_tiebreak(contextual_link, role)
            candidate_key = (score, tie_break)
            if candidate_key > best_key:
                best_key = candidate_key
                best_link = contextual_link
        if best_link is None or best_key[0] < 38:
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


def _expand_related_ir_sources(
    page_sources: list[dict[str, Any]],
    *,
    company: dict[str, Any],
    calendar_terms: set[str],
    fiscal_terms: set[str],
    target_years: set[str],
    allowed_quarters: set[int],
    max_depth: int = 2,
) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    seen_urls = {str(item.get("url") or "") for item in page_sources if str(item.get("url") or "")}
    queue: list[tuple[dict[str, Any], int]] = [
        (dict(item), 0)
        for item in page_sources
        if str(item.get("role") or "") in {"earnings_release", "earnings_call", "earnings_presentation"}
    ]
    while queue:
        page_source, depth = queue.pop(0)
        if depth >= max_depth:
            continue
        related_sources = _discover_related_ir_sources(
            page_source,
            company=company,
            calendar_terms=calendar_terms,
            fiscal_terms=fiscal_terms,
            target_years=target_years,
            allowed_quarters=allowed_quarters,
        )
        for related in related_sources:
            related_url = str(related.get("url") or "")
            if not related_url or related_url in seen_urls:
                continue
            expanded.append(related)
            seen_urls.add(related_url)
            if str(related.get("role") or "") in {"earnings_release", "earnings_call", "earnings_presentation"}:
                queue.append((related, depth + 1))
    return expanded


def _infer_historical_ir_page_sources(
    company: dict[str, Any],
    period_end: str,
    known_links: list[dict[str, str]],
) -> list[dict[str, Any]]:
    ir_url = str(company.get("ir_url") or "").strip()
    fiscal_year, fiscal_quarter = _fiscal_period_reference(company, period_end)
    if not ir_url or fiscal_year is None or fiscal_quarter is None:
        return []

    def replace_fiscal_reference(url: str) -> Optional[str]:
        patterns = (
            re.compile(
                r"(?P<prefix>.*?)(?P<fy>fy)(?P<sep1>[-_/]?)(?P<year>20\d{2})(?P<sep2>[-_/]?)(?P<q>q)(?P<quarter>[1-4])(?P<suffix>.*)",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"(?P<prefix>.*?)(?P<fy>fiscal[-_/]?year)(?P<sep1>[-_/]?)(?P<year>20\d{2})(?P<sep2>[-_/]?)(?P<q>q)(?P<quarter>[1-4])(?P<suffix>.*)",
                flags=re.IGNORECASE,
            ),
        )
        for pattern in patterns:
            match = pattern.fullmatch(url)
            if match is None:
                continue
            fy_text = str(match.group("fy") or "")
            q_text = str(match.group("q") or "")
            return (
                f"{match.group('prefix')}"
                f"{fy_text.upper() if fy_text.isupper() else fy_text}"
                f"{match.group('sep1')}{fiscal_year}{match.group('sep2')}"
                f"{q_text.upper() if q_text.isupper() else q_text}{fiscal_quarter}"
                f"{match.group('suffix')}"
            )
        return None

    inferred: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for link in known_links:
        candidate_url = str(link.get("url") or "").strip()
        if not candidate_url or not _same_domain(ir_url, candidate_url):
            continue
        tokens = _document_tokens(candidate_url, str(link.get("text") or ""))
        if not any(keyword in tokens for keyword in ("earnings", "results", "press release", "webcast", "financial results")):
            continue
        inferred_url = replace_fiscal_reference(candidate_url)
        if not inferred_url or inferred_url == candidate_url or inferred_url in seen_urls:
            continue
        inferred_tokens = _document_tokens(inferred_url, str(link.get("text") or ""))
        if not any(keyword in inferred_tokens for keyword in ("earnings", "results", "press release", "webcast")):
            continue
        inferred.append(
            {
                "label": _ir_label(company, "earnings_release", str(link.get("text") or "")),
                "url": inferred_url,
                "kind": "official_release",
                "role": "earnings_release",
                "date": "",
            }
        )
        seen_urls.add(inferred_url)
    return inferred


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
    cache_is_current = int((cached or {}).get("cache_version") or 0) == IR_DISCOVERY_CACHE_VERSION
    if cached and cache_is_current and not refresh and _cache_file_is_fresh(cache_path, IR_DISCOVERY_TTL_SECONDS):
        payload_sources = list(cached.get("sources") or [])
        if required_roles:
            payload_sources = [item for item in payload_sources if str(item.get("role") or "") in required_roles]
        return payload_sources

    try:
        home_html = _fetch_text(ir_url)
    except Exception:
        return list(cached.get("sources") or []) if cached and cache_is_current else []

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
        page_links = _page_links(page_url, html)
        for candidate in _q4_public_feed_links(page_url, html):
            if not any(str(existing.get("url") or "") == str(candidate.get("url") or "") for existing in page_links):
                page_links.append(candidate)
        for link in page_links:
            if not _same_domain(ir_url, link["url"]) or link["url"] in seen_urls:
                continue
            all_links.append(link)
            seen_urls.add(link["url"])

    calendar_terms, fiscal_terms, target_years, allowed_quarters = _quarter_reference_terms(company, calendar_quarter, period_end)
    roles = required_roles or {"earnings_release", "earnings_call", "earnings_presentation", "earnings_commentary"}
    inferred_page_sources = _infer_historical_ir_page_sources(company, period_end, all_links)
    if inferred_page_sources:
        for page_source in inferred_page_sources:
            inferred_url = str(page_source.get("url") or "")
            if not inferred_url or inferred_url in seen_urls:
                continue
            all_links.append({"url": inferred_url, "text": str(page_source.get("label") or "")})
            seen_urls.add(inferred_url)
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

    expansion_seed_sources: list[dict[str, Any]] = []
    seen_seed_urls: set[str] = set()
    for item in list(inferred_page_sources) + list(discovered):
        candidate_url = str(item.get("url") or "")
        candidate_role = str(item.get("role") or "")
        if not candidate_url or candidate_url in seen_seed_urls:
            continue
        if candidate_role not in {"earnings_release", "earnings_call", "earnings_presentation"}:
            continue
        expansion_seed_sources.append(item)
        seen_seed_urls.add(candidate_url)
    if expansion_seed_sources:
        related_sources = _expand_related_ir_sources(
            expansion_seed_sources,
            company=company,
            calendar_terms=calendar_terms,
            fiscal_terms=fiscal_terms,
            target_years=target_years,
            allowed_quarters=allowed_quarters,
        )
        seen_discovered_urls = {str(item.get("url") or "") for item in discovered}
        for item in related_sources:
            item_role = str(item.get("role") or "")
            item_url = str(item.get("url") or "")
            if item_role not in roles or not item_url or item_url in seen_discovered_urls:
                continue
            discovered.append(item)
            seen_discovered_urls.add(item_url)

    _write_cached_submissions(
        cache_path,
        {
            "cache_version": IR_DISCOVERY_CACHE_VERSION,
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
        payload = str(payload or "").strip()
        if not payload:
            return []
        if payload[:1] in {"{", "["}:
            try:
                json_payload = json.loads(payload)
            except json.JSONDecodeError:
                json_payload = None
            if json_payload is not None:
                extracted_urls: list[str] = []
                stack: list[Any] = [json_payload]
                while stack:
                    node = stack.pop()
                    if isinstance(node, dict):
                        stack.extend(node.values())
                    elif isinstance(node, list):
                        stack.extend(node)
                    elif isinstance(node, str) and _string_looks_like_link_candidate(node):
                        extracted_urls.append(node)
                loc_values = _dedupe_urls([urljoin(target_url, html.unescape(item).replace("\\/", "/")) for item in extracted_urls])
                nested_sitemap_urls = [item for item in loc_values if item.lower().endswith((".xml", ".json"))]
                page_urls = [item for item in loc_values if item not in nested_sitemap_urls]
                if nested_sitemap_urls and not page_urls and depth < 1:
                    nested_links: list[dict[str, str]] = []
                    seen_nested: set[str] = set()
                    for child_url in sorted(nested_sitemap_urls, key=_sitemap_discovery_score, reverse=True)[:8]:
                        for link in _sitemap_links_from_url(child_url, depth + 1):
                            link_url = str(link.get("url") or "")
                            if not link_url or link_url in seen_nested:
                                continue
                            nested_links.append(link)
                            seen_nested.add(link_url)
                    return nested_links
                return [{"url": loc, "text": _url_slug_text(loc)} for loc in page_urls or loc_values]
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
                f"{base_url}/sitemap.json",
                f"{base_url}/sitemap_index.xml",
                f"{base_url}/sitemap-index.xml",
                f"{base_url}/sitemap-index.json",
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
        IR_DISCOVERY_CACHE_VERSION,
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
        if _historical_sec_coverage_is_sufficient(
            resolved,
            period_end,
            prefer_sec_only=prefer_sec_only,
            filing_forms=filing_forms,
        ):
            if missing_roles <= {"earnings_release"}:
                missing_roles = set()
    if missing_roles and not prefer_sec_only:
        notify(0.74, "正在优先复用本地缓存的历史官方材料来源...")
        resolved.extend(
            _discover_cached_material_sources(
                str(company.get("id") or ""),
                calendar_quarter,
                required_roles=missing_roles,
            )
        )
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
        related_sources = _expand_related_ir_sources(
            list(sitemap_sources),
            company=company,
            calendar_terms=calendar_terms,
            fiscal_terms=fiscal_terms,
            target_years=target_years,
            allowed_quarters=allowed_quarters,
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
        related_sources = _expand_related_ir_sources(
            list(ir_sources),
            company=company,
            calendar_terms=calendar_terms,
            fiscal_terms=fiscal_terms,
            target_years=target_years,
            allowed_quarters=allowed_quarters,
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
        notify(0.93, "正在通过 Wayback 补齐历史财报、电话会与演示材料...")
        resolved.extend(
            _discover_wayback_sources(
                company,
                calendar_quarter,
                period_end,
                required_roles=missing_roles,
            )
        )
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
    if missing_roles and not prefer_sec_only:
        notify(0.97, "正在回流已缓存的历史官方材料，补齐缺失来源...")
        resolved.extend(
            _discover_cached_material_sources(
                str(company.get("id") or ""),
                calendar_quarter,
                required_roles=missing_roles,
            )
        )

    merged = _merge_sources(existing_sources, resolved)
    if not refresh:
        RESOLVED_SOURCES_MEMORY_CACHE[cache_key] = copy.deepcopy(merged)
    notify(1.0, f"官方源解析完成，共整理 {len(merged)} 个来源。")
    return merged
