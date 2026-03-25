from __future__ import annotations

import copy
import json
import os
import re
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from pypdf import PdfReader

from ..config import CACHE_DIR, ensure_directories
from ..utils import now_iso, slugify


REQUEST_HEADERS = {
    "user-agent": "EarningsDigestStudio/0.1 (+local-user)",
    "accept": "text/html,application/pdf,text/plain,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
}

OFFICIAL_MATERIALS_DIR = CACHE_DIR / "official-materials"
DISABLE_FETCH_ENV = "EARNINGS_DIGEST_DISABLE_SOURCE_FETCH"
MaterialProgressCallback = Callable[[float, str], None]
RECENT_MATERIAL_TTL_SECONDS = 12 * 60 * 60
HISTORICAL_MATERIAL_TTL_SECONDS = 45 * 24 * 60 * 60
ERROR_RETRY_TTL_SECONDS = 3 * 60 * 60
HYDRATED_MATERIALS_MEMORY_CACHE: dict[tuple[str, str, tuple[tuple[str, str, str, str, str], ...]], list[dict[str, Any]]] = {}


def _source_cache_dir(company_id: str, calendar_quarter: str) -> Path:
    ensure_directories()
    target = OFFICIAL_MATERIALS_DIR / company_id / calendar_quarter
    target.mkdir(parents=True, exist_ok=True)
    return target


def _source_key(source: dict[str, Any]) -> str:
    basis = f"{source.get('kind', 'source')}-{source.get('label', '')}-{source.get('url', '')}"
    return slugify(basis)[:96]


def _meta_path(root: Path, key: str) -> Path:
    return root / f"{key}.json"


def _normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def _excerpt(text: str, limit: int = 240) -> str:
    normalized = _normalize_whitespace(text)
    if len(normalized) <= limit:
        return normalized
    search_window = normalized[: limit + 1]
    preferred_cutoff = max(
        search_window.rfind(token)
        for token in ("。", "！", "？", "；", ".", "!", "?", ";", "，", ",", " ")
    )
    minimum_boundary = max(36, int(limit * 0.55))
    if preferred_cutoff >= minimum_boundary:
        if search_window[preferred_cutoff] == " ":
            return search_window[:preferred_cutoff].rstrip(" ,.;:，；")
        return search_window[: preferred_cutoff + 1].rstrip(" ,.;:，；")
    return normalized[:limit].rstrip(" ,.;:，；")


def _html_to_text(raw_bytes: bytes) -> tuple[str, str]:
    soup = BeautifulSoup(raw_bytes.decode("utf-8", errors="ignore"), "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    text = soup.get_text("\n", strip=True)
    return title, text


def _pdf_to_text(raw_bytes: bytes) -> tuple[str, str]:
    reader = PdfReader(BytesIO(raw_bytes))
    title = ""
    if reader.metadata and reader.metadata.title:
        title = str(reader.metadata.title)
    blocks: list[str] = []
    for page in reader.pages:
        blocks.append(page.extract_text() or "")
    text = "\n".join(blocks).strip()
    if _should_ocr_pdf(blocks) and "=== page-" not in text:
        ocr_text = _ocr_pdf_bytes(raw_bytes)
        if ocr_text:
            text = f"{text}\n\n{ocr_text}".strip()
    return title, text


def _text_to_text(raw_bytes: bytes) -> tuple[str, str]:
    text = raw_bytes.decode("utf-8", errors="ignore").strip()
    first_line = text.splitlines()[0].strip() if text else ""
    return first_line[:120], text


def _sec_submission_document_blocks(text: str) -> list[dict[str, str]]:
    documents: list[dict[str, str]] = []
    for match in re.finditer(r"<DOCUMENT>(.*?)</DOCUMENT>", text, flags=re.DOTALL | re.IGNORECASE):
        block = match.group(1)
        type_match = re.search(r"<TYPE>([^\n\r<]+)", block, flags=re.IGNORECASE)
        filename_match = re.search(r"<FILENAME>([^\n\r<]+)", block, flags=re.IGNORECASE)
        description_match = re.search(r"<DESCRIPTION>([^\n\r<]+)", block, flags=re.IGNORECASE)
        text_match = re.search(r"<TEXT>\s*(.*)", block, flags=re.DOTALL | re.IGNORECASE)
        if text_match is None:
            continue
        documents.append(
            {
                "type": str(type_match.group(1) if type_match else "").strip().upper(),
                "filename": str(filename_match.group(1) if filename_match else "").strip(),
                "description": str(description_match.group(1) if description_match else "").strip(),
                "text": str(text_match.group(1) or "").strip(),
            }
        )
    return documents


def _sec_submission_preferred_types(source: dict[str, Any]) -> list[str]:
    role = str(source.get("role") or "")
    kind = str(source.get("kind") or "")
    if role == "earnings_release" or kind == "official_release":
        return ["EX-99.1", "EX-99", "99.1", "99.01", "EX-13", "8-K", "6-K"]
    if role == "earnings_call" or kind == "call_summary":
        return ["EX-99.2", "EX-99", "99.2", "8-K", "6-K"]
    if kind == "sec_filing":
        label = str(source.get("label") or "").lower()
        url = str(source.get("url") or "").lower()
        if "6-k" in label or "6-k" in url or "form6k" in url:
            return ["EX-99.1", "99.1", "EX-99.2", "99.2", "EX-99.3", "99.3", "6-K", "20-F", "EX-13", "EX-99"]
    return ["10-Q", "10-K", "6-K", "20-F", "8-K", "EX-13", "EX-99"]


def _extract_sec_submission_text(raw_bytes: bytes, source: dict[str, Any]) -> tuple[str, str]:
    payload = raw_bytes.decode("utf-8", errors="ignore")
    documents = _sec_submission_document_blocks(payload)
    if not documents:
        return _text_to_text(raw_bytes)

    preferred_types = _sec_submission_preferred_types(source)
    normalized_preferred = [item.upper() for item in preferred_types]
    best_document: Optional[dict[str, str]] = None
    best_key: tuple[int, int, int] = (-1, -1, -1)
    for index, document in enumerate(documents):
        document_type = str(document.get("type") or "").upper()
        filename = str(document.get("filename") or "").lower()
        description = str(document.get("description") or "").lower()
        priority = -1
        for rank, candidate in enumerate(normalized_preferred):
            if document_type == candidate or document_type.startswith(candidate):
                priority = len(normalized_preferred) - rank
                break
        if priority < 0:
            if document_type.startswith("EX-99"):
                priority = 1
            elif document_type in {"XML", "GRAPHIC", "ZIP", "JSON", "EX-101.INS", "EX-101.SCH"}:
                priority = -10
            elif filename.endswith((".xml", ".xsd", ".jpg", ".png", ".gif", ".zip", ".xls", ".xlsx")):
                priority = -10
            else:
                priority = 0
        quality = 1 if ("htm" in filename or "<html" in document.get("text", "").lower()) else 0
        metadata_bonus = 1 if "earn" in filename or "result" in filename or "financial" in description else 0
        candidate_key = (priority, quality, metadata_bonus - index)
        if candidate_key > best_key:
            best_key = candidate_key
            best_document = document

    if best_document is None:
        return _text_to_text(raw_bytes)

    document_text = str(best_document.get("text") or "").strip()
    if "<html" in document_text.lower():
        title, text = _html_to_text(document_text.encode("utf-8", errors="ignore"))
    else:
        title, text = _text_to_text(document_text.encode("utf-8", errors="ignore"))
    if not title:
        title = str(best_document.get("filename") or best_document.get("description") or "").strip()
    return title[:240], text


def _guess_suffix(url: str, content_type: str) -> str:
    path = urlparse(url).path
    suffix = Path(path).suffix.lower()
    if suffix in {".html", ".htm", ".pdf", ".txt"}:
        return suffix
    lowered_type = (content_type or "").lower()
    if "pdf" in lowered_type:
        return ".pdf"
    if "html" in lowered_type:
        return ".html"
    if "text" in lowered_type or "json" in lowered_type:
        return ".txt"
    return ".bin"


def _extract_text(raw_bytes: bytes, content_type: str, suffix: str) -> tuple[str, str]:
    lowered_type = (content_type or "").lower()
    if suffix == ".pdf" or "pdf" in lowered_type:
        return _pdf_to_text(raw_bytes)
    if suffix in {".html", ".htm"} or "html" in lowered_type:
        return _html_to_text(raw_bytes)
    return _text_to_text(raw_bytes)


def _html_image_sources(raw_bytes: bytes) -> list[str]:
    soup = BeautifulSoup(raw_bytes.decode("utf-8", errors="ignore"), "html.parser")
    sources: list[str] = []
    for image in soup.find_all("img", src=True):
        src = str(image.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        if src not in sources:
            sources.append(src)
    return sources


def _should_ocr_html(text: str, image_sources: list[str]) -> bool:
    if len(image_sources) < 3:
        return False
    normalized_length = len(_normalize_whitespace(text))
    if len(image_sources) >= 8 and normalized_length / max(len(image_sources), 1) < 1300:
        return True
    return normalized_length < max(6000, len(image_sources) * 420)


def _ocr_script_path() -> Path:
    return Path(__file__).resolve().parents[2] / "scripts" / "ocr_images.swift"


def _pdf_ocr_script_path() -> Path:
    return Path(__file__).resolve().parents[2] / "scripts" / "ocr_pdf.swift"


def _ocr_image_paths(image_paths: list[Path]) -> str:
    script_path = _ocr_script_path()
    if not image_paths or shutil.which("swift") is None or not script_path.exists():
        return ""
    try:
        result = subprocess.run(
            ["swift", str(script_path), *[str(path) for path in image_paths]],
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
    except Exception:
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _should_ocr_pdf(page_blocks: list[str]) -> bool:
    if len(page_blocks) < 3:
        return False
    normalized_lengths = [len(_normalize_whitespace(block)) for block in page_blocks]
    sparse_pages = sum(length < 80 for length in normalized_lengths)
    total_length = sum(normalized_lengths)
    if sparse_pages >= 4:
        return True
    if sparse_pages >= max(3, len(page_blocks) // 4) and total_length < 18000:
        return True
    return False


def _pdf_ocr_text_looks_usable(text: str) -> bool:
    if "=== page-" not in text:
        return False
    ocr_part = text[text.find("=== page-") :]
    keyword_hits = sum(
        keyword in ocr_part
        for keyword in (
            "Revenue",
            "Margin",
            "Application",
            "Technology",
            "Communication",
            "Computer",
            "Consumer",
            "Balance Sheets",
            "Cash Flows",
            "Guidance",
        )
    )
    weird_chars = sum(not (char.isascii() or char.isspace()) for char in ocr_part)
    return keyword_hits >= 3 and weird_chars <= max(40, len(ocr_part) // 90)


def _ocr_pdf_bytes(raw_bytes: bytes, *, page_limit: int = 18) -> str:
    script_path = _pdf_ocr_script_path()
    if shutil.which("swift") is None or not script_path.exists():
        return ""
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as handle:
            handle.write(raw_bytes)
            temp_path = Path(handle.name)
        result = subprocess.run(
            ["swift", str(script_path), str(temp_path), str(page_limit)],
            capture_output=True,
            text=True,
            timeout=240,
            check=False,
        )
    except Exception:
        return ""
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _html_image_ocr_text(
    source_url: str,
    raw_bytes: bytes,
    root: Path,
    key: str,
) -> str:
    image_sources = _html_image_sources(raw_bytes)
    if not image_sources:
        return ""
    image_paths: list[Path] = []
    try:
        with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
            for index, image_source in enumerate(image_sources[:24]):
                image_url = urljoin(source_url, image_source)
                response = client.get(image_url)
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                suffix = _guess_suffix(image_url, content_type)
                if suffix not in {".jpg", ".jpeg", ".png", ".gif", ".bin"} and "image" not in content_type.lower():
                    continue
                image_path = root / f"{key}-asset-{index + 1:02d}{suffix if suffix != '.bin' else '.jpg'}"
                image_path.write_bytes(response.content)
                image_paths.append(image_path)
    except Exception:
        return ""
    return _ocr_image_paths(image_paths)


def _read_cached_material(meta_path: Path) -> Optional[dict[str, Any]]:
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        try:
            meta_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_iso_date(value: str) -> Optional[date]:
    try:
        return date.fromisoformat(str(value or ""))
    except ValueError:
        return None


def _material_cache_ttl_seconds(source: dict[str, Any], cached: dict[str, Any]) -> int:
    if str(cached.get("status") or "") == "error":
        return ERROR_RETRY_TTL_SECONDS
    source_date = _parse_iso_date(str(source.get("date") or cached.get("date") or ""))
    if source_date is not None:
        age_days = abs((date.today() - source_date).days)
        if age_days <= 180:
            return RECENT_MATERIAL_TTL_SECONDS
    return HISTORICAL_MATERIAL_TTL_SECONDS


def _material_cache_is_fresh(source: dict[str, Any], cached: dict[str, Any]) -> bool:
    kind = str(cached.get("kind") or source.get("kind") or "").casefold()
    text_length = int(cached.get("text_length") or 0)
    # Avoid reusing stale near-empty textual caches (common in transient fetch failures).
    if kind in {"official_release", "presentation", "sec_filing"} and text_length < 120:
        return False
    fetched_at = _parse_iso_datetime(str(cached.get("fetched_at") or ""))
    if fetched_at is None:
        return False
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    return age_seconds <= _material_cache_ttl_seconds(source, cached)


def _material_text_quality_issue(
    source: dict[str, Any],
    *,
    title: str,
    text: str,
    content_type: str,
    suffix: str,
) -> str:
    normalized_text = _normalize_whitespace(text)
    lowered = f"{str(title or '').lower()} {normalized_text.lower()}".strip()
    if not normalized_text:
        return "empty extracted text"

    obvious_error_phrases = (
        "access denied",
        "forbidden",
        "page not found",
        "404 not found",
        "request unsuccessful",
        "temporarily unavailable",
        "enable javascript",
        "javascript is disabled",
        "please turn javascript on",
        "are you a robot",
        "captcha",
    )
    for phrase in obvious_error_phrases:
        if phrase in lowered:
            return f"unusable source page: {phrase}"

    kind = str(source.get("kind") or "").casefold()
    role = str(source.get("role") or "").casefold()
    if kind not in {"official_release", "presentation", "call_summary", "sec_filing"} and role not in {
        "earnings_release",
        "earnings_commentary",
        "earnings_presentation",
        "earnings_call",
        "sec_filing",
    }:
        return ""

    if len(normalized_text) >= 180:
        return ""

    # Short HTML/TXT captures for official materials are usually shells, event stubs, or thin wrappers.
    html_or_text = suffix in {".html", ".htm", ".txt"} or "html" in content_type.lower() or "text" in content_type.lower()
    has_material_signal = any(
        keyword in lowered
        for keyword in (
            "revenue",
            "earnings",
            "financial results",
            "quarterly results",
            "guidance",
            "margin",
            "operator",
            "question-and-answer",
            "prepared remarks",
            "cash flow",
            "net income",
            "conference call",
            "webcast replay",
        )
    )
    if html_or_text and not has_material_signal:
        return f"insufficient extracted text ({len(normalized_text)} chars)"
    return ""


def _write_material_meta(meta_path: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata


def _maybe_refresh_cached_excerpt(
    cached: dict[str, Any],
    *,
    meta_path: Path,
) -> dict[str, Any]:
    text_path_value = str(cached.get("text_path") or "")
    if not text_path_value:
        return cached
    text_path = Path(text_path_value)
    if not text_path.exists():
        return cached
    current_text = text_path.read_text(encoding="utf-8", errors="ignore").strip()
    refreshed_excerpt = _excerpt(current_text)
    if str(cached.get("excerpt") or "") == refreshed_excerpt and int(cached.get("text_length") or 0) == len(current_text):
        return cached
    updated = dict(cached)
    updated["excerpt"] = refreshed_excerpt
    updated["text_length"] = len(current_text)
    _write_material_meta(meta_path, updated)
    return updated


def _maybe_upgrade_cached_html_ocr(
    cached: dict[str, Any],
    *,
    root: Path,
    key: str,
    meta_path: Path,
) -> dict[str, Any]:
    if cached.get("html_ocr_reviewed"):
        return cached
    text_path_value = str(cached.get("text_path") or "")
    raw_path_value = str(cached.get("raw_path") or "")
    url = str(cached.get("url") or "")
    if not text_path_value or not raw_path_value:
        return cached

    text_path = Path(text_path_value)
    raw_path = Path(raw_path_value)
    if not text_path.exists() or not raw_path.exists():
        return cached
    if raw_path.suffix.lower() not in {".html", ".htm"}:
        return cached

    current_text = text_path.read_text(encoding="utf-8", errors="ignore")
    if "-asset-" in current_text and "=== " in current_text:
        cached = dict(cached)
        cached["html_ocr_reviewed"] = True
        _write_material_meta(meta_path, cached)
        return cached

    raw_bytes = raw_path.read_bytes()
    image_sources = _html_image_sources(raw_bytes)
    if not _should_ocr_html(current_text, image_sources):
        cached = dict(cached)
        cached["html_ocr_reviewed"] = True
        _write_material_meta(meta_path, cached)
        return cached

    ocr_text = _html_image_ocr_text(url, raw_bytes, root, key)
    if not ocr_text:
        cached = dict(cached)
        cached["html_ocr_reviewed"] = True
        _write_material_meta(meta_path, cached)
        return cached

    upgraded_text = f"{current_text}\n\n{ocr_text}".strip()
    text_path.write_text(upgraded_text, encoding="utf-8")
    upgraded = dict(cached)
    upgraded["excerpt"] = _excerpt(upgraded_text)
    upgraded["text_length"] = len(upgraded_text)
    upgraded["html_ocr_reviewed"] = True
    _write_material_meta(meta_path, upgraded)
    upgraded["status"] = "cached"
    upgraded["from_cache"] = True
    return upgraded


def _maybe_upgrade_cached_pdf_ocr(
    cached: dict[str, Any],
    *,
    meta_path: Path,
) -> dict[str, Any]:
    if cached.get("pdf_ocr_reviewed"):
        return cached
    text_path_value = str(cached.get("text_path") or "")
    raw_path_value = str(cached.get("raw_path") or "")
    if not text_path_value or not raw_path_value:
        return cached

    text_path = Path(text_path_value)
    raw_path = Path(raw_path_value)
    if not text_path.exists() or not raw_path.exists():
        return cached
    if raw_path.suffix.lower() != ".pdf":
        return cached

    current_text = text_path.read_text(encoding="utf-8", errors="ignore")
    if _pdf_ocr_text_looks_usable(current_text):
        cached = dict(cached)
        cached["pdf_ocr_reviewed"] = True
        _write_material_meta(meta_path, cached)
        return cached

    _, upgraded_text = _pdf_to_text(raw_path.read_bytes())
    if not upgraded_text or len(upgraded_text) <= len(current_text):
        cached = dict(cached)
        cached["pdf_ocr_reviewed"] = True
        _write_material_meta(meta_path, cached)
        return cached

    text_path.write_text(upgraded_text, encoding="utf-8")
    upgraded = dict(cached)
    upgraded["excerpt"] = _excerpt(upgraded_text)
    upgraded["text_length"] = len(upgraded_text)
    upgraded["pdf_ocr_reviewed"] = True
    _write_material_meta(meta_path, upgraded)
    upgraded["status"] = "cached"
    upgraded["from_cache"] = True
    return upgraded


def _disabled_material(source: dict[str, Any], cached: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if cached:
        cached = dict(cached)
        cached["status"] = "cached"
        cached["from_cache"] = True
        return cached
    return {
        "label": source.get("label", "Source"),
        "url": source.get("url", ""),
        "kind": source.get("kind", "source"),
        "role": source.get("role", source.get("kind", "source")),
        "date": source.get("date", ""),
        "status": "disabled",
        "from_cache": False,
        "content_type": "",
        "title": "",
        "excerpt": "",
        "text_length": 0,
        "raw_path": "",
        "text_path": "",
        "fetched_at": "",
        "error": f"{DISABLE_FETCH_ENV}=1",
    }


def _fetch_material(
    company_id: str,
    calendar_quarter: str,
    source: dict[str, Any],
    *,
    refresh: bool = False,
    progress_callback: Optional[MaterialProgressCallback] = None,
) -> dict[str, Any]:
    def notify(progress: float, message: str) -> None:
        if progress_callback is None:
            return
        progress_callback(max(0.0, min(1.0, float(progress))), message)

    root = _source_cache_dir(company_id, calendar_quarter)
    key = _source_key(source)
    meta_path = _meta_path(root, key)
    cached = _read_cached_material(meta_path)
    if cached and not refresh and _material_cache_is_fresh(source, cached):
        notify(0.2, f"读取缓存：{source.get('label', 'Source')}")
        cached = _maybe_upgrade_cached_html_ocr(dict(cached), root=root, key=key, meta_path=meta_path)
        cached = _maybe_upgrade_cached_pdf_ocr(dict(cached), meta_path=meta_path)
        cached = _maybe_refresh_cached_excerpt(dict(cached), meta_path=meta_path)
        cached["status"] = "cached"
        cached["from_cache"] = True
        notify(1.0, f"已复用缓存：{source.get('label', 'Source')}")
        return cached
    if cached and not refresh:
        notify(0.14, f"缓存已过期，重新抓取：{source.get('label', 'Source')}")
    if os.environ.get(DISABLE_FETCH_ENV) == "1":
        notify(1.0, f"已跳过抓取：{source.get('label', 'Source')}")
        return _disabled_material(source, cached)

    try:
        notify(0.08, f"开始下载：{source.get('label', 'Source')}")
        with httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0) as client:
            response = client.get(str(source.get("url") or ""))
            response.raise_for_status()
        raw_bytes = response.content
        content_type = response.headers.get("content-type", "")
        suffix = _guess_suffix(str(source.get("url") or ""), content_type)
        raw_path = root / f"{key}{suffix}"
        raw_path.write_bytes(raw_bytes)

        notify(0.42, f"正在提取正文：{source.get('label', 'Source')}")
        if (
            suffix == ".txt"
            and "sec.gov/Archives/edgar/data" in str(source.get("url") or "")
            and str(source.get("kind") or "") in {"sec_filing", "official_release", "call_summary"}
        ):
            title, text = _extract_sec_submission_text(raw_bytes, source)
        else:
            title, text = _extract_text(raw_bytes, content_type, suffix)
        if suffix in {".html", ".htm"} or "html" in content_type.lower():
            image_sources = _html_image_sources(raw_bytes)
            if _should_ocr_html(text, image_sources):
                notify(0.68, f"正在做图片 OCR：{source.get('label', 'Source')}")
                ocr_text = _html_image_ocr_text(str(source.get("url") or ""), raw_bytes, root, key)
                if ocr_text:
                    text = f"{text}\n\n{ocr_text}".strip()
        text_path = root / f"{key}.txt"
        text_path.write_text(text, encoding="utf-8")
        quality_issue = _material_text_quality_issue(
            source,
            title=title,
            text=text,
            content_type=content_type,
            suffix=suffix,
        )
        status = "fetched"
        error = ""
        if quality_issue:
            status = "error"
            error = quality_issue
        metadata = {
            "label": source.get("label", "Source"),
            "url": source.get("url", ""),
            "kind": source.get("kind", "source"),
            "role": source.get("role", source.get("kind", "source")),
            "date": source.get("date", ""),
            "status": status,
            "from_cache": False,
            "content_type": content_type,
            "title": title or source.get("label", ""),
            "excerpt": _excerpt(text),
            "text_length": len(text),
            "raw_path": str(raw_path),
            "text_path": str(text_path),
            "fetched_at": now_iso(),
            "error": error,
        }
        if quality_issue:
            notify(1.0, f"已识别低质量材料：{source.get('label', 'Source')}")
            return _write_material_meta(meta_path, metadata)
        notify(1.0, f"已完成材料处理：{source.get('label', 'Source')}")
        return _write_material_meta(meta_path, metadata)
    except Exception as exc:
        metadata = {
            "label": source.get("label", "Source"),
            "url": source.get("url", ""),
            "kind": source.get("kind", "source"),
            "role": source.get("role", source.get("kind", "source")),
            "date": source.get("date", ""),
            "status": "error",
            "from_cache": False,
            "content_type": "",
            "title": "",
            "excerpt": "",
            "text_length": 0,
            "raw_path": "",
            "text_path": "",
            "fetched_at": now_iso(),
            "error": str(exc),
        }
        notify(1.0, f"抓取失败：{source.get('label', 'Source')}")
        return _write_material_meta(meta_path, metadata)


def hydrate_source_materials(
    company_id: str,
    calendar_quarter: str,
    sources: list[dict[str, Any]],
    *,
    refresh: bool = False,
    progress_callback: Optional[MaterialProgressCallback] = None,
) -> list[dict[str, Any]]:
    cache_key = (
        str(company_id or ""),
        str(calendar_quarter or ""),
        tuple(
            (
                str(source.get("url") or ""),
                str(source.get("label") or ""),
                str(source.get("kind") or ""),
                str(source.get("role") or ""),
                str(source.get("date") or ""),
            )
            for source in sources
            if source.get("url")
        ),
    )
    if not refresh and cache_key in HYDRATED_MATERIALS_MEMORY_CACHE:
        if progress_callback is not None:
            progress_callback(1.0, "已复用进程内官方材料缓存。")
        return copy.deepcopy(HYDRATED_MATERIALS_MEMORY_CACHE[cache_key])
    materials: list[dict[str, Any]] = []
    valid_sources = [source for source in sources if source.get("url")]
    total = len(valid_sources)
    if total == 0 and progress_callback is not None:
        progress_callback(1.0, "当前没有可抓取的官方材料。")
    for index, source in enumerate(valid_sources, start=1):
        base = (index - 1) / total
        span = 1 / total

        def report_source_progress(step_progress: float, message: str, current_base: float = base, current_span: float = span) -> None:
            if progress_callback is None:
                return
            progress_callback(current_base + current_span * max(0.0, min(1.0, float(step_progress))), message)

        if not source.get("url"):
            continue
        materials.append(
            _fetch_material(
                company_id,
                calendar_quarter,
                source,
                refresh=refresh,
                progress_callback=report_source_progress,
            )
        )
    if not refresh:
        HYDRATED_MATERIALS_MEMORY_CACHE[cache_key] = copy.deepcopy(materials)
    return materials
