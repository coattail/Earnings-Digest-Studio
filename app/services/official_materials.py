from __future__ import annotations

import copy
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

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
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"


def _source_cache_dir(company_id: str, calendar_quarter: str) -> Path:
    ensure_directories()
    target = OFFICIAL_MATERIALS_DIR / company_id / calendar_quarter
    target.mkdir(parents=True, exist_ok=True)
    return target


def _source_key(source: dict[str, Any]) -> str:
    basis = f"{source.get('kind', 'source')}-{source.get('label', '')}-{source.get('url', '')}-{source.get('fetch_url', '')}"
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


def _sec_submission_document_bonus(source: dict[str, Any], filename: str, description: str, document_type: str) -> int:
    role = str(source.get("role") or "")
    kind = str(source.get("kind") or "")
    tokens = _normalize_whitespace(f"{filename} {description} {document_type}").lower()
    if role == "earnings_call" or kind == "call_summary":
        if any(token in tokens for token in ("prepared remarks", "transcript", "conference call", "earnings call", "webcast", "script", "replay")):
            return 4
        if "99.2" in tokens or "ex-99.2" in tokens or "exhibit992" in tokens:
            return 2
    if role == "earnings_commentary":
        if any(token in tokens for token in ("commentary", "supplement", "cfo")):
            return 3
        if "99.2" in tokens or "ex-99.2" in tokens or "exhibit992" in tokens:
            return 1
    if role == "earnings_release" or kind == "official_release":
        if any(token in tokens for token in ("press release", "financial results", "quarterly results", "earnings release")):
            return 3
        if any(token in tokens for token in ("prepared remarks", "transcript", "conference call", "presentation")):
            return -2
    return 0


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
        metadata_bonus += _sec_submission_document_bonus(source, filename, description, document_type)
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
    if "wordprocessingml.document" in lowered_type:
        return ".docx"
    if "presentationml.presentation" in lowered_type:
        return ".pptx"
    if "spreadsheetml.sheet" in lowered_type:
        return ".xlsx"
    if "html" in lowered_type:
        return ".html"
    if "text" in lowered_type or "json" in lowered_type:
        return ".txt"
    return ".bin"


def _ooxml_archive(raw_bytes: bytes) -> Optional[zipfile.ZipFile]:
    try:
        return zipfile.ZipFile(BytesIO(raw_bytes))
    except zipfile.BadZipFile:
        return None


def _ooxml_title(archive: zipfile.ZipFile) -> str:
    try:
        root = ET.fromstring(archive.read("docProps/core.xml"))
    except Exception:
        return ""
    namespace = {"dc": "http://purl.org/dc/elements/1.1/"}
    node = root.find(".//dc:title", namespace)
    return str(node.text or "").strip()[:240] if node is not None else ""


def _docx_to_text(raw_bytes: bytes) -> tuple[str, str]:
    archive = _ooxml_archive(raw_bytes)
    if archive is None:
        return _text_to_text(raw_bytes)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    names = sorted(
        name
        for name in archive.namelist()
        if name == "word/document.xml" or name.startswith("word/header") or name.startswith("word/footer")
    )
    paragraphs: list[str] = []
    for name in names:
        try:
            root = ET.fromstring(archive.read(name))
        except Exception:
            continue
        for paragraph in root.findall(".//w:p", namespace):
            texts = [str(node.text or "") for node in paragraph.findall(".//w:t", namespace) if str(node.text or "")]
            if texts:
                paragraphs.append("".join(texts).strip())
    title = _ooxml_title(archive)
    return title, "\n".join(item for item in paragraphs if item).strip()


def _pptx_to_text(raw_bytes: bytes) -> tuple[str, str]:
    archive = _ooxml_archive(raw_bytes)
    if archive is None:
        return _text_to_text(raw_bytes)
    namespace = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}
    slide_names = sorted(name for name in archive.namelist() if name.startswith("ppt/slides/slide") and name.endswith(".xml"))
    slides: list[str] = []
    for name in slide_names:
        try:
            root = ET.fromstring(archive.read(name))
        except Exception:
            continue
        texts = [str(node.text or "").strip() for node in root.findall(".//a:t", namespace) if str(node.text or "").strip()]
        if texts:
            slides.append("\n".join(texts))
    title = _ooxml_title(archive)
    return title, "\n\n".join(slides).strip()


def _xlsx_to_text(raw_bytes: bytes) -> tuple[str, str]:
    archive = _ooxml_archive(raw_bytes)
    if archive is None:
        return _text_to_text(raw_bytes)
    shared_strings: list[str] = []
    try:
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        namespace = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        for item in root.findall(".//s:si", namespace):
            parts = [str(node.text or "") for node in item.findall(".//s:t", namespace)]
            shared_strings.append("".join(parts))
    except Exception:
        shared_strings = []
    sheet_names = sorted(name for name in archive.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"))
    namespace = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows: list[str] = []
    for name in sheet_names:
        try:
            root = ET.fromstring(archive.read(name))
        except Exception:
            continue
        for row in root.findall(".//s:row", namespace):
            values: list[str] = []
            for cell in row.findall("./s:c", namespace):
                raw_value = str((cell.find("./s:v", namespace) or ET.Element("v")).text or "").strip()
                if not raw_value:
                    continue
                if str(cell.get("t") or "") == "s" and raw_value.isdigit():
                    index = int(raw_value)
                    if 0 <= index < len(shared_strings):
                        values.append(shared_strings[index])
                        continue
                values.append(raw_value)
            if values:
                rows.append("\t".join(values))
    title = _ooxml_title(archive)
    return title, "\n".join(rows).strip()


def _extract_text(raw_bytes: bytes, content_type: str, suffix: str) -> tuple[str, str]:
    lowered_type = (content_type or "").lower()
    if suffix == ".pdf" or "pdf" in lowered_type:
        return _pdf_to_text(raw_bytes)
    if suffix == ".docx" or "wordprocessingml.document" in lowered_type:
        return _docx_to_text(raw_bytes)
    if suffix == ".pptx" or "presentationml.presentation" in lowered_type:
        return _pptx_to_text(raw_bytes)
    if suffix == ".xlsx" or "spreadsheetml.sheet" in lowered_type:
        return _xlsx_to_text(raw_bytes)
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
    lowered_title = str(title or "").lower()
    lowered = f"{lowered_title} {normalized_text.lower()}".strip()
    leading_lowered = f"{lowered_title} {normalized_text[:4000].lower()}".strip()
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
        "just a moment",
        "performing security verification",
        "checking your browser",
        "cloudflare",
    )
    kind = str(source.get("kind") or "").casefold()
    role = str(source.get("role") or "").casefold()
    sec_filing_signal_count = sum(
        1
        for keyword in (
            "form 10-k",
            "form 10-q",
            "form 20-f",
            "annual report",
            "quarterly report",
            "net income",
            "cash flow",
            "geographic area",
            "revenue",
            "three months ended",
            "year ended",
            "balance sheet",
        )
        if keyword in lowered
    )
    for phrase in obvious_error_phrases:
        if phrase in leading_lowered:
            if kind == "sec_filing" and role == "sec_filing" and sec_filing_signal_count >= 3:
                continue
            return f"unusable source page: {phrase}"
    if kind not in {"official_release", "presentation", "call_summary", "sec_filing"} and role not in {
        "earnings_release",
        "earnings_commentary",
        "earnings_presentation",
        "earnings_call",
        "sec_filing",
    }:
        return ""

    if kind == "call_summary" or role == "earnings_call":
        parsed = urlparse(str(source.get("url") or ""))
        generic_home_paths = {"", "/", "/investor", "/investors", "/en-us/investor", "/en-us/investors"}
        if (
            str(title or "").strip().lower() in {"home page", "homepage", "investor relations"}
            or parsed.path.lower().rstrip("/") in generic_home_paths
        ):
            return "generic investor homepage is not a usable earnings call source"

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


def _revalidate_cached_material_quality(
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
    raw_path = Path(str(cached.get("raw_path") or ""))
    quality_issue = _material_text_quality_issue(
        cached,
        title=str(cached.get("title") or ""),
        text=text_path.read_text(encoding="utf-8", errors="ignore"),
        content_type=str(cached.get("content_type") or ""),
        suffix=raw_path.suffix.lower(),
    )
    if not quality_issue:
        if str(cached.get("status") or "") == "error" or str(cached.get("error") or "").strip():
            updated = dict(cached)
            updated["status"] = "cached"
            updated["error"] = ""
            _write_material_meta(meta_path, updated)
            return updated
        return cached
    updated = dict(cached)
    updated["status"] = "error"
    updated["error"] = quality_issue
    _write_material_meta(meta_path, updated)
    return updated


def _write_material_meta(meta_path: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata


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


def _wayback_snapshot_url(timestamp: str, original_url: str) -> str:
    return f"https://web.archive.org/web/{timestamp}if_/{original_url}"


def _discover_wayback_snapshot_url(
    source_url: str,
    *,
    client: Optional[httpx.Client] = None,
) -> str:
    normalized_url = str(source_url or "").strip()
    if not normalized_url:
        return ""
    owns_client = client is None
    http_client = client or httpx.Client(headers=REQUEST_HEADERS, follow_redirects=True, timeout=40.0)
    try:
        response = http_client.get(
            WAYBACK_CDX_URL,
            params={
                "url": normalized_url,
                "output": "json",
                "fl": "timestamp,original,statuscode,mimetype",
                "filter": "statuscode:200",
                "limit": 8,
            },
        )
        response.raise_for_status()
        rows = _wayback_cdx_rows(response.json())
        if not rows:
            return ""
        best_row = max(rows, key=lambda item: str(item.get("timestamp") or ""))
        return _wayback_snapshot_url(str(best_row.get("timestamp") or ""), str(best_row.get("original") or normalized_url))
    except Exception:
        return ""
    finally:
        if owns_client:
            http_client.close()


def _response_looks_like_bot_challenge(response: httpx.Response) -> bool:
    text = str(getattr(response, "text", "") or "")
    lowered = text.lower()
    return any(
        marker in lowered
        for marker in (
            "just a moment",
            "performing security verification",
            "checking your browser",
            "cloudflare",
            "captcha",
            "cf-browser-verification",
        )
    )


def _quality_issue_warrants_wayback_retry(issue: str) -> bool:
    normalized = str(issue or "").lower()
    return any(
        marker in normalized
        for marker in (
            "unusable source page",
            "generic investor homepage",
            "insufficient extracted text",
            "empty extracted text",
        )
    )


def _transport_fetch_url(fetch_url: str) -> str:
    normalized = str(fetch_url or "").strip()
    if normalized.startswith("https://web.archive.org/"):
        return "http://" + normalized[len("https://") :]
    return normalized


def _get_with_transport_fallback(client: httpx.Client, fetch_url: str) -> httpx.Response:
    normalized = str(fetch_url or "").strip()
    try:
        return client.get(normalized)
    except Exception:
        fallback_url = _transport_fetch_url(normalized)
        if fallback_url == normalized:
            raise
        return client.get(fallback_url)


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
        "fetch_url": source.get("fetch_url", ""),
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
        cached = _revalidate_cached_material_quality(dict(cached), meta_path=meta_path)
        cached["status"] = "cached" if str(cached.get("status") or "") != "error" else "error"
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
            canonical_url = str(source.get("url") or "")
            initial_fetch_url = str(source.get("fetch_url") or canonical_url)
            pending_urls = [item for item in [initial_fetch_url] if item]
            attempted_urls: set[str] = set()
            last_error: Optional[Exception] = None
            while pending_urls:
                fetch_url = pending_urls.pop(0)
                if fetch_url in attempted_urls:
                    continue
                attempted_urls.add(fetch_url)
                try:
                    response = _get_with_transport_fallback(client, fetch_url)
                    if fetch_url == canonical_url and _response_looks_like_bot_challenge(response):
                        fallback_fetch_url = _discover_wayback_snapshot_url(canonical_url, client=client)
                        if fallback_fetch_url and fallback_fetch_url not in attempted_urls:
                            notify(0.18, f"检测到站点挑战页，切换 Wayback：{source.get('label', 'Source')}")
                            pending_urls.insert(0, fallback_fetch_url)
                        if response.status_code >= 400:
                            continue
                    response.raise_for_status()
                    raw_bytes = response.content
                    content_type = response.headers.get("content-type", "")
                    suffix = _guess_suffix(fetch_url, content_type)
                    notify(0.42, f"正在提取正文：{source.get('label', 'Source')}")
                    if (
                        suffix == ".txt"
                        and "sec.gov/Archives/edgar/data" in canonical_url
                        and str(source.get("kind") or "") in {"sec_filing", "official_release", "call_summary"}
                    ):
                        title, text = _extract_sec_submission_text(raw_bytes, source)
                    else:
                        title, text = _extract_text(raw_bytes, content_type, suffix)
                    if suffix in {".html", ".htm"} or "html" in content_type.lower():
                        image_sources = _html_image_sources(raw_bytes)
                        if _should_ocr_html(text, image_sources):
                            notify(0.68, f"正在做图片 OCR：{source.get('label', 'Source')}")
                            ocr_text = _html_image_ocr_text(fetch_url, raw_bytes, root, key)
                            if ocr_text:
                                text = f"{text}\n\n{ocr_text}".strip()
                    quality_issue = _material_text_quality_issue(
                        source,
                        title=title,
                        text=text,
                        content_type=content_type,
                        suffix=suffix,
                    )
                    if (
                        quality_issue
                        and fetch_url == canonical_url
                        and _quality_issue_warrants_wayback_retry(quality_issue)
                    ):
                        fallback_fetch_url = str(source.get("fetch_url") or "") or _discover_wayback_snapshot_url(canonical_url, client=client)
                        if fallback_fetch_url and fallback_fetch_url not in attempted_urls:
                            notify(0.24, f"实时页面正文不足，改抓 Wayback 快照：{source.get('label', 'Source')}")
                            pending_urls.insert(0, fallback_fetch_url)
                            continue
                    raw_path = root / f"{key}{suffix}"
                    raw_path.write_bytes(raw_bytes)
                    text_path = root / f"{key}.txt"
                    text_path.write_text(text, encoding="utf-8")
                    status = "fetched"
                    error = ""
                    if quality_issue:
                        status = "error"
                        error = quality_issue
                    metadata = {
                        "label": source.get("label", "Source"),
                        "url": canonical_url,
                        "fetch_url": fetch_url if fetch_url != canonical_url else str(source.get("fetch_url") or ""),
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
                    last_error = exc
                    if fetch_url == canonical_url:
                        fallback_fetch_url = str(source.get("fetch_url") or "") or _discover_wayback_snapshot_url(canonical_url, client=client)
                        if fallback_fetch_url and fallback_fetch_url not in attempted_urls:
                            notify(0.18, f"主站抓取失败，改抓 Wayback 快照：{source.get('label', 'Source')}")
                            pending_urls.insert(0, fallback_fetch_url)
                            continue
            if last_error is not None:
                raise last_error
            raise RuntimeError(f"unable to fetch source: {canonical_url}")
    except Exception as exc:
        metadata = {
            "label": source.get("label", "Source"),
            "url": source.get("url", ""),
            "fetch_url": source.get("fetch_url", ""),
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
                str(source.get("fetch_url") or ""),
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
