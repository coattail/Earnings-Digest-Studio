from __future__ import annotations

import uuid
from pathlib import Path
from typing import BinaryIO, Optional

from bs4 import BeautifulSoup
from pypdf import PdfReader

from ..config import UPLOAD_DIR
from ..db import get_connection
from ..utils import now_iso


ALLOWED_UPLOAD_SUFFIXES = {".txt", ".html", ".htm", ".pdf"}


def _extract_pdf_text(handle: BinaryIO) -> str:
    reader = PdfReader(handle)
    blocks: list[str] = []
    for page in reader.pages:
        blocks.append(page.extract_text() or "")
    return "\n".join(blocks).strip()


def _extract_html_text(raw_bytes: bytes) -> str:
    soup = BeautifulSoup(raw_bytes.decode("utf-8", errors="ignore"), "html.parser")
    return soup.get_text("\n", strip=True)


def _extract_text(filename: str, raw_bytes: bytes) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix == ".pdf":
        from io import BytesIO

        return _extract_pdf_text(BytesIO(raw_bytes))
    if suffix in {".html", ".htm"}:
        return _extract_html_text(raw_bytes)
    return raw_bytes.decode("utf-8", errors="ignore").strip()


def create_upload(filename: str, content_type: str, raw_bytes: bytes) -> dict[str, str]:
    suffix = Path(filename).suffix.lower()
    if suffix not in ALLOWED_UPLOAD_SUFFIXES:
        raise ValueError("Only PDF, TXT, and HTML uploads are supported.")

    upload_id = uuid.uuid4().hex
    target_path = UPLOAD_DIR / f"{upload_id}{suffix}"
    target_path.write_bytes(raw_bytes)
    extracted_text = _extract_text(filename, raw_bytes)

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO uploads (id, filename, content_type, original_path, extracted_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (upload_id, filename, content_type, str(target_path), extracted_text, now_iso()),
        )

    excerpt = (extracted_text[:180] + "...") if len(extracted_text) > 180 else extracted_text
    return {
        "upload_id": upload_id,
        "filename": filename,
        "content_type": content_type or "application/octet-stream",
        "excerpt": excerpt,
    }


def get_upload(upload_id: str) -> Optional[dict[str, str]]:
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()
    if row is None:
        return None
    return dict(row)
