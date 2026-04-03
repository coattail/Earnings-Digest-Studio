from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import DB_PATH, ensure_directories


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS uploads (
  id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  content_type TEXT NOT NULL,
  original_path TEXT NOT NULL,
  extracted_text TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
  id TEXT PRIMARY KEY,
  company_id TEXT NOT NULL,
  calendar_quarter TEXT NOT NULL,
  history_window INTEGER NOT NULL,
  structure_dimension_used TEXT NOT NULL,
  coverage_warnings_json TEXT NOT NULL DEFAULT '[]',
  payload_json TEXT NOT NULL,
  html_path TEXT,
  pdf_path TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS report_jobs (
  id TEXT PRIMARY KEY,
  company_id TEXT NOT NULL,
  calendar_quarter TEXT NOT NULL,
  history_window INTEGER NOT NULL,
  manual_transcript_upload_id TEXT,
  force_refresh INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  progress REAL NOT NULL DEFAULT 0,
  stage TEXT NOT NULL DEFAULT 'queued',
  message TEXT NOT NULL DEFAULT '',
  report_id TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


def init_db() -> None:
    ensure_directories()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
        connection.executescript(SCHEMA_SQL)
        report_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(reports)")
        }
        if "html_path" not in report_columns:
            connection.execute("ALTER TABLE reports ADD COLUMN html_path TEXT")
        connection.commit()


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    # Self-heal if the runtime SQLite file was deleted or truncated while the app is running.
    init_db()
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()
