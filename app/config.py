from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = ROOT_DIR.parent
DATA_DIR = ROOT_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
EXPORT_DIR = DATA_DIR / "exports"
CACHE_DIR = DATA_DIR / "cache"
INSTITUTIONAL_VIEWS_DIR = CACHE_DIR / "institutional-views"
DB_PATH = DATA_DIR / "earnings_digest.sqlite3"
STATIC_DIR = ROOT_DIR / "app" / "static"
TEMPLATES_DIR = ROOT_DIR / "app" / "templates"

TECH_ANALYSIS_DATA_PATH = WORKSPACE_DIR / "Tech-Analysis" / "data.js"
NVIDIA_SEGMENT_HISTORY_PATH = WORKSPACE_DIR / "nvidia-revenue-chart" / "data" / "nvidia_quarterly_revenue_by_segment.csv"

APP_TITLE = "Earnings Digest Studio"
DEFAULT_HISTORY_WINDOW = 12


def ensure_directories() -> None:
    for path in [DATA_DIR, UPLOAD_DIR, EXPORT_DIR, CACHE_DIR, INSTITUTIONAL_VIEWS_DIR]:
        path.mkdir(parents=True, exist_ok=True)
