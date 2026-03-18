# Earnings Digest Studio

Earnings Digest Studio is a web-based research tool for generating high-density quarterly earnings reports as previewable HTML and exportable PDF.

The product is designed for a single workflow:

- choose a listed company
- choose a calendar quarter
- dynamically fetch official materials
- parse KPI, structure, guidance, call highlights, and historical trend data
- generate a polished deep-dive report automatically

## What It Does

- Generates `14-18` page deep-dive earnings reports
- Covers current-quarter KPI, guidance, call themes, risks, catalysts, and evidence cards
- Includes a fixed `12-quarter` growth, structure, and profitability module
- Uses the same HTML template for web preview and PDF export
- Supports optional transcript upload in `PDF / TXT / HTML`
- Prioritizes official sources and uses local cache only to accelerate re-use

## Tech Stack

- FastAPI
- Jinja2 templates
- SVG-based chart rendering
- Playwright for PDF export
- SQLite for local report/job state

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/coattail/Earnings-Digest-Studio.git
cd Earnings-Digest-Studio
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -e .
python -m playwright install chromium
```

### 4. Start the app

```bash
uvicorn app.main:app --reload
```

Then open:

`http://127.0.0.1:8000`

## Run Tests

```bash
python -m unittest discover -s tests
```

## Data Expectations

This repository can dynamically fetch official materials on demand, but it also expects two local structured data sources for baseline historical series and legacy segment support:

- `../Tech-Analysis/data.js`
- `../nvidia-revenue-chart/data/nvidia_quarterly_revenue_by_segment.csv`

These paths are referenced in [app/config.py](app/config.py).

If you do not keep those repositories as sibling folders, you have two options:

1. place the required datasets in equivalent relative locations
2. update the path constants in [app/config.py](app/config.py) to match your local setup

## Generated Data and Cache

The app creates a local `data/` directory at runtime for:

- `data/cache/`: official materials, discovered sources, and other reusable cache
- `data/uploads/`: optional user-uploaded transcripts
- `data/exports/`: generated PDF files
- `data/earnings_digest.sqlite3`: local app database

These files are not required to be committed. They can be deleted and rebuilt.

## Repository Layout

```text
app/
  main.py                  FastAPI entrypoint
  config.py                app paths and shared constants
  templates/               home page and report HTML templates
  static/                  CSS and client-side JS
  services/                parsing, report building, charts, export, and source resolution
scripts/                   helper scripts for OCR and auditing
tests/                     unittest suite
data/                      runtime cache, exports, uploads, and SQLite state
```

## Current Status

The project already supports:

- a web UI for company and quarter selection
- preview and PDF export
- dynamic official-source discovery
- historical 12-quarter analysis
- company-specific parser extensions for multiple large-cap U.S. and ADR names

The codebase is still evolving, especially in:

- historical parser coverage for older quarters
- source normalization across companies
- report-generation speed and cache strategy
- layout refinement for edge-case reports

## Notes

- First-run report generation is slower because official materials may need to be discovered and downloaded.
- Repeated generation is faster because reusable source/material cache is stored under `data/cache/`.
- PDF export depends on Playwright Chromium being installed locally.

