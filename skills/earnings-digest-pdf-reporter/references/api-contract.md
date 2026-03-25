# API Contract

Project root: detected automatically, or set `EARNINGS_DIGEST_STUDIO_ROOT=/path/to/repo`.

## Sync Endpoint

`POST /skill/reports`

Request:

```json
{
  "company": "NVIDIA",
  "quarter": "Q4 2025",
  "history_window": 12,
  "force_refresh": false
}
```

Behavior:

- resolves natural company input to internal `company_id`
- normalizes quarter input to `YYYYQ#`
- creates the report
- attempts PDF export immediately

Response highlights:

- `report_id`
- `company_id`
- `company_name`
- `english_name`
- `ticker`
- `calendar_quarter`
- `preview_url`
- `export_pdf_url`
- `pdf_download_url`
- `pdf_error`
- `diagnostics`
- `payload`

## Async Endpoints

`POST /skill/report-jobs`

Request shape is the same as `/skill/reports`.

Response highlights:

- `job_id`
- `status`
- `progress`
- `stage`
- `message`
- `report_id`
- `preview_url`
- `export_pdf_url`
- `pdf_download_url`
- `pdf_error`
- `diagnostics`

`GET /skill/report-jobs/{job_id}`

Behavior:

- returns job progress
- when the job is completed and a PDF is not stored yet, it attempts PDF export before returning

## Script Path

```bash
python3 ~/.codex/skills/earnings-digest-pdf-reporter/scripts/create_pdf_report.py \
  --company "NVIDIA" \
  --quarter "Q4 2025"
```

Typical JSON output:

```json
{
  "report_id": "...",
  "company_id": "nvidia",
  "calendar_quarter": "2025Q4",
  "preview_url": "/reports/.../preview",
  "pdf_path": "/absolute/path/to/file.pdf",
  "diagnostics": []
}
```
