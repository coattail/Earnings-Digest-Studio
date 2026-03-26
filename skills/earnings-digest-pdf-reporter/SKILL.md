---
name: earnings-digest-pdf-reporter
description: Generate a complete Chinese earnings PDF for a U.S. listed company and quarter from the local earnings-digest-studio project. Use when the user wants a polished PDF report, preview link, or background job from a company name, ticker, or Chinese company name plus a quarter.
---

# Earnings Digest PDF Reporter

Generate a detailed Chinese earnings PDF from the local `earnings-digest-studio` project.

## Use This Skill When

- The user gives a U.S. listed company and a quarter and wants a full PDF report.
- The user wants a local preview link, PDF path, or async report job.
- The user refers to companies naturally in English, Chinese, or by ticker.

## Preferred Path

Use the bundled script first:

```bash
python3 ~/.codex/skills/earnings-digest-pdf-reporter/scripts/create_pdf_report.py \
  --company "NVIDIA" \
  --quarter "Q4 2025"
```

The script:

- resolves company name / Chinese name / ticker automatically
- normalizes quarter formats like `2025Q4`, `Q4 2025`, `2025年第4季度`
- auto-detects the local `earnings-digest-studio` repo root
- generates the report and exports the PDF
- prints JSON with `report_id`, `preview_url`, `pdf_path`, and `diagnostics`

Execution rule for this skill:

- Treat the user's company + quarter as executable input for on-demand generation.
- Do not inspect or mention internal company registries, built-in company lists, or whether the company is "in the library" before running.
- Do not pause to enumerate supported companies; run the script first.
- Only if the script itself cannot resolve the identifier, respond briefly and ask for a clearer English legal name or ticker.

If auto-detection fails, set:

```bash
export EARNINGS_DIGEST_STUDIO_ROOT=/absolute/path/to/Earnings-Digest-Studio
```

## Async Path

If the caller needs background execution or polling, use:

- `POST /skill/report-jobs`
- `GET /skill/report-jobs/{job_id}`

Read [references/api-contract.md](references/api-contract.md) only when you need the exact request/response contract.

## Notes

- Prefer this skill over hand-assembling report payloads.
- This project is an on-demand report generator, not a pre-baked company library workflow.
- If the user asks for the final artifact, return the preview link and absolute PDF path when available.
- When calls fail or degrade, read the returned `diagnostics` first and keep the explanation concise.
