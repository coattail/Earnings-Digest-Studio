from __future__ import annotations

import asyncio
import os
from pathlib import Path
import re
import tempfile

from playwright.async_api import async_playwright

from ..config import EXPORT_DIR
from ..utils import slugify

PDF_EXPORT_MODE_ENV = "EARNINGS_DIGEST_PDF_EXPORT_MODE"
PDF_EXPORT_PROFILE_ENV = "EARNINGS_DIGEST_PDF_PROFILE"
PDF_PAGE_WIDTH_IN = 13.333
PDF_PAGE_HEIGHT_IN = 7.5
PDF_EXPORT_DEFAULT_MODE = "vector"
PDF_EXPORT_DEFAULT_PROFILE = "fast"
PDF_READY_WAIT_UNTIL = "load"
PDF_EXPORT_PROFILE_SIGNATURES = {
    "fast": "fast-v1",
    "full": "full-v1",
}
FAST_PDF_EXPORT_OVERRIDES = """
<meta name="earnings-digest-pdf-profile" content="fast-v1" />
<style id="earnings-digest-pdf-fast-overrides">
  body.pdf-export-fast,
  body.pdf-export-fast .report-shell {
    background: #ffffff !important;
  }
  body.pdf-export-fast *,
  body.pdf-export-fast *::before,
  body.pdf-export-fast *::after {
    filter: none !important;
    backdrop-filter: none !important;
    text-shadow: none !important;
  }
  body.pdf-export-fast .page,
  body.pdf-export-fast .cover-page,
  body.pdf-export-fast .chart-card,
  body.pdf-export-fast .text-panel,
  body.pdf-export-fast .score-card,
  body.pdf-export-fast .mini-card,
  body.pdf-export-fast .evidence-card,
  body.pdf-export-fast .insight-card,
  body.pdf-export-fast .source-card,
  body.pdf-export-fast .quote-card,
  body.pdf-export-fast .cover-summary-card,
  body.pdf-export-fast .cover-chip,
  body.pdf-export-fast .page-brand-badge,
  body.pdf-export-fast .page-brand-lockup-mini,
  body.pdf-export-fast .report-body .cover-brand-lockup {
    background: #ffffff !important;
    background-image: none !important;
    box-shadow: none !important;
  }
  body.pdf-export-fast .page::before,
  body.pdf-export-fast .page::after,
  body.pdf-export-fast .cover-page::before,
  body.pdf-export-fast .cover-page::after {
    content: none !important;
    display: none !important;
  }
  body.pdf-export-fast .page-head::after {
    height: 1px !important;
    background: #cbd5e1 !important;
  }
</style>
"""
FULL_PDF_EXPORT_META = '<meta name="earnings-digest-pdf-profile" content="full-v1" />'


def _pdf_export_mode() -> str:
    raw = str(os.environ.get(PDF_EXPORT_MODE_ENV) or PDF_EXPORT_DEFAULT_MODE).strip().casefold()
    if raw in {"vector", "raster"}:
        return raw
    return PDF_EXPORT_DEFAULT_MODE


def _pdf_export_profile() -> str:
    raw = str(os.environ.get(PDF_EXPORT_PROFILE_ENV) or PDF_EXPORT_DEFAULT_PROFILE).strip().casefold()
    if raw in PDF_EXPORT_PROFILE_SIGNATURES:
        return raw
    return PDF_EXPORT_DEFAULT_PROFILE


def pdf_export_signature(profile: str | None = None) -> str:
    active_profile = str(profile or _pdf_export_profile()).strip().casefold()
    return PDF_EXPORT_PROFILE_SIGNATURES.get(active_profile, PDF_EXPORT_PROFILE_SIGNATURES[PDF_EXPORT_DEFAULT_PROFILE])


def _inject_body_class(html_content: str, body_class: str) -> str:
    body_pattern = re.compile(r"<body\b([^>]*)>", flags=re.IGNORECASE)

    def _replace(match: re.Match[str]) -> str:
        attrs = match.group(1)
        class_match = re.search(r"""class=(["'])(.*?)\1""", attrs, flags=re.IGNORECASE | re.DOTALL)
        if not class_match:
            return f'<body{attrs} class="{body_class}">'
        current_value = class_match.group(2)
        classes = current_value.split()
        if body_class not in classes:
            classes.append(body_class)
        updated_attrs = f"{attrs[:class_match.start(2)]}{' '.join(classes)}{attrs[class_match.end(2):]}"
        return f"<body{updated_attrs}>"

    if body_pattern.search(html_content):
        return body_pattern.sub(_replace, html_content, count=1)
    return html_content


def _inject_head_markup(html_content: str, markup: str) -> str:
    head_close_pattern = re.compile(r"</head>", flags=re.IGNORECASE)
    if head_close_pattern.search(html_content):
        return head_close_pattern.sub(f"{markup}\n</head>", html_content, count=1)
    return f"{markup}\n{html_content}"


def build_pdf_export_html(html_content: str, *, profile: str | None = None) -> str:
    active_profile = str(profile or _pdf_export_profile()).strip().casefold()
    if active_profile == "full":
        export_html = _inject_body_class(html_content, "pdf-export-full")
        return _inject_head_markup(export_html, FULL_PDF_EXPORT_META)
    export_html = _inject_body_class(html_content, "pdf-export-fast")
    return _inject_head_markup(export_html, FAST_PDF_EXPORT_OVERRIDES)


def _build_raster_pdf_html(image_names: list[str]) -> str:
    pages = "\n".join(
        f'<section class="pdf-page"><img src="{name}" alt="Report page {index + 1}" /></section>'
        for index, name in enumerate(image_names)
    )
    return f"""<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <style>
      @page {{
        size: {PDF_PAGE_WIDTH_IN}in {PDF_PAGE_HEIGHT_IN}in;
        margin: 0;
      }}
      html, body {{
        margin: 0;
        padding: 0;
        background: #ffffff;
      }}
      body {{
        font-size: 0;
      }}
      .pdf-page {{
        width: {PDF_PAGE_WIDTH_IN}in;
        height: {PDF_PAGE_HEIGHT_IN}in;
        page-break-after: always;
        overflow: hidden;
      }}
      .pdf-page:last-child {{
        page-break-after: auto;
      }}
      .pdf-page img {{
        display: block;
        width: 100%;
        height: 100%;
      }}
    </style>
  </head>
  <body>
    {pages}
  </body>
</html>
"""


async def _render_pdf_vector_async(filename_stem: str, html_content: str) -> str:
    output_path = EXPORT_DIR / f"{slugify(filename_stem)}.pdf"
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        page = await browser.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=2)
        await page.set_content(html_content, wait_until=PDF_READY_WAIT_UNTIL)
        await page.pdf(
            path=str(output_path),
            print_background=True,
            prefer_css_page_size=True,
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
        )
        await browser.close()
    return str(output_path)


async def _render_pdf_raster_async(filename_stem: str, html_content: str) -> str:
    output_path = EXPORT_DIR / f"{slugify(filename_stem)}.pdf"
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        source_page = await browser.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=2)
        await source_page.set_content(html_content, wait_until=PDF_READY_WAIT_UNTIL)
        page_locator = source_page.locator(".page")
        page_count = await page_locator.count()
        if page_count <= 0:
            await browser.close()
            return await _render_pdf_vector_async(filename_stem, html_content)
        with tempfile.TemporaryDirectory(prefix="earnings-digest-pdf-") as temp_dir:
            temp_path = Path(temp_dir)
            image_names: list[str] = []
            for index in range(page_count):
                image_name = f"page-{index + 1:02d}.png"
                image_path = temp_path / image_name
                await page_locator.nth(index).screenshot(path=str(image_path), type="png")
                image_names.append(image_name)
            raster_html_path = temp_path / "raster-export.html"
            raster_html_path.write_text(_build_raster_pdf_html(image_names), encoding="utf-8")
            pdf_page = await browser.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=1)
            await pdf_page.goto(raster_html_path.as_uri(), wait_until=PDF_READY_WAIT_UNTIL)
            await pdf_page.pdf(
                path=str(output_path),
                print_background=True,
                prefer_css_page_size=True,
                margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
            )
            await pdf_page.close()
        await source_page.close()
        await browser.close()
    return str(output_path)


def export_html_to_pdf(filename_stem: str, html_content: str) -> str:
    mode = _pdf_export_mode()
    export_html = build_pdf_export_html(html_content)
    if mode == "vector":
        return asyncio.run(_render_pdf_vector_async(filename_stem, export_html))
    return asyncio.run(_render_pdf_raster_async(filename_stem, export_html))
