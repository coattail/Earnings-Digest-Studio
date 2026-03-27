from __future__ import annotations

import asyncio
import os
from pathlib import Path
import tempfile

from playwright.async_api import async_playwright

from ..config import EXPORT_DIR
from ..utils import slugify

PDF_EXPORT_MODE_ENV = "EARNINGS_DIGEST_PDF_EXPORT_MODE"
PDF_PAGE_WIDTH_IN = 13.333
PDF_PAGE_HEIGHT_IN = 7.5


def _pdf_export_mode() -> str:
    raw = str(os.environ.get(PDF_EXPORT_MODE_ENV) or "raster").strip().casefold()
    if raw in {"vector", "raster"}:
        return raw
    return "raster"


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
        await page.set_content(html_content, wait_until="networkidle")
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
        await source_page.set_content(html_content, wait_until="networkidle")
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
            await pdf_page.goto(raster_html_path.as_uri(), wait_until="networkidle")
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
    if mode == "vector":
        return asyncio.run(_render_pdf_vector_async(filename_stem, html_content))
    return asyncio.run(_render_pdf_raster_async(filename_stem, html_content))
