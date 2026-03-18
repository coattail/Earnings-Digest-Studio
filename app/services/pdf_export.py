from __future__ import annotations

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

from ..config import EXPORT_DIR
from ..utils import slugify


async def _render_pdf_async(filename_stem: str, html_content: str) -> str:
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


def export_html_to_pdf(filename_stem: str, html_content: str) -> str:
    return asyncio.run(_render_pdf_async(filename_stem, html_content))
