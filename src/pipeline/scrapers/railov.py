"""Pensioenfonds Rail + OV Scraper.

Scrapes Pensioenfonds Rail + OV, the Dutch pension fund for the railway
and public-transport sector. Navigates the holdings page with
Playwright, downloads the PDF, parses the table of contents to find the
pages we care about, then walks each row inside that page range.
"""

import datetime
import re

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Rail & OV"
_HOLDINGS_URL = "https://railov.nl/over-ons/beleggen/waarin-beleggen/"
_CURRENCY = "EUR"

# Sections of the report we want; everything else (government bonds,
# narrative pages, etc.) is skipped.
_NEEDED_SECTIONS = (
    "Aandelen ontwikkelde markten",
    "Aandelen opkomende markten",
    "Bedrijfsobligaties investment grade",
    "High Yield obligaties",
)

_TOC_PATTERN = re.compile(r"(\d+)\s+([A-Z][A-Za-z\s,-]+)\s+(\d+)")
_ROW_PATTERN = re.compile(
    r"^(?P<issuer>.+?)\s+€\s*(?P<value>[\d.,]+)\s+(?P<percent>[\d.,]+%)$",
    re.MULTILINE,
)


def _download_pdf(today: datetime.date) -> str:
    """Drive the holdings page with Playwright and download the report PDF.

    Args:
        today: Date stamp for the run directory.

    Returns:
        Local path to the downloaded PDF.
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=500, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_HOLDINGS_URL)
        page.get_by_role("link", name="Overzicht beleggingen").click()
        pdf_url = page.url
        response = requests.get(pdf_url, stream=True)
        pdf_path = utils.download_file(response, "railov", today, "pdf")
        browser.close()
    return pdf_path


def _necessary_pages(toc: pd.DataFrame) -> set[int]:
    """Return the set of page numbers covered by the wanted sections.

    Args:
        toc: DataFrame with ``name``, ``category``, ``page`` columns,
            in the order they appear in the table of contents.

    Returns:
        A set of 1-indexed page numbers that fall within any of the
        ``_NEEDED_SECTIONS`` ranges. The end of each section is the
        page of the next entry, regardless of whether *that* section is
        wanted.
    """
    pages: set[int] = set()
    for i, row in toc.iterrows():
        if row["category"] not in _NEEDED_SECTIONS:
            continue
        start = int(row["page"])
        end = int(toc.iloc[i + 1]["page"]) if i + 1 < len(toc) else start + 1
        pages.update(range(start, end))
    return pages


@register("railov")
def scrape_railov() -> None:
    """Scrape Rail + OV and write a TSV under ``data/disclosures/railov/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    pdf_path = _download_pdf(today)

    with pdfplumber.open(pdf_path) as pdf:
        toc_entries: list[tuple[str, str, str]] = []
        for page in pdf.pages:
            toc_entries.extend(_TOC_PATTERN.findall(page.extract_text() or ""))
        toc = pd.DataFrame(toc_entries, columns=["name", "category", "page"])
        wanted_pages = _necessary_pages(toc)

        report_date = utils.get_pdf_date(pdf)
        rows: list[list[str]] = []
        for page in pdf.pages:
            if page.page_number not in wanted_pages:
                continue
            text = page.extract_text() or ""
            for m in _ROW_PATTERN.finditer(text):
                rows.append(
                    [
                        _PENSION_NAME,
                        m["issuer"],
                        report_date,
                        m["value"],
                        _CURRENCY,
                        _HOLDINGS_URL,
                    ]
                )

    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    df["Security - Market Value - Amount"] = df[
        "Security - Market Value - Amount"
    ].str.replace(",", "", regex=False)
    utils.export_data(df, "railov", today)


if __name__ == "__main__":
    scrape_railov()
