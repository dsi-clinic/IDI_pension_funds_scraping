"""Danica Pension Scraper.

Scrapes Danica Pension (Denmark). The reports page lists annual share
registers ("Aktiebog") going back several years; this scraper picks the
most recent one, downloads the PDF, walks each row, and emits a TSV.
"""

import datetime
import re

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Danica Pension"
_HOMEPAGE_URL = "https://danica.dk/en/personal"
_REPORTS_URL = "https://danica.dk/regnskaber/aarsrapporter"
_CURRENCY = "DKK"
_MULTIPLIER = "x1_000_000"

# We walk the year list until we find a row with the share-register link
# ("Aktiebog"). Bound this so a renamed row doesn't loop forever.
_MAX_YEARS_TO_TRY = 10

# One holding row: company name, stake in millions DKK, share percent.
# We only emit the first two columns into the TSV.
_ROW_PATTERN = re.compile(r"^(?P<issuer>.*?)\s+(?P<value>[\d\.]+)\s+[\d,%]+$")
_DATE_PATTERN = re.compile(r"\d{2}-\d{2}-\d{4}")
_TABLE_HEADER_TOKEN = "KAPITALANDELE"  # rows containing this aren't data


def _find_pdf_url() -> str:
    """Locate the most recent Aktiebog (share register) PDF URL.

    Returns:
        Absolute URL of the most recent Danica share-register PDF.

    Raises:
        RuntimeError: If no Aktiebog row is found within
            ``_MAX_YEARS_TO_TRY`` years.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(_HOMEPAGE_URL)
        # The cookie banner is part of the homepage flow; if it doesn't
        # appear in headless mode we can still proceed.
        try:
            page.get_by_role(
                "button", name="OK to Necessary"
            ).click(timeout=5000)
        except Exception:
            pass

        page.goto(_REPORTS_URL)
        years = page.locator("ul[class = 'container']").locator("li").all()

        suffix: str | None = None
        for li in years[:_MAX_YEARS_TO_TRY]:
            try:
                suffix = li.locator(
                    'table tbody tr td:has-text("Aktiebog") + td a'
                ).get_attribute("href", timeout=1000)
            except Exception:
                continue
            if suffix:
                break
        browser.close()

    if not suffix:
        raise RuntimeError(
            "Danica: no Aktiebog (share register) link found in the "
            f"first {_MAX_YEARS_TO_TRY} report years."
        )
    return f"{_REPORTS_URL}{suffix}"


@register("danica")
def scrape_danica() -> None:
    """Scrape Danica Pension and write a TSV under ``data/disclosures/danica/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    pdf_url = _find_pdf_url()
    response = requests.get(pdf_url, stream=True)
    pdf_path = utils.download_file(response, "danica", today, "pdf")

    rows: list[list[str]] = []
    report_date = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for line in (page.extract_text() or "").splitlines():
                date_match = _DATE_PATTERN.search(line)
                if date_match:
                    report_date = date_match.group()
                if _TABLE_HEADER_TOKEN in line:
                    continue
                row = _ROW_PATTERN.match(line)
                if row:
                    rows.append(
                        [
                            _PENSION_NAME,
                            row["issuer"],
                            report_date,
                            row["value"],
                            _MULTIPLIER,
                            _CURRENCY,
                            pdf_url,
                        ]
                    )

    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Report Date URL",
        ],
    )
    utils.export_data(df, "danica", today)


if __name__ == "__main__":
    scrape_danica()
