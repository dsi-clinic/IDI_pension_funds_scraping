"""BPL Pension Scraper.

Scrapes BPL Pensioen, a Dutch pension fund for employees in agriculture
and green energy. Navigates the BPL downloads page with Playwright,
fetches the most recent investment overview PDF, parses each line with a
single row regex, and writes a TSV.
"""

import datetime
import re

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "BPL Pension"
_HOLDINGS_URL = "https://www.bplpensioen.nl/beleggen"
_CURRENCY = "EUR"
_MULTIPLIER = "x1_000"

# One row of the holdings table. Edge cases: issuer names contain
# punctuation; the value column may include internal spaces (e.g.
# "12 345,00") that we strip out before emitting.
_ROW_PATTERN = re.compile(
    r"\n(?P<issuer>[A-Za-z\d /&+\-\.]+) "
    r"(?P<ownership>\d{1,3},\d{2}%) "
    r"(?P<value>[\d\. ]+) "
)


@register("bpl")
def scrape_bpl() -> None:
    """Scrape BPL Pensioen (Netherlands) and write a TSV under ``data/disclosures/bpl/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_HOLDINGS_URL)

        # Cookie banner ("Weigeren" = Refuse) doesn't always appear in
        # headless mode; swallow the timeout when it's missing.
        try:
            page.get_by_role("button", name="Weigeren").click(timeout=5000)
        except Exception:
            pass

        page.get_by_role("button", name="Verslagen en rapportages").click()
        link_button = page.get_by_role("link", name="Beleggingsoverzicht")
        pdf_path = utils.get_pdf("bpl", today, page, link_button, browser)

    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)
        text = "".join((p.extract_text() or "") for p in pdf.pages)

    rows = [
        [
            _PENSION_NAME,
            m["issuer"],
            report_date,
            m["value"].replace(" ", ""),
            _MULTIPLIER,
            _CURRENCY,
            m["ownership"],
            _HOLDINGS_URL,
        ]
        for m in _ROW_PATTERN.finditer(text)
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Stock - Percent Ownership",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "bpl", today)


if __name__ == "__main__":
    scrape_bpl()
