"""Pensioenfonds Vervoer Scraper.

Scrapes Pensioenfonds Vervoer, the Dutch transport-sector pension fund.
Navigates the holdings page with Playwright, downloads the holdings PDF
via the popup helper, and walks each line — the layout is two
side-by-side mini-tables per page, so each text line yields up to two
holdings.
"""

import datetime
import re

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Vervoer"
_LANDING_URL = (
    "https://www.pfvervoer.nl/over-ons/beleggen/spreiden-van-beleggingen"
)
_CURRENCY = "EUR"
_MULTIPLIER = "x1"

# Each PDF text line carries two holdings side-by-side ("left" and
# "right"). One regex captures both halves; we then keep each half
# independently if its value is numeric (a "-" placeholder means no
# data).
_LINE_PATTERN = re.compile(
    r"(?P<l_issuer>[A-Za-z\s,]+?)\s+(?P<l_value>[\d\.]+(?: [\d\.]+)*,?|-)"
    r"\s+"
    r"(?P<r_issuer>[A-Za-z\s,]+?)\s+(?P<r_value>[\d\.]+(?: [\d\.]+)*,?|-)"
)


@register("vervoer")
def scrape_vervoer() -> None:
    """Scrape Pensioenfonds Vervoer and write a TSV under ``data/disclosures/vervoer/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_LANDING_URL)
        # The "overzicht van onze beleggingen (pdf)" link is the most
        # likely thing to drift if Vervoer redesigns the page.
        link_button = page.get_by_role(
            "link", name="overzicht van onze beleggingen (pdf)"
        )
        pdf_path = utils.get_pdf("vervoer", today, page, link_button, browser)

    rows: list[list[str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)
        for pdf_page in pdf.pages:
            text = pdf_page.extract_text(layout=True, x_density=4) or ""
            for line in text.splitlines():
                match = _LINE_PATTERN.search(line.strip())
                if not match:
                    continue
                for issuer_key, value_key in (
                    ("l_issuer", "l_value"),
                    ("r_issuer", "r_value"),
                ):
                    value = match[value_key]
                    if value == "-":
                        continue
                    rows.append(
                        [
                            _PENSION_NAME,
                            match[issuer_key],
                            report_date,
                            value,
                            _MULTIPLIER,
                            _CURRENCY,
                            _LANDING_URL,
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
            "Data Source URL",
        ],
    )
    utils.export_data(df, "vervoer", today)


if __name__ == "__main__":
    scrape_vervoer()
