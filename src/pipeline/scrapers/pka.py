"""PKA Scraper.

Scrapes Pensionskassernes Administration a/s, a Danish pension manager.
Navigates the holdings page with Playwright, downloads the holdings PDF
via the popup helper, and walks each row with a single regex.
"""

import datetime
import re

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Pensionskassernes Administration a/s"
_LANDING_URL = (
    "https://pka.dk/ansvarlighed/ansvarlige-investeringer/"
    "politikker-og-rapporter"
)

# One row of the holdings table: issuer name, ISIN, market value,
# percent ownership.
_ROW_PATTERN = re.compile(
    r"^(?P<issuer>.*?)\s+"
    r"(?P<isin>[A-Z]{2}[A-Z0-9]{10})\s+"
    r"(?P<value>[\d.,]+)\s+"
    r"(?P<ownership>[\d.,]+\s*%)$"
)
_DKK_PATTERN = re.compile(r"DKK", re.IGNORECASE)


def _download_pdf(today: datetime.date) -> str:
    """Drive the landing page with Playwright and download the holdings PDF.

    Args:
        today: Date stamp for the run directory.

    Returns:
        Local path to the downloaded PDF.
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_LANDING_URL)
        try:
            page.get_by_role(
                "button", name="Afvis Alle"
            ).click(timeout=5000)
        except Exception:
            pass
        page.get_by_role(
            "button", name="Beholdningslisten", exact=True
        ).click()
        link_button = page.get_by_role(
            "link", name="Se beholdningslisten", exact=True
        )
        pdf_path = utils.get_pdf("pka", today, page, link_button, browser)
    return pdf_path


@register("pka")
def scrape_pka() -> None:
    """Scrape PKA (Denmark) and write a TSV under ``data/disclosures/pka/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    pdf_path = _download_pdf(today)

    rows: list[list[str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)

        first_page_text = pdf.pages[0].extract_text() or ""
        currency_code = "DKK" if _DKK_PATTERN.search(first_page_text) else ""

        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                match = _ROW_PATTERN.match(line.strip())
                if not match:
                    continue
                rows.append(
                    [
                        _PENSION_NAME,
                        match["issuer"],
                        report_date,
                        match["value"],
                        currency_code,
                        match["isin"],
                        match["ownership"],
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
            "Security - Market Value - Currency Code",
            "Security - ISIN",
            "Stock - Percent Ownership",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "pka", today)


if __name__ == "__main__":
    scrape_pka()
