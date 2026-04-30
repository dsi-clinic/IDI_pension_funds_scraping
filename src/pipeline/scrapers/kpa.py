"""KPA Pension Scraper.

Scrapes KPA Pension, a Swedish pension and asset-management group.
Navigates the site with Playwright to find the latest holdings PDF,
downloads it, and extracts each issuer name from the PDF based on the
rendered line height — the entry font is ~9.5pt, distinguishable from
headers and other ornamentation.
"""

import datetime
from urllib.parse import urljoin

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "KPA"
_LANDING_URL = (
    "https://www.kpa.se/om-kpa-pension/vart-hallbarhetsarbete/"
    "ansvarsfulla-investeringar/innehav-och-uteslutna-bolag/"
)

# Holdings rows render as ~9.5pt (bbox height between top and bottom).
# Re-measure if KPA changes the report's body font.
_ENTRY_HEIGHT_MIN = 9.0
_ENTRY_HEIGHT_MAX = 10.0


def _find_pdf_url() -> str:
    """Open the landing page and return the absolute holdings-PDF URL.

    Returns:
        Absolute URL of the most recent holdings PDF.

    Raises:
        RuntimeError: If the "Innehav" link is missing or has no
            ``href``.
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=5, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_LANDING_URL)
        try:
            page.get_by_role("button", name="Avvisa cookies").click(
                timeout=5000
            )
        except Exception:
            pass
        href = page.get_by_role(
            "link", name="Innehav", exact=False
        ).get_attribute("href")
        browser.close()

    if not href:
        raise RuntimeError(
            "KPA: 'Innehav' link not found — the landing page layout "
            "may have changed."
        )
    return urljoin(_LANDING_URL, href)


@register("kpa")
def scrape_kpa() -> None:
    """Scrape KPA Pension and write a TSV under ``data/disclosures/kpa/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    pdf_url = _find_pdf_url()
    response = requests.get(pdf_url, stream=True)
    pdf_path = utils.download_file(response, "kpa", today, "pdf")

    rows: list[list[str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)
        for page in pdf.pages:
            for line in page.extract_text_lines():
                height = line["bottom"] - line["top"]
                if _ENTRY_HEIGHT_MIN < height < _ENTRY_HEIGHT_MAX:
                    rows.append(
                        [
                            _PENSION_NAME,
                            line["text"],
                            report_date,
                            pdf_url,
                        ]
                    )

    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "kpa", today)


if __name__ == "__main__":
    scrape_kpa()
