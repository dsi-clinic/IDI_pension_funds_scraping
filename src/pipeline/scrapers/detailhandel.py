"""Pensioenfonds Detailhandel Scraper.

Scrapes Pensioenfonds Detailhandel, the Dutch pension fund for non-food
retail employees and retirees. Navigates to the website with Playwright
to discover the holdings PDF link (the link target is a regular URL —
not a popup — so we can't use ``utils.get_pdf``), downloads the PDF, and
walks each entry. Each PDF page is split at a known x-coordinate so the
left and right columns can be processed in order.

Note: only entry-font lines are kept while iterating, to bound memory.
"""

import datetime
import re

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Pensioenfonds Detailhandel"
_LANDING_URL = (
    "https://pensioenfondsdetailhandel.nl/onze-organisatie/"
    "pensioenzaken/de-beleggingsportefeuille"
)
_CURRENCY = "EUR"

# Each PDF page has two columns of holdings; this is the x-coordinate
# that splits left from right. Re-measure if Detailhandel changes the
# template.
_COLUMN_SPLIT = 416

# Holdings rows are rendered in CenturyGothic. pdfplumber prefixes
# subsetted fonts with a random tag (e.g. "AAAAAG+"), so match the
# suffix.
_ENTRY_FONT_SUFFIX = "CenturyGothic"

# One holding row. Some entries (sanctioned) have no value, hence the
# trailing ``*``.
_ROW_PATTERN = re.compile(r"(?P<issuer>[A-Za-z\d\- '/&]+) (?P<value>[\d\.]*)")


def _find_pdf_url() -> str:
    """Discover the holdings-PDF URL with Playwright.

    Returns:
        The absolute URL of the most recent holdings PDF.

    Raises:
        RuntimeError: If the "Beleggingen per" link is missing or has
            no ``href``.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_LANDING_URL)
        try:
            page.get_by_role("button", name="alles weigeren").click(
                timeout=5000
            )
        except Exception:
            pass
        href = page.get_by_role(
            "link", name="Beleggingen per", exact=False
        ).get_attribute("href")
        browser.close()

    if not href:
        raise RuntimeError(
            "Detailhandel: 'Beleggingen per' link not found — "
            "the landing page layout may have changed."
        )
    return href


@register("detailhandel")
def scrape_detailhandel() -> None:
    """Scrape Pensioenfonds Detailhandel and write a TSV under ``data/disclosures/detailhandel/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    pdf_url = _find_pdf_url()
    response = requests.get(pdf_url, stream=True)
    pdf_path = utils.download_file(response, "detailhandel", today, "pdf")

    rows: list[list[str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)
        for page in pdf.pages:
            left = page.crop((0, 0, _COLUMN_SPLIT, page.height))
            right = page.crop((_COLUMN_SPLIT, 0, page.width, page.height))
            for column in (left, right):
                for line in column.extract_text_lines(return_chars=True):
                    chars = line.get("chars") or []
                    if not chars or not chars[0]["fontname"].endswith(
                        _ENTRY_FONT_SUFFIX
                    ):
                        continue
                    match = _ROW_PATTERN.search(line["text"])
                    if not match:
                        continue
                    value = match["value"]
                    sectype = "Equity" if value else "Sanctionized"
                    rows.append(
                        [
                            _PENSION_NAME,
                            match["issuer"].strip(),
                            sectype,
                            report_date,
                            value,
                            _CURRENCY,
                            pdf_url,
                        ]
                    )

    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Type",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "detailhandel", today)


if __name__ == "__main__":
    scrape_detailhandel()
