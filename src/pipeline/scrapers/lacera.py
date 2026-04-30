"""LACERA Scraper.

Scrapes LACERA, the Los Angeles County Employees Retirement Association.
The fund's investment-holdings page links to a public PDF; this scraper
finds that link, downloads the PDF, parses the report date off page 2,
and walks each holdings row with a single regex applied per page (rather
than over the whole document, to keep memory bounded).
"""

import datetime
import re

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "LACERA"
_HOLDINGS_URL = "https://www.lacera.gov/accountability/investment-holdings"
_CURRENCY = "USD"
_MULTIPLIER = "x1"

_PDF_LINK_PATTERN = re.compile(r"https://.+public[\w\d_]+\.pdf")
_REPORT_DATE_PATTERN = re.compile(
    r"(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{4})"
)

# One row of the holdings table. Columns are space-padded; spaces are
# matched as ``+`` so that an empty ISIN cell collapses cleanly.
_ROW_PATTERN = re.compile(
    r"\n(?P<fund_name>[A-Z\d \-\.]+) +"
    r"(?P<sectype>EQUITY|FIXED INCOME|CASH|CASH EQUIVALENT|BOND|CORPORATE BOND) +"
    r"(?P<isin>[A-Z\d]+)? +"
    r"(?P<issuer>[A-Z\d\t ,+/\-\.]+) +"
    r"(?P<shares_par>[\d,]+\.\d{3}) +"
    r"(?P<base_price>[\d,]+\.\d+) +"
    r"(?P<value>[\d,]+\.\d+)"
)


def _find_pdf_url() -> str:
    """Fetch the holdings page and return the PDF URL.

    Returns:
        The absolute URL of the most recent investment-holdings PDF.

    Raises:
        RuntimeError: If no matching link is found on the page.
    """
    response = requests.get(_HOLDINGS_URL)
    page_html = str(BeautifulSoup(response.content, "html.parser"))
    match = _PDF_LINK_PATTERN.search(page_html)
    if not match:
        raise RuntimeError(
            "LACERA: investment-holdings PDF link not found — "
            "the page layout may have changed."
        )
    return match.group()


def _extract_report_date(pdf: pdfplumber.PDF) -> str:
    """Pull the report date off page 2 of the PDF as ``YYYY-MM-DD``.

    Args:
        pdf: An open pdfplumber PDF.

    Returns:
        The report date formatted ``YYYY-MM-DD``.

    Raises:
        RuntimeError: If no MM/DD/YYYY date is found on page 2.
    """
    text = pdf.pages[1].extract_text() or ""
    match = _REPORT_DATE_PATTERN.search(text)
    if not match:
        raise RuntimeError(
            "LACERA: report date not found on page 2 — "
            "the PDF layout may have changed."
        )
    return f"{match['year']}-{int(match['month']):02d}-{int(match['day']):02d}"


@register("lacera")
def scrape_lacera() -> None:
    """Scrape LACERA and write a TSV under ``data/disclosures/lacera/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    response = requests.get(_find_pdf_url(), stream=True)
    pdf_path = utils.download_file(response, "lacera", today, "pdf")

    rows: list[list[str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        report_date = _extract_report_date(pdf)
        # Iterate page-by-page and only hold one page's text in memory at
        # a time — the holdings PDF can be hundreds of pages.
        for page in pdf.pages:
            text = page.extract_text(layout=True) or ""
            for m in _ROW_PATTERN.finditer(text):
                rows.append(
                    [
                        _PENSION_NAME,
                        m["issuer"].strip(),
                        m["sectype"].strip(),
                        (m["isin"] or "").strip(),
                        report_date,
                        m["value"].strip(),
                        _MULTIPLIER,
                        _CURRENCY,
                        _HOLDINGS_URL,
                    ]
                )

    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Type",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "lacera", today)


if __name__ == "__main__":
    scrape_lacera()
