"""AP4 Scraper.

Scrapes AP4, the fourth Swedish National Pension fund. Navigates the
holdings page with Playwright to find the most recent ``-listed-`` PDF,
downloads it, and walks each row.

Each holdings row is laid out so that the "No of Shares" and "Fair
Value" columns are visually separated but produce a single ambiguous
text run when extracted normally. We work around it by cropping each
page into a left and right column at a known x-coordinate, extracting
each side's lines, and rejoining the two halves with ``!`` so the row
regex has a stable boundary token between shares and value.

Holdings are split into Swedish (issuer country = ``SE``) and Foreign
TSVs to match the historical ``ap4_swedish`` / ``ap4_foreign`` outputs.
"""

import datetime
import re
from pathlib import Path

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "AP4"
_HOLDINGS_URL = "https://www.ap4.se/en/reports/holdings/"
_BASE_URL = "https://ap4.se"

# AP4 publishes new reports each year. Bound the year walk-back so a
# permanent URL change can't loop forever.
_MAX_YEARS_BACK = 5

# Page x-coordinate that splits "issuer/country/shares" (left) from
# "value/isin/ticker/ownership/voting" (right). Re-measure if AP4
# changes the report template.
_COLUMN_SPLIT = 260

# One holdings row, with ``!`` and ``!!!`` as stitching tokens added by
# ``_stitch_rows``. ISIN, ticker, ownership, and voting are optional —
# AP4 leaves them blank for some rows.
_ROW_PATTERN = re.compile(
    r"(?P<issuer>[A-Z\d][A-Za-z\d \-+&'/]+) "
    r"(?P<country>[A-Z]{2}) "
    r"(?P<shares>[\d ]+)!"
    r"(?P<value>[\d ]+) ?"
    r"(?P<isin>[A-Z\d]+)? ?"
    r"(?P<ticker>[A-Z\d]+ [A-Z]{2})? ?"
    r"(?P<ownership>\d,\d{2})? ?"
    r"(?P<power>\d,\d{2})?!!!"
)


def _find_pdf_url() -> str:
    """Open the holdings page and return the most recent ``-listed-`` PDF URL.

    Returns:
        Absolute URL of the most recent listed-holdings PDF.

    Raises:
        RuntimeError: If no matching link is found within
            ``_MAX_YEARS_BACK`` years.
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_HOLDINGS_URL)
        try:
            page.get_by_role(
                "button", name="Only necessary"
            ).click(timeout=5000)
        except Exception:
            pass

        hrefs = [
            link.get_attribute("href")
            for link in page.get_by_role("link").all()
        ]
        browser.close()

    valid = [h for h in hrefs if h]
    year = datetime.date.today().year
    for _ in range(_MAX_YEARS_BACK):
        pattern = re.compile(rf".+-listed.+{year}.+")
        for href in valid:
            if pattern.search(href):
                return f"{_BASE_URL}{href}"
        year -= 1
    raise RuntimeError(
        f"AP4: no -listed- PDF found within {_MAX_YEARS_BACK} years — "
        "the URL scheme may have changed."
    )


def _stitch_rows(pdf_path: Path) -> str:
    """Crop each page at ``_COLUMN_SPLIT`` and rejoin lines with ``!``/``!!!``.

    Args:
        pdf_path: Path to the downloaded AP4 PDF.

    Returns:
        A single string of all rows concatenated, with ``!`` between the
        left and right halves of each row and ``!!!`` between rows. This
        is the input that ``_ROW_PATTERN`` expects.
    """
    chunks: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            left = page.crop(
                (0, 0, _COLUMN_SPLIT, page.height), strict=False
            ).extract_text_lines(return_chars=False)
            right = page.crop(
                (_COLUMN_SPLIT, 0, page.width, page.height), strict=False
            ).extract_text_lines(return_chars=False)
            for left_line, right_line in zip(left, right, strict=False):
                chunks.append(
                    f"{left_line['text']}!{right_line['text']}!!!"
                )
    return "".join(chunks)


def _frame(rows: list[list[str]]) -> pd.DataFrame:
    """Build the AP4 IDI-shaped DataFrame from collected rows.

    Args:
        rows: One row per holding, in the order emitted by
            ``scrape_ap4``.

    Returns:
        A DataFrame with the IDI column names in canonical order.
    """
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Issuer - Country Name",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value",
            "Stock - Number of Shares",
            "Stock - Percent Ownership",
            "Stock - Percent Voting Power",
            "Data Source URL",
        ],
    )


@register("ap4")
def scrape_ap4() -> None:
    """Scrape AP4 (Sweden) Swedish and Foreign holdings into two TSVs."""
    today = datetime.date.today()
    pdf_url = _find_pdf_url()
    response = requests.get(pdf_url, stream=True)
    pdf_path = utils.download_file(response, "ap4", today, "pdf")

    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)

    text = _stitch_rows(pdf_path)

    swedish: list[list[str]] = []
    foreign: list[list[str]] = []
    for m in _ROW_PATTERN.finditer(text):
        row = [
            _PENSION_NAME,
            m["issuer"],
            m["country"],
            m["isin"] or "",
            report_date,
            m["value"],
            m["shares"],
            m["ownership"] or "",
            m["power"] or "",
            _HOLDINGS_URL,
        ]
        (swedish if m["country"] == "SE" else foreign).append(row)

    utils.export_data(_frame(swedish), "ap4", today, subname="swedish")
    utils.export_data(_frame(foreign), "ap4", today, subname="foreign")


if __name__ == "__main__":
    scrape_ap4()
