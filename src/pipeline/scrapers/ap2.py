"""AP2 Scraper.

Scrapes AP2 (Sweden), which manages buffer-fund pension assets. The reports
page lists per-year PDFs tagged with date and language; this scraper picks
the most recent Swedish and Foreign equity reports, downloads them, and
emits one TSV per language.

Both PDFs lay rows out so that the "Number of Shares" and "Market Value"
columns can't be reliably split with a single regex. The workaround is to
crop each page into two columns at a known x-coordinate, extract lines from
each, and stitch the two halves with ``-`` so the row regex has an explicit
boundary token to anchor on.

Note: re-measure ``_SWEDISH_COLUMN_SPLIT`` / ``_FOREIGN_COLUMN_SPLIT`` and
the page-1 Y offsets if AP2 changes either report's layout.
"""

import datetime
import re
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

# The name of the pension fund.
_PENSION_NAME = "AP2"

# AP2 publishes one report per year at a predictable URL.
_REPORTS_URL = "https://ap2.se/en/asset-management/holdings/"

# AP2 publishes new holdings each year. If the current upload year has no
# matching pair, walk backwards a few years; bounded so a permanent URL
# change can't loop forever.
_MAX_YEARS_BACK = 5

# Per-template page geometry. The Swedish and Foreign reports have different
# column widths and different first-page header heights.
_SWEDISH_COLUMN_SPLIT = 373
_FOREIGN_COLUMN_SPLIT = 456
_SWEDISH_FIRST_PAGE_Y = 114
_FOREIGN_FIRST_PAGE_Y = 132

# Uploads are organized as /uploads/{upload_year}/<...>YYYY_MM_DD_<lang>_<...>.pdf
# where <lang> is "Svenska" or "Utlandska" (with optional first-letter casing).
_LINK_PATTERN = re.compile(
    r".+/uploads/(?P<upload_year>\d{4})/.+"
    r"(?P<date>\d{4}_\d{2}_\d{2})_"
    r"(?P<language>[Ss]venska|[Uu]tlandska)_.+\.pdf"
)

# Row regexes for each report. The "-" between shares and value is the
# stitch token added by ``_stitched_lines``.
# TODO(student): the Swedish output sets a "Security - Market Value - Multiplier"
# of "x1_000" but the Foreign output has no multiplier column at all. Confirm
# against the IDI schema whether this asymmetry is intentional, or whether
# Foreign should also report a multiplier.
_SWEDISH_PATTERN = re.compile(
    r"^(?P<issuer>.+?)\s+"
    r"(?P<isin>SE\d{10})\s+"
    r"(?P<country>[A-Z]{2})\s+"
    r"(?P<shares>[\d\s]+)-"
    r"(?P<value>[\d\s]+)\s+"
    r"(?P<share_capital>[\d,.]+%)\s+"
    r"(?P<voting_capital>[\d,.]+%)$"
)
_FOREIGN_PATTERN = re.compile(
    r"^(?P<issuer>.+?)\s+"
    r"(?P<isin>[A-Z]{2}[A-Z0-9]{9,})\s+"
    r"(?P<country>[A-Z]{2})\s+"
    r"(?P<shares>[\d\s]+)-"
    r"(?P<value>[\d\s]+)$"
)


def _find_report_links() -> tuple[tuple[str, str], tuple[str, str]]:
    """Return ``((swedish_url, swedish_date), (foreign_url, foreign_date))``.

    Dates are normalized to ``YYYY-MM-DD``.

    Raises:
        RuntimeError: If a matching pair isn't found within ``_MAX_YEARS_BACK``.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_REPORTS_URL)
        page.get_by_role("button", name="Deny").click()
        hrefs = [
            link.get_attribute("href")
            for link in page.get_by_role("link").all()
        ]
        browser.close()

    target_year = datetime.date.today().year
    for _ in range(_MAX_YEARS_BACK):
        swedish: tuple[str, str] | None = None
        foreign: tuple[str, str] | None = None
        for href in hrefs:
            if href is None:
                continue
            match = _LINK_PATTERN.match(href)
            if not match or int(match["upload_year"]) != target_year:
                continue
            entry = (href, match["date"].replace("_", "-"))
            language = match["language"].lower()
            if language == "svenska" and swedish is None:
                swedish = entry
            elif language == "utlandska" and foreign is None:
                foreign = entry
            if swedish and foreign:
                return swedish, foreign
        target_year -= 1

    raise RuntimeError(
        f"AP2: did not find both Swedish and Foreign reports within "
        f"{_MAX_YEARS_BACK} years — the URL/upload scheme may have changed."
    )


def _stitched_lines(
    pdf_path: Path, x_split: int, first_page_y_offset: int
) -> Iterator[str]:
    """Yield ``"<left>-<right>"`` strings, one per row of the holdings table.

    The columns "Number of Shares" and "Market Value" can't be reliably
    split with a single regex pass over the raw extraction; cropping to
    two columns at ``x_split`` and rejoining with ``-`` gives the row
    regex a stable token to anchor on.

    Args:
        pdf_path: The path to the PDF report.
        x_split: The horizontal split point between the left and right
            columns.
        first_page_y_offset: The Y offset for the first page.

    Yields:
        ``"<left>-<right>"`` strings, one per row of the holdings table.
    """
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            y = first_page_y_offset if i == 0 else 0
            left = page.crop((0, y, x_split, page.height))
            right = page.crop((x_split, y, page.width, page.height))
            for left_line, right_line in zip(
                left.extract_text_lines(return_chars=False),
                right.extract_text_lines(return_chars=False),
                strict=False,
            ):
                yield f"{left_line['text']}-{right_line['text']}"


def _parse_swedish(pdf_path: Path, report_date: str) -> pd.DataFrame:
    """Parse a Swedish report into a DataFrame.

    Args:
        pdf_path: The path to the PDF report.
        report_date: The report date.

    Returns:
        A DataFrame with one row per shareholder.
    """
    rows = []
    for line in _stitched_lines(
        pdf_path, _SWEDISH_COLUMN_SPLIT, _SWEDISH_FIRST_PAGE_Y
    ):
        m = _SWEDISH_PATTERN.search(line)
        if not m:
            continue
        rows.append(
            [
                _PENSION_NAME,
                m["issuer"],
                m["country"],
                m["isin"],
                report_date,
                m["value"],
                "x1_000",
                m["shares"],
                m["share_capital"],
                m["voting_capital"],
                _REPORTS_URL,
            ]
        )
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Issuer - Country Code",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Stock - Number of Shares",
            "Stock - Percent Ownership",
            "Stock - Percent Voting Power",
            "Data Source URL",
        ],
    )


def _parse_foreign(pdf_path: Path, report_date: str) -> pd.DataFrame:
    """Parse a Foreign report into a DataFrame.

    Args:
        pdf_path: The path to the PDF report.
        report_date: The report date.

    Returns:
        A DataFrame with one row per shareholder.
    """
    rows = []
    for line in _stitched_lines(
        pdf_path, _FOREIGN_COLUMN_SPLIT, _FOREIGN_FIRST_PAGE_Y
    ):
        m = _FOREIGN_PATTERN.search(line)
        if not m:
            continue
        rows.append(
            [
                _PENSION_NAME,
                m["issuer"],
                m["country"],
                m["isin"],
                report_date,
                m["value"],
                m["shares"],
                _REPORTS_URL,
            ]
        )
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Issuer - Country Code",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Stock - Number of Shares",
            "Data Source URL",
        ],
    )


@register("ap2")
def scrape_ap2() -> None:
    """Scrape AP2 (Sweden) Swedish and Foreign equity reports into two TSVs."""
    today = datetime.date.today()
    (swedish_url, swedish_date), (foreign_url, foreign_date) = (
        _find_report_links()
    )

    swedish_pdf_path = utils.download_file(
        requests.get(swedish_url, stream=True),
        "ap2",
        today,
        "pdf",
        subname="swedish",
    )
    foreign_pdf_path = utils.download_file(
        requests.get(foreign_url, stream=True),
        "ap2",
        today,
        "pdf",
        subname="foreign",
    )

    utils.export_data(
        _parse_swedish(swedish_pdf_path, swedish_date),
        "ap2",
        today,
        subname="swedish",
    )
    utils.export_data(
        _parse_foreign(foreign_pdf_path, foreign_date),
        "ap2",
        today,
        subname="foreign",
    )


if __name__ == "__main__":
    scrape_ap2()
