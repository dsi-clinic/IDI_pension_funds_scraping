"""AP3 Scraper.

Scrapes AP3 (Sweden), which manages Swedish public pension assets. AP3
publishes four per-category PDFs every six months: Swedish stocks, foreign
stocks, fixed income, and private equity. This scraper finds the most
recent PDF for each category, downloads them, and emits one TSV per
category.

Note: AP3 changed the layout of the equity reports at some point. The
Swedish, Foreign, and Fixed PDFs no longer carry a Bloomberg ticker
column, the row order is now ``<currency> <type> <name> ...``, and section
types are uppercase ("EQUITY") instead of mixed case ("Equity"). The
patterns and parsers in this module reflect the *current* format
(retrieved 2026-04-30); re-measure them if AP3 changes the layout again.
"""

import datetime
import re
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "AP3"
_REPORTS_URL = (
    "https://www.ap3.se/en/forvaltning/ap3s-portfolj/ap3s-vardepapper"
)

# AP3 publishes new reports every six months. If neither half-year is up
# yet for the current year, walk backwards a few years; bounded so a
# permanent URL change can't loop forever.
_MAX_YEARS_BACK = 5

_CATEGORIES = ("swedish", "foreign", "fixed", "private")


def _link_pattern(category: str, year: int) -> re.Pattern[str]:
    """Build a regex that matches a category PDF URL for ``year``.

    The original scraper used a character class (``[december0-9-]``) by
    mistake, which matches any sequence of those letters and digits
    rather than the literal word "december".

    Args:
        category: One of the names in ``_CATEGORIES``.
        year: Four-digit year to match in the URL.

    Returns:
        Compiled, case-insensitive ``re.Pattern`` for that (category, year).
    """
    return re.compile(
        rf".+{category}.+(?:december|june)[-_\d]*{year}\.pdf",
        re.IGNORECASE,
    )


# --- Row regexes ------------------------------------------------------------
#
# Each Swedish, Fixed, and Private PDF is laid out so that text-extracted
# rows are unambiguous: numeric fields use comma-thousands or no separator
# at all, so a single row regex per file works.
#
# Foreign is different — it uses *space*-separated thousands (European
# style). On a text dump that makes the boundary between the "Units" and
# "Value" columns ambiguous, so Foreign uses word-position-based parsing
# below instead of a row regex.

_SWEDISH_PATTERN = re.compile(
    r"^(?P<currency>[A-Z]{3}) "
    r"(?P<sectype>EQUITY|FUND EQ) "
    r"(?P<issuer>.+?) "
    r"(?P<units>\d+) "
    r"(?P<value>\d{1,3}(?:,\d{3})*) "
    r"(?P<ownership>\d+\.\d+) "
    r"(?P<voting>\d+\.\d+) "
    r"(?P<isin>[A-Z0-9]{12})$",
    re.MULTILINE,
)

_FIXED_PATTERN = re.compile(
    r"^(?P<currency>[A-Z]{3}) "
    r"(?P<sectype>Corporates|Governments & Sovereigns|Mortgages & Agencies|FUND FI|Bond) "
    r"(?P<issuer>.+?) "
    r"(?P<value>\d{1,3}(?:,\d{3})*) "
    r"(?P<maturity>\d{1,2}/\d{1,2}/\d{4}) "
    r"(?P<isin>[A-Z0-9]{12})$",
    re.MULTILINE,
)

# TODO(student): the value group below is fixed at exactly two digits
# (\d{2}). For a private-equity market value that almost certainly drops
# real rows. Verify against a few rows of the actual PDF and widen to
# \d{1,3}(?:,\d{3})* (matching the other patterns) if appropriate.
_PRIVATE_PATTERN = re.compile(
    r"(?P<issuer>[A-Za-z\-\.\d\| /&,()]+) "
    r"(?P<currency>[A-Z]{3}) "
    r"(?P<value>\d{2}) "
    r"(?P<vintage_year>\d{4})"
)

# --- Foreign-specific column geometry ---------------------------------------
#
# The Foreign report's columns are right-aligned and share an x-band, so
# we group words by their y-coordinate (one row = one shared y) and assign
# each word to a column based on x. Boundaries derived from the header
# row's word positions on a recent report; re-measure if the layout shifts.
_FOREIGN_COLUMNS: tuple[tuple[float, float, str], ...] = (
    (0, 100, "currency"),
    (100, 170, "sectype"),
    (170, 410, "issuer"),
    (410, 450, "units"),
    (450, 520, "value"),
    (520, 580, "ownership"),
    (580, 630, "voting"),
    (630, 1000, "isin"),
)


def _column_of(x: float) -> str | None:
    """Return the column name covering x-coordinate ``x``, or ``None``.

    Args:
        x: Word x0 from pdfplumber.

    Returns:
        Column name from ``_FOREIGN_COLUMNS`` whose ``[x0, x1)`` range
        contains ``x``, or ``None`` when no column covers it.
    """
    for x0, x1, name in _FOREIGN_COLUMNS:
        if x0 <= x < x1:
            return name
    return None


def _foreign_rows(pdf_path: Path) -> Iterator[dict[str, str]]:
    """Yield one dict per data row in the Foreign holdings PDF.

    Words on the page are grouped by their (rounded) top y-coordinate so
    that one shared y maps to one row, then each word is bucketed into a
    column by its x-coordinate. Rows that don't look like holdings (e.g.
    the report header, the column header) are filtered out.

    Args:
        pdf_path: Path to the downloaded Foreign PDF.

    Yields:
        ``dict[column_name -> joined_text]`` for each holdings row.
    """
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            lines: dict[int, list[dict]] = defaultdict(list)
            for word in page.extract_words():
                lines[round(word["top"])].append(word)
            for top in sorted(lines):
                cells: dict[str, list[str]] = defaultdict(list)
                for word in sorted(lines[top], key=lambda w: w["x0"]):
                    column = _column_of(word["x0"])
                    if column is not None:
                        cells[column].append(word["text"])
                row = {col: " ".join(parts) for col, parts in cells.items()}
                currency = row.get("currency", "")
                isin = row.get("isin", "")
                if (
                    len(currency) == 3
                    and currency.isalpha()
                    and currency.isupper()
                    and len(isin) == 12
                ):
                    yield row


def _find_report_links() -> dict[str, str]:
    """Visit AP3's reports page and return a per-category URL map.

    Returns:
        Mapping from each name in ``_CATEGORIES`` to the most recent
        matching PDF URL.

    Raises:
        RuntimeError: If any category can't be located within
            ``_MAX_YEARS_BACK`` years.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_REPORTS_URL)
        # Cookie banner doesn't always appear in headless mode; if it isn't
        # there the click times out, so swallow that case.
        try:
            page.get_by_role(
                "button", name="Only accept necessary"
            ).click(timeout=5000)
        except Exception:
            pass
        hrefs = [
            link.get_attribute("href")
            for link in page.get_by_role("link").all()
        ]
        browser.close()

    valid_hrefs = [h for h in hrefs if h]

    links: dict[str, str] = {}
    for category in _CATEGORIES:
        year = datetime.date.today().year
        for _ in range(_MAX_YEARS_BACK):
            pattern = _link_pattern(category, year)
            for href in valid_hrefs:
                match = pattern.search(href)
                if match:
                    links[category] = match.group()
                    break
            if category in links:
                break
            year -= 1
        if category not in links:
            raise RuntimeError(
                f"AP3: did not find a {category!r} report within "
                f"{_MAX_YEARS_BACK} years — the URL scheme may have changed."
            )
    return links


def _pdf_text(pdf_path: Path) -> tuple[str, str]:
    """Read a PDF and return its concatenated text plus the report date.

    Args:
        pdf_path: Path to the PDF on disk.

    Returns:
        A tuple ``(full_text, report_date)`` where ``full_text`` is every
        page's text concatenated (empty pages skipped) and ``report_date``
        is pulled from the PDF's ``CreationDate`` metadata.
    """
    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)
        text = "".join((page.extract_text() or "") for page in pdf.pages)
    return text, report_date


def _pdf_report_date(pdf_path: Path) -> str:
    """Return the report date from a PDF's metadata.

    Args:
        pdf_path: Path to the PDF on disk.

    Returns:
        The report date as a ``YYYY-MM-DD`` string.
    """
    with pdfplumber.open(pdf_path) as pdf:
        return utils.get_pdf_date(pdf)


def _parse_swedish(pdf_path: Path) -> pd.DataFrame:
    """Parse the Swedish-stocks PDF into an IDI-shaped DataFrame.

    Args:
        pdf_path: Path to the downloaded Swedish PDF.

    Returns:
        DataFrame with one row per holding, in IDI column order.
    """
    text, report_date = _pdf_text(pdf_path)
    rows = [
        [
            _PENSION_NAME,
            m["issuer"],
            m["sectype"],
            m["isin"],
            report_date,
            m["value"],
            m["currency"],
            "",  # Ticker — current PDF has an empty Bloomberg column.
            m["units"],
            _REPORTS_URL,
        ]
        for m in _SWEDISH_PATTERN.finditer(text)
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Type",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value",
            "Security - Market Value - Currency Code",
            "Stock - Ticker",
            "Stock - Number of Shares",
            "Data Source URL",
        ],
    )


def _parse_foreign(pdf_path: Path) -> pd.DataFrame:
    """Parse the foreign-stocks PDF into an IDI-shaped DataFrame.

    Foreign uses space-separated thousands, so this parser reconstructs
    rows from word x-positions rather than a single row regex (see
    ``_foreign_rows``).

    Args:
        pdf_path: Path to the downloaded foreign PDF.

    Returns:
        DataFrame with one row per holding, in IDI column order.
    """
    report_date = _pdf_report_date(pdf_path)
    rows = [
        [
            _PENSION_NAME,
            row["issuer"],
            row["sectype"],
            row["isin"],
            report_date,
            row["value"],
            row["currency"],
            "",  # Ticker — current PDF has an empty Bloomberg column.
            row["units"],
            _REPORTS_URL,
        ]
        for row in _foreign_rows(pdf_path)
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Type",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value",
            "Security - Market Value - Currency Code",
            "Stock - Ticker",
            "Stock - Number of Shares",
            "Data Source URL",
        ],
    )


def _parse_fixed(pdf_path: Path) -> pd.DataFrame:
    """Parse the fixed-income PDF into an IDI-shaped DataFrame.

    Args:
        pdf_path: Path to the downloaded fixed-income PDF.

    Returns:
        DataFrame with one row per holding, in IDI column order.
    """
    text, report_date = _pdf_text(pdf_path)
    rows = [
        [_PENSION_NAME, m["issuer"], m["sectype"], report_date, m["value"], _REPORTS_URL]
        for m in _FIXED_PATTERN.finditer(text)
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Type",
            "Security - Report Date",
            "Security - Market Value",
            "Data Source URL",
        ],
    )


def _parse_private(pdf_path: Path) -> pd.DataFrame:
    """Parse the private-equity PDF into an IDI-shaped DataFrame.

    Args:
        pdf_path: Path to the downloaded private-equity PDF.

    Returns:
        DataFrame with one row per holding, in IDI column order.
    """
    text, report_date = _pdf_text(pdf_path)
    rows = [
        [
            _PENSION_NAME,
            issuer,
            report_date,
            value,
            "x1_000_000",
            currency,
            vintage_year,
            _REPORTS_URL,
        ]
        for issuer, currency, value, vintage_year in (
            _PRIVATE_PATTERN.findall(text)
        )
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Private Equity - Vintage year",
            "Data Source URL",
        ],
    )


_PARSERS = {
    "swedish": _parse_swedish,
    "foreign": _parse_foreign,
    "fixed": _parse_fixed,
    "private": _parse_private,
}


@register("ap3")
def scrape_ap3() -> None:
    """Scrape AP3 (Sweden) Swedish, foreign, fixed-income, and private holdings into four TSVs."""
    today = datetime.date.today()
    links = _find_report_links()
    for category in _CATEGORIES:
        response = requests.get(links[category], stream=True)
        pdf_path = utils.download_file(
            response, "ap3", today, "pdf", subname=category
        )
        df = _PARSERS[category](pdf_path)
        utils.export_data(df, "ap3", today, subname=category)


if __name__ == "__main__":
    scrape_ap3()
