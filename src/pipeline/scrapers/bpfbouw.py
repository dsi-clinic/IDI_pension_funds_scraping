"""bpfBOUW Scraper.

Scrapes bpfBOUW, the Dutch construction-industry pension fund.
Navigates the holdings page with Playwright, downloads the most recent
shareholder PDF, walks each row, parses the report date out of the PDF,
and removes country subheadings (which the row regex picks up as if
they were issuers) using a supplemental CSV.
"""

import datetime
import re
from pathlib import Path

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "bpfBOUW"
_HOLDINGS_URL = "https://www.bpfbouw.nl/over-bpfbouw/hoe-we-beleggen"
_CURRENCY = "EUR"

# The PDF reports market values like ``"1.234,5"`` (Dutch thousands +
# one decimal). After we strip both the period and comma the underlying
# unit is ten-times the published value, so the IDI multiplier is x100
# (the original ``x1000`` minus one factor of ten for the decimal).
_MULTIPLIER = "x100"

_TABLE_PATTERN = re.compile(r"^(.+?)\s+([\d.]+,\d+)$", re.MULTILINE)
_DATE_PATTERN = re.compile(r"\b\d{2}\s+[A-Za-z]+\s+\d{4}\b")

# Map Dutch month names to their two-digit form. Avoids the fragile
# ``locale.setlocale(LC_ALL, "nl_NL.UTF-8")`` global mutation that the
# earlier version relied on (that locale isn't installed by default
# everywhere).
_DUTCH_MONTHS: dict[str, str] = {
    "januari": "01",
    "februari": "02",
    "maart": "03",
    "april": "04",
    "mei": "05",
    "juni": "06",
    "juli": "07",
    "augustus": "08",
    "september": "09",
    "oktober": "10",
    "november": "11",
    "december": "12",
}

_COUNTRIES_CSV = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "countries"
    / "dutchcountries.csv"
)


def _parse_dutch_date(date_str: str) -> str:
    """Convert a ``"DD <maand> YYYY"`` Dutch date string to ``YYYY-MM-DD``.

    Args:
        date_str: A date as it appears in the PDF, e.g. ``"31 december 2024"``.

    Returns:
        The same date in ISO format.

    Raises:
        ValueError: If the month name isn't recognized.
    """
    day, month_name, year = date_str.split()
    month = _DUTCH_MONTHS[month_name.lower()]
    return f"{year}-{month}-{int(day):02d}"


@register("bpfbouw")
def scrape_bpfbouw() -> None:
    """Scrape bpfBOUW and write a TSV under ``data/disclosures/bpfbouw/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=500, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_HOLDINGS_URL)
        try:
            page.get_by_role("button", name="Alle cookies accepteren").click(
                timeout=5000
            )
        except Exception:
            pass
        link_button = page.get_by_role("link", name="Aandelenportefeuille")
        pdf_path = utils.get_pdf("bpfbouw", today, page, link_button, browser)

    rows: list[tuple[str, str]] = []
    date_str = ""
    with pdfplumber.open(pdf_path) as pdf:
        for pdf_page in pdf.pages:
            text = pdf_page.extract_text() or ""
            rows.extend(_TABLE_PATTERN.findall(text))
            for date_match in _DATE_PATTERN.findall(text):
                date_str = date_match

    if not date_str:
        raise RuntimeError(
            "bpfBOUW: no report date found in the PDF — the layout may "
            "have changed."
        )
    report_date = _parse_dutch_date(date_str)

    df = pd.DataFrame(
        rows, columns=["Issuer - Name", "Security - Market Value - Amount"]
    )
    df["Security - Report Date"] = report_date
    df["Shareholder - Name"] = _PENSION_NAME
    df["Security - Market Value - Multiplier"] = _MULTIPLIER
    df["Security - Market Value - Currency Code"] = _CURRENCY
    df["Data Source URL"] = _HOLDINGS_URL
    df = df[
        [
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ]
    ]
    df["Security - Market Value - Amount"] = df[
        "Security - Market Value - Amount"
    ].str.replace(r"[,.]", "", regex=True)

    # Country subheadings ("BELGIË", "DUITSLAND", ...) get picked up by
    # the row regex; filter them out using the supplemental list.
    countries = pd.read_csv(_COUNTRIES_CSV).values.flatten().tolist()
    df = df[~df["Issuer - Name"].isin(countries)].reset_index(drop=True)

    utils.export_data(df, "bpfbouw", today)


if __name__ == "__main__":
    scrape_bpfbouw()
