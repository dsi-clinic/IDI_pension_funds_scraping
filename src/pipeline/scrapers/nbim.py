"""Norges Bank Investment Management Scraper.

Scrapes NBIM, the Norwegian sovereign pension fund. NBIM publishes its
investment listing as a CSV at a predictable URL keyed off a half-year
report date. This scraper iterates a small set of candidate report
dates, downloads the most recent one that returns OK, and rewrites it
into the IDI schema.
"""

import datetime

import pandas as pd
import requests

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Norges Bank Investment Management"
_DATA_SOURCE_URL = "https://www.nbim.no/en/investments/all-investments"
_REPORT_URL_TEMPLATE = (
    "https://www.nbim.no/api/investments/v2/report/"
    "?assetType=eq&date={date}&fileType=csv"
)
_CURRENCY = "NOK"

# NBIM publishes one report per half-year. If neither the year-end nor
# the mid-year report is available for the current year, walk back a
# few years; bounded so a permanent URL change can't loop forever.
_MAX_YEARS_BACK = 5
_REPORT_SUFFIXES = ("-12-31", "-06-30")


def _candidate_report_dates() -> list[str]:
    """Return half-year report dates from this year backwards, newest first.

    Returns:
        List of ``YYYY-MM-DD`` strings, e.g. ``["2026-12-31",
        "2026-06-30", "2025-12-31", ...]``.
    """
    this_year = datetime.date.today().year
    return [
        f"{year}{suffix}"
        for year in range(this_year, this_year - _MAX_YEARS_BACK, -1)
        for suffix in _REPORT_SUFFIXES
    ]


def _fetch_latest_report() -> tuple[requests.Response, str]:
    """Return ``(response, report_date)`` for the most recent OK report.

    Returns:
        A streaming ``requests.Response`` plus the matched
        ``YYYY-MM-DD`` report date.

    Raises:
        RuntimeError: If no candidate URL returns ``response.ok``.
    """
    for report_date in _candidate_report_dates():
        url = _REPORT_URL_TEMPLATE.format(date=report_date)
        response = requests.get(url, stream=True)
        if response.ok:
            return response, report_date
    raise RuntimeError(
        f"NBIM: no report found in the last {_MAX_YEARS_BACK} years — "
        "the URL/API may have changed."
    )


@register("nbim")
def scrape_nbim() -> None:
    """Scrape NBIM and write a TSV under ``data/disclosures/nbim/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    response, report_date = _fetch_latest_report()
    csv_path = utils.download_file(response, "nbim", today, "csv")

    # NBIM ships the CSV as UTF-16 with a semicolon delimiter.
    data = pd.read_csv(csv_path, sep=";", encoding="utf-16")
    length = len(data)

    df = pd.DataFrame(
        {
            "Shareholder - Name": [_PENSION_NAME] * length,
            "Issuer - Name": data["Name"],
            "Issuer - Country Name": data["Country"],
            "Security - Report Date": [report_date] * length,
            "Issuer - Sector": data["Industry"],
            "Security - Type": ["Equity"] * length,
            "Security - Market Value - Currency": [_CURRENCY] * length,
            "Security - Market Value - Amount": data["Market Value(NOK)"],
            "Stock - Percent Ownership": data["Ownership"],
            "Stock - Percent Voting Power": data["Voting"],
            "Data Source URL": [_DATA_SOURCE_URL] * length,
        }
    )
    utils.export_data(df, "nbim", today)


if __name__ == "__main__":
    scrape_nbim()
