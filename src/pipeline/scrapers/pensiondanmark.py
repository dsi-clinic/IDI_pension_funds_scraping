"""PensionDanmark Scraper.

Scrapes PensionDanmark, a Danish labor-market pension fund. The fund
publishes its equity list as a static HTML page; this scraper downloads
the page, normalizes whitespace, pulls the report date out of the page
text, and walks each holding row with a single regex.
"""

import datetime
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "PensionDanmark"
_HOLDINGS_URL = "https://www.pensiondanmark.com/en/investments/equity-list/"
_CURRENCY = "EUR"
_MULTIPLIER = "x1_000_000"

_DATE_PATTERN = re.compile(
    r"(?P<day>\d{1,2}) (?P<month>[A-Za-z]+) (?P<year>\d{4})"
)

# Each holding is rendered across four short lines (country, issuer,
# value, sector); after collapsing runs of 3+ blank lines into one, the
# pattern below matches one row at a time.
_ROW_PATTERN = re.compile(
    r"\n{2}(?P<country>[A-Za-z ]+)\n"
    r"(?P<issuer>[A-Za-z\d\- &,]+)\n"
    r"(?P<value>[\d\.]+)\n"
    r"(?P<sector>[A-Za-z\- &,]+)"
)


@register("pensiondanmark")
def scrape_pension_danmark() -> None:
    """Scrape PensionDanmark and write a TSV under ``data/disclosures/pensiondanmark/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    response = requests.get(_HOLDINGS_URL, stream=True)
    html_path = utils.download_file(response, "pensiondanmark", today, "html")

    with open(html_path, "rb") as f:
        parsed = BeautifulSoup(f.read(), "html.parser")
    text = re.sub(r"\n{3}", "\n", parsed.get_text())

    date_match = _DATE_PATTERN.search(text)
    if not date_match:
        raise RuntimeError(
            "PensionDanmark: report date not found on the page — "
            "the layout may have changed."
        )
    month = utils.convert_month(date_match["month"])
    report_date = (
        f"{date_match['year']}-{month}-{int(date_match['day']):02d}"
    )

    rows = [
        [
            _PENSION_NAME,
            m["issuer"],
            m["country"],
            m["sector"],
            report_date,
            m["value"],
            _MULTIPLIER,
            _CURRENCY,
            _HOLDINGS_URL,
        ]
        for m in _ROW_PATTERN.finditer(text)
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Issuer - Country Name",
            "Issuer - Sector",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "pensiondanmark", today)


if __name__ == "__main__":
    scrape_pension_danmark()
