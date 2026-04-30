"""AP7 Scraper.

Scrapes AP7, a "building block in the national pension system's premium
pension component" in Sweden. AP7 publishes a static HTML page with the
fund's holdings; this scraper downloads it, normalizes whitespace, slices
off the page preamble, and runs a single row regex over the rest.
"""

import datetime
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "AP7"
_HOLDINGS_URL = "https://www.ap7.se/english/ap7-equity-fund/"

# The holdings table on the page is preceded by an unrelated narrative;
# this marker is the last non-data text before the rows begin. If AP7
# rewords the page header this string is the first thing to update.
_PREAMBLE_MARKER = "Securities Share Market value Position"

# One row of the holdings table, captured as named groups.
# Notes on the character classes:
#   - issuer: first char is a capital letter or a digit followed by a
#     capital letter (the latter accounts for tickers like "3M");
#   - P_I (price/interest): can carry a scientific-notation exponent;
#   - report_date: day and month allowed as 1 or 2 digits.
_ROW_PATTERN = re.compile(
    r"(?P<issuer>(?:[A-Za-z]|\d[A-Z])[A-Za-z\d /&\-\.)(]+) "
    r"(?P<ownership>[\d,]+\%)[\d ]+"
    r"Market value (?P<value>\d+) "
    r"Position (?P<shares>\d+) "
    r"Currency (?P<currency>[A-Z]{3}) "
    r"Exchange rate (?P<ex_rate>[\d\.]+) "
    r"Price/interest (?P<p_i>[\d\.]+(?:E-\d)?) "
    r"Securities F_Market Value - (?P<sectype>[A-Za-z ]+) "
    r"Datum: (?P<report_date>\d{4}-\d{1,2}-\d{1,2})"
)


@register("ap7")
def scrape_ap7() -> None:
    """Scrape AP7 (Sweden) static holdings HTML and write a TSV under ``data/disclosures/ap7/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    response = requests.get(_HOLDINGS_URL, stream=True)
    html_path = utils.download_file(response, "ap7", today, "html")

    with open(html_path, "rb") as f:
        text = BeautifulSoup(f.read(), "html.parser").get_text()
    text = re.sub(r"\s+", " ", text)

    _, _, body = text.partition(_PREAMBLE_MARKER)
    if not body:
        raise RuntimeError(
            f"AP7: preamble marker {_PREAMBLE_MARKER!r} not found — "
            "the holdings page layout may have changed."
        )

    rows = [
        [
            _PENSION_NAME,
            m["issuer"],
            m["sectype"],
            m["report_date"],
            m["value"],
            m["currency"],
            m["ex_rate"],
            m["shares"],
            m["ownership"],
            _HOLDINGS_URL,
        ]
        for m in _ROW_PATTERN.finditer(body)
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Type",
            "Security - Report Date",
            "Security - Market Value",
            "Security - Market Value - Currency Code",
            "Stock - Exchange",
            "Stock - Number of Shares",
            "Stock - Percent Ownership",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "ap7", today)


if __name__ == "__main__":
    scrape_ap7()
