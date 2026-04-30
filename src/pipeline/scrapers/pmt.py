"""PMT Scraper.

Scrapes PMT (Metal and Technology Pension Fund, Netherlands). The fund
publishes its holdings as an HTML table on its public website; this
scraper downloads the page, walks each ``<td>`` to separate company-name
cells from market-value cells, and emits a TSV.
"""

import datetime
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Metal and Technology Pension Fund"
_HOLDINGS_URL = (
    "https://www.pmt.nl/over-pmt/zo-beleggen-we/"
    "waar-beleggen-we-in/aandelen-en-obligaties/#"
)
_CURRENCY = "EUR"

# The HTML table doesn't tag company-name cells distinctly from
# market-value cells; both render as plain ``<td>``. Company-name cells
# carry the Dutch label "Bedrijfsnaam" as a prefix, so that's what we
# split on.
_COMPANY_LABEL = "Bedrijfsnaam"
_NUMBER_PATTERN = re.compile(r"[\d.]+")
_DATE_PATTERN = re.compile(r"\d+\s[A-Za-z]+\s\d{4}")


@register("pmt")
def scrape_pmt() -> None:
    """Scrape PMT (Netherlands) and write a TSV under ``data/disclosures/pmt/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    response = requests.get(_HOLDINGS_URL, stream=True)
    html_path = utils.download_file(response, "pmt", today, "html")

    with open(html_path, "rb") as f:
        parsed = BeautifulSoup(f.read(), "html.parser")

    companies: list[str] = []
    values: list[str] = []
    for table in parsed.find_all("table"):
        body = table.find("tbody")
        if body is None:
            continue
        for cell in body.find_all("td"):
            text = cell.text
            if _COMPANY_LABEL in text:
                companies.append(text.removeprefix(_COMPANY_LABEL).strip())
            else:
                match = _NUMBER_PATTERN.search(text)
                if match:
                    values.append(match.group())

    date_match = _DATE_PATTERN.search(parsed.text)
    if not date_match:
        raise RuntimeError(
            "PMT: report date not found on the page — "
            "the layout may have changed."
        )
    report_date = datetime.datetime.strptime(
        date_match.group(), "%d %B %Y"
    ).strftime("%Y-%m-%d")

    rows = [
        [
            _PENSION_NAME,
            issuer,
            report_date,
            value,
            _CURRENCY,
            _HOLDINGS_URL,
        ]
        for issuer, value in zip(companies, values, strict=False)
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "pmt", today)


if __name__ == "__main__":
    scrape_pmt()
