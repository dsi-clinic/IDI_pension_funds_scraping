"""AP7 Scraper.

Scrapes AP7, a "building block in the national pension system's premium
pension component" in Sweden. Downloads static HTML file with requests,
reformats and matches using regular expressions, and saves as TSV. No manual
steps needed unless the website or format changes.
"""

import re

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

from pipeline import utils
from pipeline.registry import register


@register("ap7")
def scrape_ap7() -> None:
    """Scrape AP7 (Sweden) static holdings HTML and write a TSV under ``data/ap7/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # ------------/Download raw HTML/-----------#

    # Get content from URL with requests
    url = "https://www.ap7.se/english/ap7-equity-fund/"
    req = requests.get(url)
    content = bs(req.content, "html.parser")

    # Create folder and html path
    path = utils.create_path("ap7")
    html_path = path / "ap7.html"

    # Copy html info onto new file at specified path with binary
    with open(html_path, "wb") as f:
        for chunk in req.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # -------------/Format and parse text/---------#

    # Extract text from html
    text = content.get_text()
    # Remove excess spaces and newlines (format into 1 line)
    text = re.sub(r"\s+", " ", text)
    # Remove preface (index at the end is to access the (.+) group)
    text = re.findall(
        re.compile("Securities Share Market value Position(.+)"), text
    )[0]

    # Regex pattern. Follows site schema very closely, with information we are actually interested in capturing in named groups.
    # Edge cases: In issuer group, first character is either a capital letter or occasionally a digit, but this group is always preceeded with digits, hence the or statement; In P_I group, some numbers have exponents; In report date group, I don't have access to a report in a single digit day or month, so the {1,2} is there to account for whichever way they format.
    pattern = re.compile(
        r"(?P<issuer>(?:[A-Za-z]|\d[A-Z])[A-Za-z\d /&\-\.)(]+) (?P<ownership>[\d,]+\%)[\d ]+Market value (?P<value>\d+) Position (?P<shares>\d+) Currency (?P<currency>[A-Z]{3}) Exchange rate (?P<ex_rate>[\d\.]+) Price/interest (?P<P_I>[\d\.]+(?:E-\d)?) Securities F_Market Value - (?P<sectype>[A-Za-z ]+) Datum: (?P<report_date>\d{4}-\d{1,2}-\d{1,2})"
    )
    matches = re.findall(pattern, text)  # Find all matches

    # If match, append to entries list in accordance to IDI schema
    entries = []
    for match in matches:
        # Get groups into variables
        (
            issuer,
            ownership,
            value,
            shares,
            currency,
            P_I,
            ex_rate,
            sectype,
            report_date,
        ) = match
        # Order according to schema
        entry = [
            "AP7",
            issuer,
            sectype,
            report_date,
            value,
            currency,
            ex_rate,
            shares,
            ownership,
            url,
        ]
        # Append
        entries.append(entry)

    # -------------/Export/---------#

    # Format column names according to IDI schema
    df = pd.DataFrame(
        entries,
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
    # Export as TSV at path
    utils.export_df(df, "ap7", path)


# ---------/Scrape Locally/---------#
if __name__ == "__main__":
    scrape_ap7()
