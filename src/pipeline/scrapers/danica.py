"""Danica Pension scraper.

This is automated barring structural changes to their website.
it matches the shareholder tracker in most ways, except the ticker and cusip.
"""

import re

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("danica")
def scrape_danica() -> None:
    """Scrape Danica Pension (Denmark) and write a TSV under ``data/danica/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # Create path
    path = utils.create_path("danica")

    # use playwright to get the correct url for the pdf
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        browser.new_context()
        page = browser.new_page()

        # go to homepage#
        page.goto("https://danica.dk/en/personal")

        # accept necessary cookies#
        page.get_by_role("button", name="OK to Necessary").click()

        # go to reports page#
        page.goto("https://danica.dk/regnskaber/aarsrapporter")

        # finds the most recent form containing "Aktibog" in its html tag.
        # this translates to share register.
        # saves the suffix of that url as pdf_url
        frame = page.locator("ul[class = 'container']")

        years_to_try = frame.locator("li").all()

        pdf_url = ""
        i = 0
        while not bool(pdf_url):
            try:
                pdf_url = (
                    years_to_try[i]
                    .locator('table tbody tr td:has-text("Aktiebog") + td a')
                    .get_attribute("href", timeout=1000)
                )
            except Exception:
                i = i + 1
                continue

        browser.close()
        p.stop()

    # goes to the webpage of the pdf
    r = requests.get(f"https://danica.dk/regnskaber/aarsrapporter{pdf_url}")
    # download pdf
    pdf_path = utils.download_file(r, path / "raw_danica.pdf", path)
    # print(r.status_code)

    # makes a blank list for data and sets up the regular expression for the data sheet, as well as one to find the date this pdf was published #
    data = []
    pattern = re.compile(r"^(.*?)\s+([\d\.]+)\s+([\d,%]+)$")
    date_pattern = re.compile(r"\d\d\-\d\d\-\d\d\d\d")

    # uses ioBytes to stream the pdf content so there isn't any need to download
    # the first group will be company name, the second group will be the stake in million DKK,
    # the third group is the share percentage, which we don't need
    pdf = pdfplumber.open(pdf_path)
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            lines = text.splitlines()
            for line in lines:
                date_pat = date_pattern.search(line)
                if date_pat:
                    date = date_pat.group()
                if "KAPITALANDELE" not in line:
                    match = pattern.match(line)
                    if match:
                        data.append(
                            {
                                "Shareholder - Name": "Danica Pension",
                                "Issuer - Name": match.groups()[0],
                                "Security - Report Date": date,
                                "Security - Market Value - Amount": f"{match.groups()[1]}",
                                "Security - Market Value - Multiplier": "x1_000_000",
                                "Security - Market Value - Currency Code": "DKK",
                                "Report Date URL": f"https://danica.dk/regnskaber/aarsrapporter{pdf_url}",
                            }
                        )

    # makes a dataframe out of the data list, exports it to csv just for testing purposes for now#
    df = pd.DataFrame(data)
    utils.export_df(df, "danica", path)


# Run function locally
if __name__ == "__main__":
    scrape_danica()
