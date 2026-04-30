"""Pensioenfonds Rail + OV Scraper.

Scrapes Pensioenfonds Rail + OV, a Dutch pension fund for the railways and
public transport sector. Scraper navigates to the Rail + OV website using
playwright and downloads the PDF. Searches the table of contents for pages
with necessary shareholdings. Extracts and matches entries from the PDF using
pdfplumber and regular expressions. Writes to TSV.
"""

import re

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("railov")
def scrape_railov() -> None:
    """Scrape Pensioenfonds Rail + OV (Netherlands, railway/public-transport pension) and write a TSV under ``data/railov/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # set up
    filename = "railov"
    path = utils.create_path(filename)

    # defining constants
    shareholder = "Rail & OV"
    URL = "https://railov.nl/over-ons/beleggen/waarin-beleggen/"
    currency = "EUR"

    # starting playwright
    playwright = sync_playwright().start()

    browser = playwright.chromium.launch(
        headless=True, slow_mo=500, channel="chromium"
    )
    page = browser.new_page()

    # go to page that leads to the PDF
    page.goto(URL)

    # extracting link to PDF from button
    link_button = page.get_by_role("link", name="Overzicht beleggingen")

    # not using the get.pdf() function as the pdf does not open in a new tab and cannot be recognized as a popup,
    # however using essentially the same process
    link_button.click()
    url = page.url

    r = requests.get(url)

    # setup filename
    filename = "raw_" + filename + ".pdf"
    pdf_path = path / filename

    # write pdf data to directory
    with open(pdf_path, "wb") as f:  # wb = with binary
        for chunk in r.iter_content(
            chunk_size=8192
        ):  # chunk size to slow download speed (avoid errors)
            if chunk:
                f.write(chunk)

    # close browser
    browser.close()

    # open PDF
    pdf = pdfplumber.open(pdf_path)
    playwright.stop()

    # extracting table of contents
    contents = []
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            pattern = re.compile(r"(\d+)\s+([A-Z][A-Za-z\s,-]+)\s+(\d+)")
            matches = pattern.findall(text)
            for match in matches:
                contents.append(match)

    contents_df = pd.DataFrame(contents, columns=["name", "category", "page"])

    # what are assumed to be the necessary sections of the pdf, excludes sections such as government bonds.
    needed = [
        "Aandelen ontwikkelde markten",
        "Aandelen opkomende markten",
        "Bedrijfsobligaties investment grade",
        "High Yield obligaties",
    ]
    contents_df["necessary"] = contents_df["category"].isin(needed)

    # defines the page range which is necessary to scrape
    page_range = []
    for i in range(len(contents_df)):
        if contents_df["necessary"][i]:
            page_range.append(
                list(
                    range(
                        int(contents_df["page"][i]),
                        (int(contents_df["page"][i + 1])),
                    )
                )
            )

    page_range = sum(page_range, [])

    # extracting and matching text using regular expression
    tabs = []
    for page in pdf.pages:
        if page.page_number in page_range:
            text = page.extract_text()
            if text:
                pattern = re.compile(
                    r"^(.+?)\s+€\s*([\d.,]+)\s+([\d.,]+%)$", re.MULTILINE
                )
                matches = pattern.findall(text)
                for match in matches:
                    tabs.append(match)

    # initializing dataframe
    df = pd.DataFrame(
        tabs,
        columns=[
            "Issuer - Name",
            "Security - Market Value - Amount",
            "Percent of Portfolio, to be Deleted",
        ],
    )

    # adding the remaining columns and constants
    df["Security - Report Date"] = utils.get_pdf_date(pdf)
    df["Shareholder - Name"] = shareholder
    df["Security - Market Value - Currency Code"] = currency
    df["Data Source URL"] = URL

    # reordering columns to match IDI's order
    df = df[
        [
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ]
    ]

    # removing commas and periods from market value
    no_commas = []
    for amount in df["Security - Market Value - Amount"]:
        no_commas.append(re.sub(r"[,]", "", amount))

    df["Security - Market Value - Amount"] = no_commas

    # export dataframe!
    utils.export_df(df, "railov", path)


# if run outside of main
if __name__ == "__main__":
    scrape_railov()
