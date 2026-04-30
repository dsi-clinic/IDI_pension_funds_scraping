"""Pensioenfonds Vervoer Scraper.

Scrapes Pensioenfonds Vervoer, a nonprofit pension fund in transport, based
in the Netherlands. Scraper navigates to the pdf preview and downloads it,
then extracts and formats text to be filtered with regular expressions.
Additional filtering and formatting is done in tandem as entries are prepared
for export. Exports to TSV. No manual steps needed unless the website or
format changes.
"""

import re

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("vervoer")
def scrape_vervoer() -> None:
    """Scrape Pensioenfonds Vervoer (Netherlands, transport pension) and write a TSV under ``data/vervoer/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # /-----Setup-----/#

    # Create directory for PDFs and CSVs (create_path returns path object used later)
    filename = "vervoer"
    path = utils.create_path(filename)

    # Columns for final dataframe (constants)
    shareholder = "Vervoer"
    URL = "https://www.pfvervoer.nl/over-ons/beleggen/spreiden-van-beleggingen"
    currency = "EUR"
    multiplier = "x1"

    # Playwright Start
    playwright = sync_playwright().start()

    # Establish page and browser
    browser = playwright.chromium.launch(
        headless=True, slow_mo=1, channel="chromium"
    )
    page = browser.new_page()

    # Go to page that leads to PDF
    page.goto(URL)

    # Save button that leads to pdf preview
    link_button = page.get_by_role(
        "link", name="overzicht van onze beleggingen (pdf)"
    )  # likely to break here on future updates of website

    # Get PDF
    pdf_path = utils.get_pdf(filename, page, link_button, browser, path)
    pdf = pdfplumber.open(pdf_path)
    playwright.stop()

    # /-----Extract PDF-----/#

    # Fills list with strings containing the text of each page
    tabs = []
    for page in pdf.pages:
        # Collate all text on a page into one string, with parameters telling python where to find text
        text = page.extract_text(layout=True, x_density=4)
        # Strips text of new lines, and appends each page to empty list tabs
        tabs.append(text.strip() + "\n")

    # Combines list entries into one string
    tabs2 = " ".join(tabs)
    # Splits breaks in string into a list of strings
    tabs3 = tabs2.splitlines()

    # /-----Create and apply regular expression-----/#

    # Create regex, looking for 2 sets of groups of words followed by numbers, or a dash (with many edge cases)
    pattern = re.compile(
        r"(?P<l_key>[A-Za-z\s,]+?)\s+(?P<l_value>([\d\.]+( [\d\.]+)*,?)|(-))\s+(?P<r_key>[A-Za-z\s,]+?)\s+(?P<r_value>([\d\.]+( [\d\.]+)*,?)|(-))"
    )

    # Create two empty lists so that the order may be maintained
    tabs4 = []
    tabs5 = []
    report_date = utils.get_pdf_date(pdf)
    for tab in tabs3:
        var = None
        var = pattern.search(tab.strip())
        # If match is found in entry, further break it down
        if var:
            # Remove Entries with commas and dashes
            var2 = re.search(r"(?!\d+?[,-])\d+?", var.group(2))
            if var2:
                # Assemble entry in column order
                issuer = var.group(1)
                m_value = var.group(2)
                list = [
                    shareholder,
                    issuer,
                    report_date,
                    m_value,
                    multiplier,
                    currency,
                    URL,
                ]
                tabs4.append(list)

            var2 = re.search(r"(?!\d+?[,-])\d+?", var.group(7))
            if var2:
                issuer = var.group(6)
                m_value = var.group(7)
                list = [
                    shareholder,
                    issuer,
                    report_date,
                    m_value,
                    multiplier,
                    currency,
                    URL,
                ]
                tabs5.append(list)
    # Combine lists
    tabs6 = tabs4 + tabs5

    # /-----Export-----/#

    # Create dataframe
    df = pd.DataFrame(
        tabs6,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    # export
    utils.export_df(df, filename, path)


# Run function locally
if __name__ == "__main__":
    scrape_vervoer()
