"""PME Pensioenfonds Scraper.

Scrapes PME pensioenfonds, a Dutch pension fund for employees in the metal
and tech industries. Info is stored in dynamic HTML, which takes approximately
5-10 minutes to scrape with playwright. Scraper establishes a connection to
the data source page with playwright, and extracts and formats needed
information such as the next button, the equity number of pages, and the
report date. Then, a while loop is created both to save raw data and loop
through pages to format that data. This script sometimes fails due to a bad
network -- if it fails, try running again once or twice before giving up. The
script is fully automatic and won't need to be changed unless the format of
the website does.

Note: Attempts to fetch data directly were made both synchronously and
asynchronously. Sync was slower, and async was faster but often missed pages.
Playwright seems to be the best approach for now.
"""

import logging
import re
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

# Append to log when imported by main.py; create a fresh log when run standalone.
log_mode = "w" if __name__ == "__main__" else "a"


@register("pme")
def scrape_pme() -> None:
    """Scrape PME pensioenfonds (Netherlands, metal/tech pension) and write a TSV under ``data/pme/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # -------------/Setup/------------#

    # Create and save directory
    path = utils.create_path("pme")
    # URL to Dynamic HTML
    url = "https://www.pmepensioen.nl/en/investments/we-do-invest-in"

    # Find directory of repository
    parent_dir = Path(__file__).parent.parent
    # Setup logging
    logging.basicConfig(
        filename=parent_dir / "log.log",
        level=logging.INFO,
        filemode=log_mode,
        format="%(asctime)s - %(message)s",
    )

    # ----------------/Get page text/-----------------#

    # Playwright Start
    playwright = sync_playwright().start()

    # Establish page and browser
    browser = playwright.chromium.launch(
        headless=True, slow_mo=1, channel="chromium"
    )
    page = browser.new_page()

    # Go to page that leads to PDF
    page.goto(url)

    # Cookies (sometimes doesn't appear in headless mode)
    try:
        page.get_by_role("button", name="Decline").click()
    except Exception:
        pass

    # Sometimes playwright struggles to find the button we need. This loop ensures the script runs until it is found.
    next_pension = ""
    while not next_pension:
        # Find all next buttons.
        locator = page.get_by_role("link", name="next", exact=False).all()
        try:
            # First next button is the one that corresponds to equities.
            next_pension = locator[0]
        except Exception:
            pass

    # Scroll to next button to be able to click it.
    next_pension.scroll_into_view_if_needed()

    # Get all text on page
    text = page.inner_text("div")

    # --------------/Get constants/----------------#

    # Find the number of pages as an integer
    page_numbers = re.search(r"Page 1 of (?P<max>\d+)", text)
    page_numbers = int(page_numbers.group(1))

    # Report date not listed, however the next report date is, as well as the fact that they report every 3 months. Using this, we can approximate report dates.
    # Find date of next report using regex
    next_report_date = re.search(
        r"(?P<month>[A-Z][a-z]+) (?P<day>\d+)[a-z]+, (?P<year>\d{4})", text
    )
    if next_report_date:
        # Split groups into vars
        month, day, year = next_report_date.groups()

        # Convert month word to date with offset
        month = utils.convert_month(month, -3)

        # Convert day to double digit if necessary
        if len(day) < 2:
            day = "0" + day

        # Piece together report date in desired format
        report_date = year + "-" + month + "-" + day
    else:
        # If not found, report date is NA
        report_date = ""

    # Establish other constants
    shareholder = "PME pensioenfonds"
    currency = "EUR"
    multiplier = "x1"

    # Setup variables for loop
    count = 1  # Progress loop
    new_text = ""  # New text to parse
    old_text = (
        ""  # Old text to compare new text to (ensure that page has turned)
    )
    entries = []  # Entries for DF

    # Regular expression to match entries. Follows schema of entries on site, with each category being seperated by a tab.
    # Edge cases: Symbols and numbers in issuer names. Some country names have multiple words. Some countries have ", Republic of" as a suffix, which the expression does not capture.
    entry_pattern = re.compile(
        "(?P<issuer>[A-Za-z\\d\\. /&\\-,]+)\t(?P<value>[\\d\\.]+)\t(?P<country>[A-Za-z]+(?: [A-Za-z ]+)?)(?:, Republic of)?\t(?P<sector>[A-Za-z ,\\-]+)\t(?P<type>[A-Za-z ]+)"
    )

    # -------------/Loop through pages, Scrape data/------------#

    # Open context manager to a text file. Essentially saves a snapshot of every page, just to have some documentation of raw data.
    with open(path / "raw_data_pme.txt", "w") as file:
        logging.info("PME - Begin cycling through pages")

        # Loop through each page
        while count <= page_numbers:
            # Collect text on a page
            new_text = page.inner_text("div")

            # If new text is not the same as old text, proceed with regex
            if new_text != old_text:
                # New text (unformatted) is now the old text
                old_text = new_text
                # Write unformatted new text to text file
                file.write(new_text)

                # Split text by lines
                new_text = new_text.splitlines()

                # For every line
                for line in new_text:
                    # Equity entries don't continue past this text. When reached, break loop.
                    if line == "Investments in the Netherlands":
                        break
                    # If it is a potential equity entry,
                    else:
                        # Apply pattern to 1 line
                        match = re.search(entry_pattern, line)

                        # If match is found,
                        if match:
                            # Split into variables
                            issuer, value, country, sector, sectype = (
                                match.groups()
                            )
                            # Append an entry according to IDI schema
                            entries.append(
                                [
                                    shareholder,
                                    issuer.strip(),
                                    country.strip(),
                                    sector.strip(),
                                    sectype.strip(),
                                    report_date,
                                    value.strip(),
                                    multiplier,
                                    currency,
                                    url,
                                ]
                            )

                # After all lines are looped through, tick counter
                count += 1

                # Ideally, a modulo operator could've been used to log every dozen pages, but due to playwright lag that doesn't work.
                # Uncomment this for debug purposes
                # logging.info(f"PME - Found page {count}")

            # If new text and old text are the same, try to click again
            else:
                try:
                    next_pension.click()
                except Exception:
                    pass

        # At end of pages, close browser, stop playwright, and log success
        browser.close()
        playwright.stop()
        logging.info("PME - Finished cycling through pages.")

    # -----------/Create and export DF/--------------#

    # Create DF according to IDI schema
    df = pd.DataFrame(
        entries,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Issuer - Country Name",
            "Issuer - Sector",
            "Security - Type",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    # Export as tsv
    utils.export_df(df, "pme", path)


# --------/Run function locally/--------#
if __name__ == "__main__":
    scrape_pme()
