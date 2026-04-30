"""AP3 Scraper.

Scrapes AP3, a manager for the Swedish public's pension assets. Scraper
navigates to AP3 data page, searching for the most recent pdfs in the
categories: Swedish, foreign, fixed, and private, downloading them, and
returning a list of directories. Then, one-by-one the pdfs are parsed through
to find matching entries, the entries are formatted into dataframes, and
exported to TSVs. No manual steps needed unless the website or format changes.

Note: This scraper creates four PDFs and four TSVs.
"""

import datetime
import re

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("ap3")
def scrape_ap3() -> None:
    """Scrape AP3 (Sweden) Swedish, foreign, fixed-income, and private holdings into four TSVs under ``data/ap3/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # ----------------------------/Setup/------------------------------#

    # Set constants, create path
    shareholder = "AP3"
    url = "https://www.ap3.se/en/forvaltning/ap3s-portfolj/ap3s-vardepapper"
    path = utils.create_path("ap3")

    # Start playwright
    playwright = sync_playwright().start()

    # Establish page and browser
    browser = playwright.chromium.launch(
        headless=True, slow_mo=1, channel="chromium"
    )
    page = browser.new_page()

    # Go to page that leads to PDF
    page.goto(url)

    # Accept Cookies
    cookies_button = page.get_by_role("button", name="Only accept necessary")
    cookies_button.click()

    # --------------/Find Links to Download/--------------#

    # Get metadata of all links on page
    link_locators = page.get_by_role("link").all()

    # Desired pdfs, and empty list to append desired links to
    wanted_pdfs = ["swedish", "foreign", "fixed", "private"]
    pdf_links = []

    # Loop through wanted pdfs, searching for the most recent version with regex
    for w_pdf in wanted_pdfs:
        # Setup vars for 1 pdf loop
        year = int(datetime.date.today().year)
        link_append = ""

        # Until link append is found, loop through regex, each time checking one year back
        while not link_append:
            # Search for anything; followed by keyword; followed by anything; followed by specific month, symbols, numbers; followed by year; followed by .pdf
            dec_pat = (
                ".+" + w_pdf + ".+" + r"[december0-9\-]+" + str(year) + r"\.pdf"
            )
            june_pat = (
                ".+" + w_pdf + ".+" + r"[june0-9\-]+" + str(year) + r"\.pdf"
            )

            # Search through every link
            for link in link_locators:
                link = link.get_attribute("href")

                # Search for december pattern, if not found, search for june pattern
                match = re.search(dec_pat, link)
                if not match:
                    match = re.search(june_pat, link)

                # If match, move on to next pdf
                if match:
                    link_append = match.group()
                    pdf_links.append(link_append)
                    break

            # If failed, go back a year
            year -= 1

    # ---------------/Download PDFs/----------------#

    # List for paths to pdfs
    pdf_paths = []

    # Loop through each link, downloading each and saving the directory
    for index, pdf in enumerate(pdf_links, start=0):
        # Get data on pdf
        r = requests.get(pdf)

        # Piece together path and save
        filename = "raw_ap3_" + wanted_pdfs[index] + ".pdf"
        pdf_path = path / filename
        pdf_paths.append(pdf_path)

        # Copy data
        with open(pdf_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    # Stop playwright
    playwright.stop()

    # --------------------------/Scrape Swedish/--------------------------#

    # Open context manager for raw_ap3_swedish.pdf
    with pdfplumber.open(pdf_paths[0]) as pdf:
        # Get date of pdf
        report_date = utils.get_pdf_date(pdf)

        # Extract text into one string
        text = ""
        for page in pdf.pages:
            text = text + page.extract_text()

        # Search for: Names and symbols; Space; All caps letters or digits followed by a space and "SS"; Space; Search for types of equity with or statement; Space; 3 capital letters; Minimum of 1 digit, followed by any number of groups of digits of 3 (non capturing), followed by a period and 2 digits; Space; Search for numbers the same way but without the periods; Space?; Any number of all caps letters or digits (?)
        swedish_pattern = re.compile(
            r"(?P<issuer>[A-Za-z\-\.\d\| /&,()]+) (?P<ticker>[A-Z\d]+ SS) (?P<sectype>Equity|Fund EQ) (?P<currency>[A-Z]{3}) (?P<shares>\d{1,3}(?:,\d{1,3})*\.\d{2}) (?P<value>\d{1,3}(?:,\d{1,3})*) ?(?P<isin>[A-Z0-9]+)?"
        )

        # Find all matches in string
        entries = []
        matches = re.findall(swedish_pattern, text)
        for match in matches:
            # Access groups
            issuer, ticker, sectype, currency, shares, value, isin = match
            # Order variables in 1 entry
            append_list = [
                shareholder,
                issuer,
                sectype,
                isin,
                report_date,
                value,
                currency,
                ticker,
                shares,
                url,
            ]
            # Append entry
            entries.append(append_list)

        # Create dataframe according to IDI schema, and export
        df_swedish = pd.DataFrame(
            entries,
            columns=[
                "Shareholder - Name",
                "Issuer - Name",
                "Security - Type",
                "Security - ISIN",
                "Security - Report Date",
                "Security - Market Value",
                "Security - Market Value - Currency Code",
                "Stock - Ticker",
                "Stock - Number of Shares",
                "Data Source URL",
            ],
        )
        utils.export_df(df_swedish, "ap3_swedish", path)

    # --------------------------/Scrape Foreign/--------------------------#

    # Open context manager for raw_ap3_foreign.pdf
    with pdfplumber.open(pdf_paths[1]) as pdf:
        # Get date of pdf
        report_date = utils.get_pdf_date(pdf)

        # Extract text into one string
        text = ""
        for page in pdf.pages:
            text = text + page.extract_text()

        # Search for: Any number of letters and symbols; Space; Letters,digits,dashes followed by 2 capital letters; Space; An or statement for security type; Space; 3 capital letters; Space; Groups of 1-3 numbers seperated by commas and ending with a period and 2 numbers; Space; Capital letters and digits (Optional)
        foreign_pattern = re.compile(
            r"(?P<issuer>[A-Z\-\./& ]+) (?P<ticker>[A-Z\d/]+ [A-Z]{2}) (?P<sectype>Equity|Fund EQ) (?P<currency>[A-Z]{3}) (?P<shares>\d{1,3}(?:,\d{1,3})*\.\d{2}) (?P<value>\d{1,3}(?:,\d{1,3})*) (?P<isin>[A-Z0-9]+)?"
        )

        # Find all matches in string
        entries = []
        matches = re.findall(foreign_pattern, text)
        for match in matches:
            # Access groups
            issuer, ticker, sectype, currency, shares, value, isin = match
            # Order variables in 1 entry
            append_list = [
                shareholder,
                issuer,
                sectype,
                isin,
                report_date,
                value,
                currency,
                ticker,
                shares,
                url,
            ]
            # Append entry
            entries.append(append_list)

        # Create dataframe according to IDI schema, and export
        df_foreign = pd.DataFrame(
            entries,
            columns=[
                "Shareholder - Name",
                "Issuer - Name",
                "Security - Type",
                "Security - ISIN",
                "Security - Report Date",
                "Security - Market Value",
                "Security - Market Value - Currency Code",
                "Stock - Ticker",
                "Stock - Number of Shares",
                "Data Source URL",
            ],
        )
        utils.export_df(df_foreign, "ap3_foreign", path)

    # --------------------------/Scrape Fixed/--------------------------#

    # Open context manager for raw_ap3_fixed.pdf
    with pdfplumber.open(pdf_paths[2]) as pdf:
        # Get pdf date
        report_date = utils.get_pdf_date(pdf)

        # Extract pages to a string
        text = ""
        for page in pdf.pages:
            text = text + page.extract_text()

        # Searches for: Letters and symbols, but must start with a capital letter; Space; Or statement for security type; At least one group of digits ranging from 1-3 seperated by commas. Lines end here
        fixed_pattern = re.compile(
            r"(?P<issuer>^[A-Z][A-Za-z\-\./& ]+) (?P<sectype>Corporates|Governments & Sovereigns|Mortgages & Agencies|FUND FI|Bond) (?P<value>\d{1,3}(?:,\d{1,3})*)$",
            re.MULTILINE,
        )

        # Find all matches in string
        matches = re.findall(fixed_pattern, text)
        entries = []
        for match in matches:
            # Access groups
            issuer, sectype, value = match
            # Order variables in 1 entry
            append_list = [
                shareholder,
                issuer,
                sectype,
                report_date,
                value,
                url,
            ]
            # Append entry
            entries.append(append_list)

        # Create dataframe according to IDI schema, and export
        df_fixed = pd.DataFrame(
            entries,
            columns=[
                "Shareholder - Name",
                "Issuer - Name",
                "Security - Type",
                "Security - Report Date",
                "Security - Market Value",
                "Data Source URL",
            ],
        )
        utils.export_df(df_fixed, "ap3_fixed", path)

    # --------------------------/Scrape Private/--------------------------#

    # Open context manager for raw_ap3_private.pdf
    with pdfplumber.open(pdf_paths[3]) as pdf:
        # Set constants
        report_date = utils.get_pdf_date(pdf)
        multiplier = "x1_000_000"

        # Extract text to single string
        text = ""
        for page in pdf.pages:
            text = text + page.extract_text()

        # Search for: Letters, digits, symbols; Space; 3 all caps letters; Space; 2 digits; Space; 4 Digits
        private_pattern = re.compile(
            r"(?P<issuer>[A-Za-z\-\.\d\| /&,()]+) (?P<currency>[A-Z]{3}) (?P<value>\d{2}) (?P<vintage_year>\d{4})"
        )

        # Find all matches in string
        entries = []
        matches = re.findall(private_pattern, text)
        for match in matches:
            # Access Groups
            issuer, currency, value, vintage_year = match
            # Order groups in 1 entry
            append_list = [
                shareholder,
                issuer,
                report_date,
                value,
                multiplier,
                currency,
                vintage_year,
                url,
            ]
            # Append entry
            entries.append(append_list)

        # Create dataframe according to IDI schema, and export
        df_private = pd.DataFrame(
            entries,
            columns=[
                "Shareholder - Name",
                "Issuer - Name",
                "Security - Report Date",
                "Security - Market Value",
                "Security - Market Value - Multiplier",
                "Security - Market Value - Currency Code",
                "Private Equity - Vintage year",
                "Data Source URL",
            ],
        )
        utils.export_df(df_private, "ap3_private", path)


# ---------/Scrape Locally/---------#
if __name__ == "__main__":
    scrape_ap3()
