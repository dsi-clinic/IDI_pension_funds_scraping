"""Sampension Scraper.

Scrapes sampension, a Danish company that invests in "labor market and
company pensions." This PDF's data is not stored in text but images, so
Tesseract must be installed in order for it to work (see main git page).
Scraper finds the path to the Tesseract executable (untested on non-Windows
operating systems). Then, fetches the pdf with playwright and gets the report
date from the HTML code. Each pdf page is looped through, creating an image,
applying regex, and repeating until done (image is deleted when finished).
Data is further formatted, then exported as a TSV. No manual steps needed
unless the website or format changes.

Note: Tesseract messes up a lot, so upon updates manual review of this
scraper is probably needed (the log should indicate if this is the case). An
easy way to compare columns is by pasting them into a spreadsheet and looking
for when they fall out of sync with the PDF.
"""

import logging
import os
import platform
import re
from pathlib import Path

import pandas as pd
import pdfplumber
import pytesseract
import requests
from PIL import Image
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

# Append to logs when imported by main.py; create a fresh log when run standalone.
log_type = "w" if __name__ == "__main__" else "a"


@register("sampension")
def scrape_sampension() -> None:
    """Scrape sampension (Denmark) image-based PDF via Tesseract OCR and write a TSV under ``data/sampension/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # Set up Logging
    parent_dir = Path(__file__).parent.parent
    log_path = parent_dir / "log.log"
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        filemode=log_type,
        format="%(asctime)s - %(message)s",
    )

    # ----------------------/Find tesseract EXE/-------------------------#

    # Find operating system
    osys = platform.system()
    # Find user profile name
    user = os.getlogin()

    # Ifelse ladder that checks different paths for the tesseract EXE. If not found, the function stops.
    # For each system, prorgrams for all users are checked first, and if not found, the current user's programs are checked
    if osys == "Windows":
        if os.path.isfile(r"C:\Program Files\Tesseract-OCR\tesseract.exe"):
            # Setting this specific attribute of pytesseract to the absolute file path
            pytesseract.pytesseract.tesseract_cmd = (
                r"C:\Program Files\Tesseract-OCR\tesseract.exe"
            )
        elif os.path.isfile(
            rf"C:\Users\{user}\AppData\Local\Programs\Tesseract-OCR"
        ):
            pytesseract.pytesseract.tesseract_cmd = (
                rf"C:\Users\{user}\AppData\Local\Programs\Tesseract-OCR"
            )
        else:
            # Break function if not found (and log)
            logging.info("Sampension - Failed to find tesseract path")
            return

    # Untested - I don't have a mac to test this with
    elif osys == "iOS":
        if os.path.isfile("/Applications/Tesseract-OCR/tesseract.exe"):
            pytesseract.pytesseract.tesseract_cmd = (
                "/Applications/Tesseract-OCR/tesseract.exe"
            )
        elif os.path.isfile(
            f"/Users/{user}/Applications/Tesseract-OCR/tesseract.exe"
        ):
            pytesseract.pytesseract.tesseract_cmd = (
                f"/Users/{user}/Applications/Tesseract-OCR/tesseract.exe"
            )
        else:
            logging.info("Sampension -Failed to find tesseract path")
            return

    # Untested - I don't have linux to test this with
    elif osys == "Linux":
        if os.path.isfile("/usr/local/bin/Tesseract-OCR/tesseract.exe"):
            pytesseract.pytesseract.tesseract_cmd = (
                "/usr/local/bin/Tesseract-OCR/tesseract.exe"
            )
        elif os.path.isfile("/usr/bin/Tesseract-OCR/tesseract.exe"):
            pytesseract.pytesseract.tesseract_cmd = (
                "/usr/bin/Tesseract-OCR/tesseract.exe"
            )
        elif os.path.isfile(
            f"/home/{user}/.local/share/Tesseract-OCR/tesseract.exe"
        ):
            pytesseract.pytesseract.tesseract_cmd = (
                f"/home/{user}/.local/share/Tesseract-OCR/tesseract.exe"
            )
        else:
            logging.info("Sampension - Failed to find tesseract path")
            return

    else:
        logging.info("Sampension - Failed to identify operating system")
        return

    logging.info("Sampension - Tesseract found")

    # ----------------------/Get PDF with playwright/-------------------------#

    # Create path
    path = utils.create_path("sampension")
    # URL to reports
    url = "https://www.sampension.dk/om-sampension/ansvarlighed/ansvarlige-investeringer/aabenhed-paa-esg-omraadet/"

    # Start Playwright
    playwright = sync_playwright().start()

    # Go to page
    browser = playwright.chromium.launch(
        headless=True, slow_mo=1, channel="chromium"
    )
    page = browser.new_page()
    page.goto(url)

    # Reject Cookies
    reject_cookies = page.get_by_role("button", name="Afvis alle")
    reject_cookies.click()

    # Find Holdings
    link_button = page.get_by_role("button", name="Aktier (pdf)", exact=False)

    # Format PDF link
    pdf_link = link_button.get_attribute("href")
    pdf_link = "https://www.sampension.dk/" + pdf_link

    # Request Data
    req = requests.get(pdf_link)

    # Download file and save path
    pdf_path = utils.download_file(req, "raw_sampension.pdf", path)

    # Find and format report date listed on website
    # Get all paragraph objects
    page_text = page.get_by_role("paragraph").all()

    # For each object
    for line in page_text:
        # Access text
        line = line.inner_text()
        # Search for date
        m = re.search(
            r"(?P<date>\d+)\. (?P<month>[A-Za-z]+) (?P<year>\d{4})", line
        )

        # if date is found, format
        if m:
            # Split the groups into individual vars
            date, month, year = m.groups()

            # Convert month to number
            month = utils.convert_month(month)

            # Make date two digits if it is only one
            if len(date) < 2:
                date = "0" + date

            # Stitch together formatted date
            report_date = year + "-" + month + "-" + date

            # Break for loop
            break
    # Delete all page text
    del page_text

    # Stop Playwright
    browser.close()
    playwright.stop()

    # ----------------------/Format Data/-------------------------#

    # Text that tesseract gathers is split like: All issuers on a page, followed by All values and isins on a page, thus we must use 2 regex (and ensure they match eachother exactly)

    # Issuer pattern - Search for all caps words with symbols. Currently picks up more than just issuers, but edge cases addressed later.
    issuer_pattern = re.compile(r"(?P<issuer>[A-Za-z\d\-\. /&()'_:,]+)")
    # Values and isins - Search for digits and dots, a space, and an ISIN code that is 12 or more chars.
    value_isin_pattern = re.compile(
        r"(?P<value>\d[\d\. ]+) (?P<isin>[A-Z\d ]{12,})"
    )

    # Set other constants
    currency = "DKK"
    shareholder = "Sampension"

    # Set vars for loop
    string_cheese = ""
    issuer_col = []
    values_col = []
    isin_col = []

    # Open PDF
    pdf = pdfplumber.open(pdf_path)

    # For each page, extract text from image and apply regex
    for page_num, _page in enumerate(pdf.pages):
        # Reset string cheese
        string_cheese = ""

        # Get text
        try:
            # Save 1 pdf page as an image
            (
                pdf.pages[page_num]
                .to_image(resolution=144)
                .save(path / "sampension.png")
            )
            # Use tesseract to get text from said image
            string_cheese = pytesseract.image_to_string(
                Image.open(path / "sampension.png")
            )
        except Exception:
            # If failed, log and continue
            logging.info(f"Sampension - Failed to get text on page {page_num}")
            pass

        # If text is found
        if string_cheese:
            # Tess is kind of bad at its job. Manually removing symbols it made up.
            string_cheese = re.sub(r"’|£|\|", "", string_cheese)

            # Find all issuers
            issuers = re.findall(issuer_pattern, string_cheese)
            # For each issuer found
            for m in issuers:
                # Many ISINs are accidently picked up. The following checks to see if there are less than 3 digits present (ISINs typically have more)
                is_issuer = re.findall(r"\d", m)
                is_issuer = len(is_issuer)
                if is_issuer <= 2:
                    issuer_col.append(m)

            # Search for values and ISINs
            values_isin = re.findall(value_isin_pattern, string_cheese)
            # For each match found
            for m in values_isin:
                # Split into 2 vars
                value, isin = m

                # If at least one period is present, the matches may be appended to their respective columns
                if re.search(r"\.", value):
                    values_col.append(value)
                    isin_col.append(isin)

    # Issuer regex picks up titles and column names. Remove them.
    issuer_col_fixed = []
    for i in issuer_col:
        if re.search("[Ss]ampension", i):
            pass
        elif re.search("DKK", i):
            pass
        else:
            issuer_col_fixed.append(i)

    # Issue where is last page only has one entry, new lines are not picked up to seperate it.
    # Check to see if number of entries for columns match (isin and value will always match). If not, try to get the last issuer again/
    if len(issuer_col_fixed) != len(isin_col):
        last = re.search("[A-Z ]+", string_cheese)
        issuer_col_fixed.append(last.group())

    # Delete image used
    os.remove(path / "sampension.png")

    # ----------------------/Export Data/-------------------------#

    # Find length of dataset
    length = len(issuer_col_fixed)
    # Create dictionary for dataframe, multiplying all constants by length of dataset
    df_dict = {
        "Shareholder - Name": [shareholder] * length,
        "Issuer - Name": issuer_col_fixed,
        "Security - ISIN": isin_col,
        "Security - Report Date": [report_date] * length,
        "Security - Market Value - Amount": values_col,
        "Security - Market Value - Currency Code": [currency] * length,
        "Data Source URL": [url] * length,
    }

    # Create dataframe
    try:
        df = pd.DataFrame(df_dict)
    except Exception:
        # If failed, log and end function
        logging.info(
            "Sampension - DF column lengths do not match. Manual review needed."
        )
        return
    # Export as TSV
    utils.export_df(df, "sampension", path)


# ----------/Run function locally/----------#
if __name__ == "__main__":
    scrape_sampension()
