"""BPL Pension Scraper.

Scrapes BPL pension, a Dutch pension company for employees in agriculture or
green energy. Scraper navigates to the downloads page of the BPL website, and
uses playwright and requests to download the pdf. Then, it loads the pdf with
pdfplumber and sorts through entries with regular expressions. Lastly, the
data is formatted and exported as a TSV. No manual steps needed unless the
website or format changes.
"""

import re

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("bpl")
def scrape_bpl() -> None:
    """Scrape BPL Pensioen (Netherlands, agriculture/green-energy pension) and write a TSV under ``data/bpl/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # -----------------/Setup and get PDF with playwright/-----------------#

    # Create path for files
    path = utils.create_path("bpl")
    # Website url
    url = "https://www.bplpensioen.nl/beleggen"

    # Start playwright instance
    playwright = sync_playwright().start()

    # Establish page and browser
    browser = playwright.chromium.launch(
        headless=True, slow_mo=1, channel="chromium"
    )
    page = browser.new_page()

    # Go to page that leads to PDF
    page.goto(url)

    # Refuse cookies (Sometimes doesn't show in headless mode)
    try:
        cookies_button = page.get_by_role("button", name="Weigeren")  # Refuse
        cookies_button.click()
    except Exception:
        pass

    # Expand list
    list_button = page.get_by_role(
        "button", name="Verslagen en rapportages"
    )  # Reports
    list_button.click()

    # Save button that leads to pdf preview
    link_button = page.get_by_role(
        "link", name="Beleggingsoverzicht"
    )  # Investment overview

    # Download pdf (returns pdf path)
    pdf_path = utils.get_pdf("bpl", page, link_button, browser, path)

    # Stop playwright instance
    playwright.stop()

    # -------------------/Find valid entries/-----------------#

    # Open pdf
    pdf = pdfplumber.open(pdf_path)

    # Extract all text into one string
    text = ""
    for p in pdf.pages:
        text = text + p.extract_text()

    # Regex follows schema of an entry in one column. Edge cases: Symbols in issuer, and occasional spaces in value number.
    pattern = re.compile(
        "\n(?P<issuer>[A-Za-z\\d /&+\\-\\.]+) (?P<stock>\\d{1,3},\\d{2}%) (?P<value>[\\d\\. ]+) "
    )

    # Setup constants for entries
    shareholder = "BPL Pension"
    report_date = utils.get_pdf_date(pdf)
    currency = "EUR"
    multiplier = "x1_000"
    entries = []

    # Apply regex, and format every match
    matches = re.findall(pattern, text)
    for m in matches:
        # Store groups in seperate variables
        issuer, stock, value = m
        # Remove any empty spaces in values (edge case)
        value = value.replace(" ", "")
        # Format one entry in accordance to IDI schema
        entry = [
            shareholder,
            issuer,
            report_date,
            value,
            multiplier,
            currency,
            stock,
            url,
        ]
        # Append entry
        entries.append(entry)

    # ----------/Export data/-------#

    # Create dataframe in accordance to IDI schema (matches entry format)
    df = pd.DataFrame(
        entries,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Multiplier",
            "Security - Market Value - Currency Code",
            "Stock - Percent Ownership",
            "Data Source URL",
        ],
    )
    utils.export_df(df, "bpl", path)


# --------/Run function locally--------/#
if __name__ == "__main__":
    scrape_bpl()
