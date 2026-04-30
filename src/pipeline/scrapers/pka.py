"""PKA Scraper.

Scrapes Pensionskassernes Administration a/s, a Denmark-based company that
claims to invest in causes that comply with the EU's green and social agenda.
Scraper begins by downloading the pdf with playwright, while attempting to
deny cookies if prompted. Then, inside a context manager, it extracts entries
and checks for matches with pdfplumber and regular expressions before writing
to a TSV. No manual steps needed unless the website or format changes.
"""

import csv
import re

import pdfplumber
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("pka")
def scrape_pka() -> None:
    """Scrape Pensionskassernes Administration (Denmark) and write a TSV under ``data/pka/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # /-----Setup - Download PDF-----/#

    # Create path and url constant
    path = utils.create_path("pka")
    url = "https://pka.dk/ansvarlighed/ansvarlige-investeringer/politikker-og-rapporter"

    # Playwright Start
    playwright = sync_playwright().start()

    # Establish page and browser
    browser = playwright.chromium.launch(
        headless=True, slow_mo=1, channel="chromium"
    )
    page = browser.new_page()

    # Go to page that leads to PDF
    page.goto(url)

    # Deny all Cookies (Doesn't always appear in headless mode)
    try:
        reject_cookies = page.get_by_role("button", name="Afvis Alle")
        reject_cookies.click()
    except Exception:
        pass

    # Expand menu
    button_1 = page.get_by_role("button", name="Beholdningslisten", exact=True)
    button_1.click()

    # Save button that leads to pdf preview
    link_button = page.get_by_role(
        "link", name="Se beholdningslisten", exact=True
    )

    # Get PDF
    pdf_path = utils.get_pdf("pka", page, link_button, browser, path)
    playwright.stop()

    # /-----Apply Regex and write TSV-----/#

    # Regex to match the line format: company name, ISIN, market value, share percent
    pattern = re.compile(
        r"^(.*?)\s+([A-Z]{2}[A-Z0-9]{10})\s+([\d.,]+)\s+([\d.,]+\s*%)$"
    )

    # Open pdf from path
    with pdfplumber.open(pdf_path) as pdf:
        # Create and open tsv file in path
        with open(path / "pka.tsv", "w", newline="", encoding="utf-8") as f:
            # ---Set column constants---#

            shareholder = "Pensionskassernes Administration a/s"
            report_date = utils.get_pdf_date(pdf)

            # Search for DKK, if not found, return no currency
            currency_search = pdf.pages[0].extract_text()
            currency_search = re.search("(dkk)|(DKK)", currency_search)
            if currency_search:
                currency_code = "DKK"
            else:
                currency_code = " "

            # ---Match and write---#

            # Open file, set spaces to tabs, and line seperators to new lines
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            # Write heading column following IDI schema
            writer.writerow(
                [
                    "Shareholder - Name",
                    "Issuer - Name",
                    "Security - Report Date",
                    "Security - Market Value - Amount",
                    "Security - Market Value - Currency Code",
                    "Security - ISIN",
                    "Stock - Percent Ownership",
                    "Data Source URL",
                ]
            )

            # For index and page in pages: Extract, format, match, and if match, append
            for _i, page in enumerate(pdf.pages, start=1):
                # Extract
                text = page.extract_text()
                # Split into entries
                if text:
                    lines = text.splitlines()

                    # Check line for matches
                    for line in lines:
                        match = pattern.match(line.strip())
                        # If match, write to tsv
                        if match:
                            # Set variables to corresponding group
                            issuer, isin, value, percent_ownership = (
                                match.groups()
                            )
                            # Write to CSV
                            writer.writerow(
                                [
                                    shareholder,
                                    issuer,
                                    report_date,
                                    value,
                                    currency_code,
                                    isin,
                                    percent_ownership,
                                    url,
                                ]
                            )


# Run locally
if __name__ == "__main__":
    scrape_pka()
