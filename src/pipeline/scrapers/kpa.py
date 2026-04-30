"""KPA Pension Scraper.

Scrapes KPA pensions, a group of companies based in Sweden that offer pension
management, insurance, asset management, and more. Scraper navigates to pdf
preview and downloads, then filters for entries based on text size. Then, the
data is formatted into a dictionary and exported as a TSV. No manual steps
needed unless the website or format changes.
"""

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register


@register("kpa")
def scrape_kpa() -> None:
    """Scrape KPA Pension (Sweden) and write a TSV under ``data/kpa/<YYYY-MM-DD>/``.

    Raises:
        Exception: Propagates network, parsing, or I/O failures to the
            caller; the CLI logs and continues with the next scraper.
    """
    # Setup
    filename = "KPA"
    url = "https://www.kpa.se/om-kpa-pension/vart-hallbarhetsarbete/ansvarsfulla-investeringar/innehav-och-uteslutna-bolag/"
    path = utils.create_path(filename)

    # Start Playwright
    playwright = sync_playwright().start()

    # Go to page
    browser = playwright.chromium.launch(
        headless=True, slow_mo=5, channel="chromium"
    )
    page = browser.new_page()
    page.goto(url)

    # Reject Cookies
    reject_cookies = page.get_by_role("button", name="Avvisa cookies")
    reject_cookies.click()

    # Find Holdings
    link_button = page.get_by_role("link", name="Innehav", exact=False)

    # Format PDF link
    pdf_link = link_button.get_attribute("href")
    pdf_link = "https://www.kpa.se/" + pdf_link

    # Request Data
    req = requests.get(pdf_link)

    # Download file and save path
    pdf_path = utils.download_file(req, "raw_kpa.pdf", path)

    # Stop Playwright
    browser.close()
    playwright.stop()

    # Open PDF
    pdf = pdfplumber.open(pdf_path)

    # Extract Entries Based on Font Size
    entries = []
    for page in pdf.pages:
        # Extract every line in a page, formatted in a list of dictionaries
        text = page.extract_text_lines()

        # For each line, calculate and check height
        for t in text:
            height = t["bottom"] - t["top"]
            # If height falls within range, it is a match
            if height > 9 and height < 10:
                entries.append(t["text"])

    # Formatting Data

    # Create columns
    shareholder_name = [filename]
    report_date = [utils.get_pdf_date(pdf)]
    url = [url]

    number_of_entries = len(entries)

    # Repeat constants per number of entries
    shareholder_name = shareholder_name * number_of_entries
    report_date = report_date * number_of_entries
    url = url * number_of_entries

    # Format dataframe in dictionary
    df = {
        "Shareholder - Name": shareholder_name,
        "Issuer - Name": entries,
        "Security - Report Date": report_date,
        "Data Source URL": url,
    }

    # Export
    final_df = pd.DataFrame(df)
    # Export as tsv
    utils.export_df(final_df, filename, path)


if __name__ == "__main__":
    scrape_kpa()
