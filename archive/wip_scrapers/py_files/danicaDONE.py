"""Danica Pension scraper
this is automated barring structural changes to their website.
it matches the shareholder tracker in most ways, except the ticker and cusip
"""

"""standard imports"""

import io
import re
from datetime import date

import requests

"""third party imports"""
import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

"""use playwright to get the correct url for the pdf"""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = browser.new_page()

    """go to homepage"""
    page.goto("https://danica.dk/en/personal")

    """accept necessary cookies"""
    page.get_by_role("button", name="OK to Necessary").click()

    """go to reports page"""
    page.goto("https://danica.dk/regnskaber/aarsrapporter")

    """finds the most recent form containing "Aktibog" in its html tag.
    this translates to share register.
    saves the suffix of that url as pdf_url """
    frame = page.locator("ul[class = 'container']")

    years_to_try = frame.locator("li").all()

    pdf_url = ""
    i = 0
    while bool(pdf_url) == False:
        try:
            pdf_url = (
                years_to_try[i]
                .locator('table tbody tr td:has-text("Aktiebog") + td a')
                .get_attribute("href", timeout=1000)
            )
        except:
            i = i + 1
            continue

    browser.close()

"""goes to the webpage of the pdf"""

r = requests.get(f"https://danica.dk/regnskaber/aarsrapporter{pdf_url}")

"""makes a blank list for data and sets up the regular expression for the data sheet, as well as one to find the date this pdf was published """
data = []
pattern = re.compile(r"^(.*?)\s+([\d\.]+)\s+([\d,%]+)$")
date_pattern = re.compile(r"\d\d\-\d\d\-\d\d\d\d")

"""uses ioBytes to stream the pdf content so there isn't any need to download
the first group will be company name, the second group will be the stake in million DKK, 
the third group is the share percentage, which we don't need"""
with io.BytesIO(r.content) as file:
    pdf = pdfplumber.open(file)
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
                                "COMPANY": match.groups()[0],
                                "SHAREHOLDER": "Danica Pension",
                                "STAKE": f"{match.groups()[1]} million DKK",
                                "REPORT DATE": date.today(),
                                "FILING DATE": date,
                                "FORM LINK": f"https://danica.dk/regnskaber/aarsrapporter{pdf_url}",
                            }
                        )


"""makes a dataframe out of the data list, exports it to csv just for testing purposes for now"""
df = pd.DataFrame(data)
df.to_csv("danicapensionshares.csv", index=False)
