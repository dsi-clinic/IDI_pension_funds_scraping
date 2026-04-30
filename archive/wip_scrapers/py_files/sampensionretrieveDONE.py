"""library imports"""

import io
import re
from datetime import date

import pandas as pd
import pdfplumber
import requests
from playwright.sync_api import sync_playwright

"""
using playwright to pull pdf from website to ensure it is up to date
"""
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = browser.new_page()
    page.goto(
        "https://www.sampension.dk/om-sampension/ansvarlighed/ansvarlige-investeringer/aabenhed-paa-esg-omraadet/"
    )

    page.get_by_role("button", name="Accepter alle").click()

    button = page.get_by_role("button", name="Aktier (pdf)")
    link = "https://www.sampension.dk/" + button.get_attribute("href")

    context.close()
    browser.close()

"""retrieving pdf link with requests"""
r = requests.get(link)

"""creating regular expression patterns to extract information"""
tabs = []
rd = []
tablepattern = re.compile(
    r"([A-Z0-9\-\&\.\s',]+?)\s+([\d\.]+)\s+([A-Z]{2}[A-Z0-9]{8,})"
)
datepattern = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")

"""streaming pdf to scrape information"""
with io.BytesIO(r.content) as file:
    pdf = pdfplumber.open(file)
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            matches = tablepattern.findall(text)
            for match in matches:
                tabs.append(match)
        if text:
            matches = datepattern.findall(text)
            for match in matches:
                rd.append(match)

rdstr = str(rd[0])
rdfinal = f"{rdstr[-4:]}-{rdstr[3:5]}-{rdstr[:2]}"


"""assigning values to columns of table, creating table and adding columns to match the Shareholder Tracker"""
newtabs = [(name.strip(), value, isin) for name, value, isin in tabs]
table = pd.DataFrame(newtabs[1:], columns=["Company", "Stake", "ISIN"])
table["Stake"] = table["Stake"] + " DKK"
table["Ticker"] = "-"
table["CUSIP"] = "-"
table["Shareholder"] = "Sampension Livsforsikring A/S"
table["Other Shareholders"] = "-"
table["Report Date"] = rdfinal
table["Filing Date"] = date.today()
table["PDF link"] = link

neworder = [
    "Company",
    "Ticker",
    "ISIN",
    "CUSIP",
    "Shareholder",
    "Other Shareholders",
    "Stake",
    "Report Date",
    "Filing Date",
    "PDF link",
]
table = table[neworder]

"""exporting table to CSV file"""
table.to_csv("sampension.csv")
