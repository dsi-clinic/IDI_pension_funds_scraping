'''scrapes bpfBOUW, a dutch company that manages pension funds in the construction industry. scraper navigates to the bpfBOUW website and downloads the most recent PDF of their shareholder report. scraper uses regular expression to extract information, and does additional reformatting including extracting the date. a csv file of dutch countries is imported, and removes countries the regex views as companies. exports to tsv'''

#import modules
from playwright.sync_api import sync_playwright
from pathlib import Path
import re
import pdfplumber
import pandas as pd
import locale
from datetime import datetime

if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions

#main function
def scrape_bpfbouw():
    #set up
    filename = "bpfbouw"
    path = functions.create_path(filename)

    #defining constants
    shareholder = "bpfBOUW"
    URL = "https://www.bpfbouw.nl/over-bpfbouw/hoe-we-beleggen"
    currency = "EUR"
    multiplier = "x100"

    #starting playwright
    playwright = sync_playwright().start()

    browser = playwright.chromium.launch(headless=True, slow_mo=500, channel="chromium")
    page = browser.new_page()

    #go to page that leads to the PDF
    page.goto(URL)

    #accepting cookies in pop-up window
    page.get_by_role("button", name="Alle cookies accepteren").click()

    #extracting link to PDF from button
    link_button = page.get_by_role("link", name="Aandelenportefeuille")

    #get PDF
    pdf_path = functions.get_pdf(filename, page, link_button, browser, path)
    pdf = pdfplumber.open(pdf_path)
    playwright.stop()
    
    #defining regular expression patterns to match PDF
    tabs = []
    table_pattern = re.compile(r'^(.+?)\s+([\d.]+,\d+)$', re.MULTILINE)
    date_pattern = re.compile(r'\b\d{2}\s+[A-Za-z]+\s+\d{4}\b')

    #extracting the information from PDF
    for page in pdf.pages:
        text = page.extract_text()
        if text: #matching the actual table, appending matching text to list
            matches = table_pattern.findall(text)
            for match in matches:
                tabs.append(match)
        if text: #matching the date published
            matches = date_pattern.findall(text)
            for match in matches:
                date_str = match

    #changing locale to the netherlands in order to translate the extracted date, and converting the month from string to number format
    locale.setlocale(locale.LC_ALL, 'nl_NL.UTF-8') 
    number_month=datetime.strptime(date_str[3:-5], "%B").strftime("%m") 

    #correctly reformating date
    date = date_str[-4:] + "-" + number_month + "-" + date_str[:2]

    #creating dataframe from list of extracted text and values
    df = pd.DataFrame(tabs, columns=["Issuer - Name", "Security - Market Value - Amount"])

    #adding the remaining columns and constants
    df["Security - Report Date"] = date
    df["Shareholder - Name"] = shareholder
    df["Security - Market Value - Multiplier"] = multiplier
    df["Security - Market Value - Currency Code"] = currency
    df["Data Source URL"] = URL

    #reordering columns to match IDI's order
    df = df[["Shareholder - Name", "Issuer - Name", "Security - Report Date", "Security - Market Value - Amount", "Security - Market Value - Multiplier", "Security - Market Value - Currency Code", "Data Source URL"]]

    # removing commas and periods from market value. this is essentially multiplying by 10, since the original values each have
    # 1 decimal point and the original multiplier was x1000, the multiplier for the finished dataframe here is only x100.

    no_decimals = []
    for amount in df["Security - Market Value - Amount"]:
        no_decimals.append(re.sub(r'[,.]', '', amount))

    df["Security - Market Value - Amount"] = no_decimals


    # removing country subheadings from the dataframe, as regular expression views them as companies. 
    # importing a list of Dutch countries

    csv_dir = Path(__file__).parent
    csv_dir = csv_dir/"supplement/dutchcountries.csv"
    countries = pd.read_csv(csv_dir)
    countries = sum(countries.values.tolist(), [])

    # finding which countries have matches and finding their indices
    matching_entries = df["Issuer - Name"].isin(countries)
    index = []
    for i in range(len(matching_entries)):
        if matching_entries[i]:
            index.append(i)

    # removing indicies which are countries
    df = df.drop(index)

    #export dataframe!
    functions.export_df(df, filename, path)

#if run outside of main
if __name__ == "__main__":
    scrape_bpfbouw()