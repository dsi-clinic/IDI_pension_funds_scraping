'''Imports'''
#Python Modules
import re

#External modules
import pdfplumber
import pandas as pd
from playwright.sync_api import sync_playwright

#If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions


def scrape_kpa():
    '''Setup'''
    filename = "KPA"
    url = "https://www.kpa.se/om-kpa-pension/vart-hallbarhetsarbete/ansvarsfulla-investeringar/innehav-och-uteslutna-bolag/"
    path = functions.create_path(filename)


    #Load page
    playwright = sync_playwright().start()

    browser = playwright.chromium.launch(headless=True, slow_mo=5, channel="chromium")
    page = browser.new_page()

    page.goto(url)

    #Reject Cookies
    reject_cookies = page.get_by_role('button', name="Avvisa cookies")
    reject_cookies.click()

    #Find Holdings
    link_button = page.get_by_role('link', name="Innehav", exact=False)


    #Get PDF
    pdf_path = functions.get_pdf(filename, page, link_button, browser, path)
    pdf = pdfplumber.open(pdf_path)



    '''Extract Entries Based on Font Size'''
    entries = []
    for page in pdf.pages:
        #Extract every line in a page, formatted in a list of dictionaries
        text = page.extract_text_lines()

        #For each line, calculate and check height
        for t in text:
            height = t['bottom'] - t['top']
            #If height falls within range, it is a match
            if height > 9 and height < 10:
                entries.append(t['text'])



    '''Formatting Data'''

    #Create columns
    shareholder_name = [filename]
    report_date = [functions.get_pdf_date(pdf)]
    url = [url]

    number_of_entries = len(entries)

    #Repeat constants per number of entries
    shareholder_name = shareholder_name*number_of_entries
    report_date = report_date*number_of_entries
    url = url*number_of_entries

    #Format dataframe in dictionary
    df = {
        "Shareholder - Name" : shareholder_name,
        "Issuer - Name" : entries,
        "Security - Report Date" : report_date,
        "Data Source URL" : url
    }



    '''Export'''
    final_df = pd.DataFrame(df)
    #Export as tsv
    functions.export_df(final_df, filename, path)


if __name__ != "__main__":
    scrape_kpa()