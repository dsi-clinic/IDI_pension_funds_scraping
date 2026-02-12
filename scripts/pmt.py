"""Scraper for PMT - The Metal and Technology Pension Fund from the Netherlands. This pension fund has their investments stored in the html of their website. This scraper navigates to the webpage, scrapes the html, writes it to an html file, and then finds and scrapes the tabular information, which is the issuing company and the market value in euros. No manual steps will be needed unless PMT changes the url or the format of their html tables."""

# standard library imports
from datetime import datetime
import re
import requests

# third party imports
from bs4 import BeautifulSoup
import pandas as pd

#If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions

def scrape_pmt():

    '''Scrapes the PMT pension fund.'''

    # going to the url and getting the html, then writing it to the data folder
    url = "https://www.pmt.nl/over-pmt/zo-beleggen-we/waar-beleggen-we-in/aandelen-en-obligaties/#"
    html = requests.get(url)
    parsed = BeautifulSoup(html.content, 'html.parser')
    filename = "pmt"
    path = functions.create_path(filename)

    with open(f"{path}/{filename}.html",'w') as html_file:
        html_file.write(str(parsed))

    # scraping the tabular values of the issuing company name and the market value
    tables = parsed.find_all("table")
    companies = []
    values = []
    numbers = r'[\d.]+'
    company_name = r'Bedrijfsnaam'
    for table in tables:
        body = table.find("tbody")
        info = body.find_all("td")
        for inf in info:
            # the current html structure doesn't distinguish between an entry that is the company name or the market value
            # so to find the ones that are company names, we look for the Danish word for company, and then start the scrape after that word
            if re.search(company_name, inf.text):
                companies.append(inf.text[12:])
            else:
                # if the Danish word for company isn't there, find the digits in the html, which will be the market value
                values.append(re.search(numbers, inf.text).group())

    # write the scraped information to a dataframe, with proper column headings matching the schema
    companies_ser = pd.Series(companies, name = "Issuer - Name")
    values_ser = pd.Series(values, name = "Security - Market Value - Amount")
    df = pd.concat([companies_ser, values_ser], axis = 1)

    #find and scrape the date of the report - which is in DD - Month - YYYY format
    date_pattern = r'[\d]+\s[a-zA-Z]+\s[0-9][0-9][0-9][0-9]'
    date_string = re.search(date_pattern, parsed.text).group()
    date_of_report = datetime.strptime(date_string, "%d %B %Y")

    #add necessary columns of the table, as per the schema
    df['Shareholder - Name'] = "Metal and Technology Pension Fund"
    df['Security - Report Date'] = date_of_report
    df['Security - Market Value - Currency Code'] = "EUR"
    df['Data Source URL'] = url

    #write the dataframe as a tsv to the appropriate directory
    functions.export_df(df, filename, path)

if __name__ == "__main__":
    scrape_pmt()