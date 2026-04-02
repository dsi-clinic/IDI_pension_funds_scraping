'''Scrapes LACERA, a pension fund for Los Angelos County's retirees. Scraper parses through html content of data source page and uses regex to find link to PDF. PDF is downloaded with requests. Searches for entries with Regex one page at a time for memory. Entries are formatted and converted to dataframe. DF saved as TSV. No manual steps needed unless the format of the website, pdf link, or format changes.'''
#Python Module
import re

#External Modules
import pdfplumber
import pandas as pd
import requests
from bs4 import BeautifulSoup

#Import our functions locally if script run locally
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions




'''Main function'''
def scrape_lacera():

    #---------------------/Setup and Get PDF/------------------#

    #Make path for output
    path = functions.create_path("lacera")
    #Url to data source page
    url = "https://www.lacera.gov/accountability/investment-holdings"


    #Request response from website
    request_html = requests.get(url)
    #Get content of response
    parsed = BeautifulSoup(request_html.content, 'html.parser')
    #Convert content to string
    parsed = str(parsed)

    #Search content for desired link with regex (https://, anything, public, words numbers and underscores, .pdf)
    match = re.search("https://.+public[\w\d_]+\.pdf", parsed)
    #Get URL
    pdf_url = match.group()
    #Delete parsed info of data source (for memory)
    del parsed

    #PDF data request
    req = requests.get(pdf_url)
    #Download data and save path
    pdf_path = functions.download_file(req, "raw_lacera.pdf", path)




    #-------------------/Scrape and Format Data/----------------------#

    #Open PDF as instance of pdf class
    pdf = pdfplumber.open(pdf_path)


    #Extract text of page 1 to find report date
    text = pdf.pages[1].extract_text()
    #Search text for report date
    report_date = re.search("(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{4})", text)
    #Split report date into 3 vars
    month, day, year = report_date.groups()

    #Day and month should both be double digits. If they are only one, add 0 to beginning of string
    if len(day) < 2:
        day = "0" + day
    if len(month) < 2:
        month = "0" + month
    #Assemble formatted report date
    report_date = year + "-" + month + "-" + day


    #Set other constants
    shareholder = "LACERA"
    currency = "USD"
    multiplier = "x1"


    #Vars for loop
    text = ""
    entries = []

    #Regex pattern for entries. Follows column schema closely. All entries begin with \n
    #Edge cases: To easily distinguish between fund name and sectypes, sectypes find 1 of 6 set possibilities; Isin is optional; Spaces are 1 or more to account for blank columns (mainly ISIN)
    pattern = re.compile("\n(?P<fund_name>[A-Z\d \-\.]+) +(?P<sectype>EQUITY|FIXED INCOME|CASH|CASH EQUIVALENT|BOND|CORPORATE BOND) +(?P<isin>[A-Z\d]+)? +(?P<issuer>[A-Z\d\t ,+/\-\.]+) +(?P<shares_par>[\d,]+\.\d{3}) +(?P<base_price>[\d,]+\.\d+) +(?P<value>[\d,]+\.\d+)")


    #Loop through a page, find matches, then again until all pages are looped through
    for p in pdf.pages:

        #Extract text with layout entact to help distinguish when a column is blank
        text = p.extract_text(layout=True)

        #Apply regex
        matches = re.findall(pattern, text)
        #Per match found, format and append to entries
        for m in matches:
            #Split into vars
            fund_name, sectype, isin, issuer, shares_par, base_price, value = m
            #Append according to IDI schema
            entries.append([shareholder, issuer.strip(), sectype.strip(), isin.strip(), report_date, value.strip(), multiplier, currency, url])


    #----------/Export/---------#

    #Create df according to IDI schema
    df = pd.DataFrame(entries, columns=["Shareholder - Name","Issuer - Name","Security - Type","Security - ISIN","Security - Report Date", "Security - Market Value - Amount", "Security - Market Value - Multiplier", "Security - Market Value - Currency Code", "Data Source URL"])
    #Export as TSV
    functions.export_df(df, "lacera", path)




#-----/Run function locally/-------#
if __name__ == "__main__":
    scrape_lacera()