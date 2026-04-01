'''Scrapes PensionDanmark: a Danish labor market pension fund. Scrapes a static html with BS4. Scraper downloads and format html data, then sorts through it using regex. Output exported as tsv. No manual steps needed unless the website or format changes.'''
#Python module
import re

#External modules
from bs4 import BeautifulSoup
import requests
import pandas as pd

#Import custom module differently depending on where you run from.
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions




'''Main function'''
def scrape_pension_danmark():

    #----------------/Setup/-----------------#

    #Create path for files
    path = functions.create_path("pension_danmark")

    #Url to static HTML
    url="https://www.pensiondanmark.com/en/investments/equity-list/"
    #Request for info
    html = requests.get(url)
    #Download raw HTML file
    functions.download_file(html, "raw_pension_danmark.html", path)


    #Parse HTML data for content 
    parsed = BeautifulSoup(html.content, 'html.parser')

    #Extract text from content
    text=parsed.get_text()
    #Format text for regex. For every 3 new line chars in a row, replace with only one.
    text = re.sub("\n{3}", "\n", text)


    #Search for report date in format Day (digits) Month (word) Year (digits)
    date = re.search("(?P<day>\d{1,2}) (?P<month>[A-za-z]+) (?P<year>\d{4})", text)

    #Format report date
    day, month, year = date.groups()
    #Convert month to digit
    month = functions.convert_month(month)
    #Convert day to double digit (if applicable)
    if len(day) < 2:
        day = "0" + day
    
    #Assemble report date
    report_date = year + "-" + month + "-" + day


    #Establish other entry constants
    shareholder = "PensionDanmark"
    currency = "EUR"
    multiplier = "x1_000_000"




    #------------------/Find entries/---------------------#

    #Regex pattern for entries. After substituion, all entries should be formatted by 2 double new lines. No notable edge cases.
    pattern = re.compile("\n{2}(?P<country>[A-Za-z ]+)\n(?P<issuer>[A-Za-z\d\- &,]+)\n(?P<value>[\d\.]+)\n(?P<sector>[A-Za-z\- &,]+)")
    #Apply regex
    matches = re.findall(pattern, text)


    #Format matches into entries
    entries = []
    for m in matches:
        #Split groups into named variables
        country, issuer, value, sector = m
        #Format 1 entry according to IDI schema
        entries.append([shareholder, issuer, country, sector, report_date, value, multiplier, currency, url])





    #-----------------/Export data/-----------------#

    #Create data frame according to IDI schema
    df = pd.DataFrame(entries, columns=["Shareholder - Name", "Issuer - Name", "Issuer - Country Name", "Issuer - Sector", "Security - Report Date", "Security - Market Value - Amount", "Security - Market Value - Multiplier", "Security - Market Value - Currency Code", "Data Source URL"])
    #Export data as tsv
    functions.export_df(df, "pension_danmark", path)




#--------/Run function locally/--------#
if __name__ == "__main__":
    scrape_pension_danmark()