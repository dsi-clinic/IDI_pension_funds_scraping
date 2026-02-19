'''Scrapes AP2, a Swedish based company that manages pension funds in buffers. Scraper navigates to the AP2 webpage with historical documents, searches for and downloads the most recent reports for Swedish and Foreign equities. Then, for each pdf it extracts the entries in two segments, as there is no simple/consistent way to seperate the columns "Number" and "Market value" with regular expressions. It then reformats and combines these extractions, and only then regular expressions are able to be used to sort through them. Lastly, a dataframe is created and exported to TSV. No manual steps needed unless the website or format changes. Note: This scraper creates two PDFs and two TSVs.'''
#Python Modules
import re
import datetime
import requests

#External Modules
from playwright.sync_api import sync_playwright
import pandas as pd
import pdfplumber

#If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions


'''Main function'''
def scrape_ap2():
    #Setup

    #Set constants
    shareholder = "ap2"
    url = "https://ap2.se/en/asset-management/holdings/"
    path = functions.create_path(shareholder)
    year = int(datetime.date.today().year)

    #Get URLs to pdfs with Playwright
    playwright = sync_playwright().start()

    #Establish page and browser
    browser = playwright.chromium.launch(headless=True, slow_mo=1, channel="chromium")
    page = browser.new_page()

    #Go to page that leads to PDF
    page.goto(url)

    #Reject Cookies
    cookies_button = page.get_by_role('button', name="Deny")
    cookies_button.click()

    #Get all links on download page
    link_locators = page.get_by_role('link').all()




    #Match PDFs based on date and keywords. Save url, and last date updated into list.
    swedish_pdf = []
    foreign_pdf = []
    while not swedish_pdf and not foreign_pdf:
        #Dynamic pattern, tries latest year and goes back a year if failed.
        pattern = ".+/uploads/" + str(year) + "/.+"

        #For every link, 
        for link in link_locators:
            #Get URL embedded in metadata
            link = link.get_attribute('href')
            #Search date
            match = re.search(pattern, link)

            #If link matches the year, search for keywords
            if match:
                #Match must be string
                match = str(match.group())
                #Search for data and Svenska (Swedish)
                search_match = re.search(".+(\d{4}_\d{2}_\d{2})_[Ss]venska_.+", match)
                if search_match:
                    #Append link
                    swedish_pdf.append(str(search_match.group()))
                    #Append date
                    swedish_pdf.append(search_match.group(1))
                else:
                    #If failed, search for date abd Utlandska (foreign)
                    search_match = re.search(".+(\d{4}_\d{2}_\d{2})_[Uu]tlandska_.+", match)
                    if search_match:
                        foreign_pdf.append(str(search_match.group()))
                        foreign_pdf.append(search_match.group(1))

            #After first two matches found, break loop
            if swedish_pdf and foreign_pdf:
                break
        #If failed, go back a year and try again
        year -= 1




    #Downloading the PDFs

    #Get data on pdf 1
    r = requests.get(swedish_pdf[0])
    #Copy data to new file
    with open(path/"raw_ap2_swedish.pdf", 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192): 
            if chunk:
                f.write(chunk)

    #Get data on pdf2
    r = requests.get(foreign_pdf[0])
    #Copy
    with open(path/"raw_ap2_foreign.pdf", 'wb') as f: 
        for chunk in r.iter_content(chunk_size=8192): 
            if chunk:
                f.write(chunk)

    #Stop playwright
    playwright.stop()




    #Extracting data - Swedish

    #Open PDF
    pdf = pdfplumber.open(path/"raw_ap2_swedish.pdf")

    #Set vars
    tabs = [] 
    offset = 114 #Y Offset for page 1
    length = len(pdf.pages) #Length of pdf
    count = 0 #Counts through while statement

    #Extract text in two sections, as columns "Number" and "Market Value" can not be seperated with regex consistently/easily
    while count < length:
        #Setup page
        page = pdf.pages[count] 
        left = page.crop((0, offset, 373, page.height)) #All columns up to Market Value
        right = page.crop((373, offset, page.width, page.height)) #Market value and all columns after
        offset = 0 #After first page, offset uneeded


        #Extract text
        temp1 = left.extract_text_lines(return_chars=False)
        temp2 = right.extract_text_lines(return_chars=False)


        #Iterate through page
        count2 = 0
        length2 = len(temp1) #Length of entries
        while count2 < length2:
            #Seperate matches via dash instead of space
            tabs.append(temp1[count2]['text'] + "-" + temp2[count2]['text'])
            count2 += 1


        #Used to iterate as many times as there are pages
        count += 1





    #Regex - Swedish

    #Establish pattern. Follows schema of pdf columns.
    pattern = re.compile(r'^(?P<ShareholderName>.+?)\s+(?P<ISIN>SE\d{10})\s+(?P<Country>[A-Z]{2})\s+(?P<NumberOfShares>[\d\s]+)-(?P<MarketValue>[\d\s]+)\s+(?P<ShareCapital>[\d,.]+%)\s+(?P<VotingCapital>[\d,.]+%)$')

    #Set constants
    report_date = str(swedish_pdf[1])
    report_date = report_date.replace("_", "-") #Format date
    multiplier = "x1_000"

    #Search through entries, if match, append to data following IDI column schema.
    data = []
    for tab in tabs:
        match = pattern.search(tab)
        if match:
            data.append([shareholder, match.group(1), match.group(3), match.group(2), report_date, match.group(5), match.group(4), multiplier, match.group(6), match.group(7), url])




    #Export - Swedish

    #Create data
    df = pd.DataFrame(data, columns=["Shareholder - Name", "Issuer - Name", "Issuer - Country Code", "Security - ISIN", "Security - Report Date", "Security - Market Value - Amount", "Security - Market Value - Multiplier", "Stock - Number of Shares" , "Stock - Percent Ownership", "Stock - Percent Voting Power", "Data Source URL"])
    #Export as TSV
    functions.export_df(df, "ap2_swedish", path)




    #Extracting Data - Foreign

    #Open pdf
    pdf = pdfplumber.open(path/"raw_ap2_foreign.pdf")

    #Set vars
    tabs = []
    offset = 132 #Y Offset for page 1
    length = len(pdf.pages) #Length of document
    count = 0 #Counts through while statement

    #Extract text in two sections, as columns "Number" and "Market Value" can not be seperated with regex consistently/easily
    while count < length:
        #Set vars
        page = pdf.pages[count]
        left = page.crop((0, offset, 456, page.height))
        right = page.crop((456, offset, page.width, page.height))
        offset = 0

        #Extract text
        temp1 = left.extract_text_lines(return_chars=False)
        temp2 = right.extract_text_lines(return_chars=False)

        #Iterate through each page
        count2 = 0
        length2 = len(temp1) 
        while count2 < length2:
            #Seperate matches via dash instead of space
            tabs.append(temp1[count2]['text'] + "-" + temp2[count2]['text'])
            count2 += 1

        #Iterate through while statement as many times as there are pages
        count += 1




    #Regex - Foreign

    #Pattern following pdf column schema.
    pattern2 = re.compile(r'^(?P<Name>.+?)\s+(?P<ISIN>[A-Z]{2}[A-Z0-9]{9,})\s+(?P<Country>[A-Z]{2})\s+(?P<NumberOfShares>[\d\s]+)-(?P<MarketValue>[\d\s]+)$')

    #Set report date
    report_date = str(foreign_pdf[1])
    report_date = report_date.replace("_", "-")

    #Search through entries, if match, append to data following IDI column schema.
    data = []
    for tab in tabs:
        match = pattern2.search(tab)
        if match:
            data.append([shareholder, match.group(1), match.group(3), match.group(2), report_date, match.group(5), match.group(4), url])




    #Export Data - Foreign
    #Create dataframe following IDI column schema.
    df2 = pd.DataFrame(data, columns=["Shareholder - Name", "Issuer - Name", "Issuer - Country Code", "Security - ISIN", "Security - Report Date", "Security - Market Value - Amount", "Stock - Number of Shares", "Data Source URL"])
    #Export as TSV
    functions.export_df(df2, "ap2_foreign", path)



#Run function locally
if __name__ == "__main__":
    scrape_ap2()