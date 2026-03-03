
'''Scrapes the fourth Swedish National Pension fund, AP4. Scraper naviagtes to AP4 holdings page with playwright, grabs a list of all links on it, and searches with regex starting from the most recent link until it finds a match. The PDF downloads with requests, and then is scraped with pdfplumber and regex. While extracting the text, each line is split into to and then remerged with ! as a seperator, as the columns "No of Shares" and "Fair Value" would otherwise be difficult/unreliable to search through with regex. Matched data is added to either to a Swedish dataframe or Foreign one depending on country of origin, then 2 tsvs are created (as they were last time AP4 was scraped). No manual steps needed unless the website or format changes.'''
#Python modules
import re
import requests
import datetime
#External modules
import pdfplumber
import pandas as pd
from playwright.sync_api import sync_playwright

#If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions




'''Main function'''
def scrape_ap4():

    #-----------------------------/Setup/---------------------------#

    #Set constants
    shareholder = "AP4"
    url = "https://www.ap4.se/en/reports/holdings/"
    path = functions.create_path("ap4")



    #Playwright Start
    playwright = sync_playwright().start()

    #Establish page and browser
    browser = playwright.chromium.launch(headless=True, slow_mo=1, channel="chromium")
    page = browser.new_page()

    #Go to page that leads to PDF
    page.goto(url)

    #Click cookies if present (sometimes does not happen in headless mode.)
    try:
        cookies_button = page.get_by_role('button', name="Only necessary")
        cookies_button.click()
    except:
        pass

    #Get list of links on page
    link_locators = page.get_by_role('link').all()

    #Set variables for while loop
    pdf_link = ""
    year = int(datetime.date.today().year)



    #Until matching link is found, search for every link in a year.
    while not pdf_link:

        #Pattern to search for. The way the website is organized, this should always pick the most recent entry.
        link_pattern = ".+" + "-listed" + ".+" + str(year) + ".+"

        #For each link, apply pattern.
        for link in link_locators:

            #Access link in metadata
            link = link.get_attribute('href')

            #Apply pattern. If match, break for loop and end while loop.
            match = re.search(link_pattern, link)
            if match:
                pdf_link = "https://ap4.se" + link
                break

        #If failed, try previous year.
        year -= 1



    #Write pdf data to directory with requests.
    pdf_path = path/"raw_ap4.pdf"
    with open(pdf_path, 'wb') as f: 
        r = requests.get(pdf_link)
        for chunk in r.iter_content(chunk_size=8192): 
            if chunk:
                f.write(chunk)


    #Stop playwright
    playwright.stop()




    #-----------------------------/Scrape PDF/---------------------------#

    with pdfplumber.open(pdf_path) as pdf:

        #Get date of pdf
        report_date = functions.get_pdf_date(pdf)

        #Columns "No of Shares" and "Fair Value" not easily/consistently seperated with regex. Addressed while extracting text here.
        text = ""
        for page in pdf.pages:

            #Divide the page into 2 and extract each part seperatley.
            left = page.crop((0, 0, 260, page.height), strict=False)
            right = page.crop((260, 0, page.width, page.height), strict=False)

            left = left.extract_text_lines(return_chars=False)
            right = right.extract_text_lines(return_chars=False)

            #For each line, stich the 2 parts back together with exclamation marks used as seperators, and add to string.
            for i, d in enumerate(right, start=0):
                text = text + left[i]['text'] + "!" + right[i]['text'] + "!!!"
        


        #Search for: Word beginning with a capital letter or digit, followed by any letter, digits, spaces, and symbols; Space; Two captial letters; Space; Any number of digits and spaces;!;Any number of digits and spaces;Space;Digits and capital letters, followed by space, followed by 2 captial letters; Space; a digit, a comma, and 2 digits; Space; a digit, a comma, and 2 digits;!!!. Sometimes ownership, power, and isin missing, addressed by ?
        listed_pattern = re.compile("(?P<issuer>[A-Z\d][A-Za-z\d \-+&'/]+) (?P<issuer_country>[A-Z]{2}) (?P<shares>[\d ]+)!(?P<value>[\d ]+) ?(?P<isin>[A-Z\d]+)? ?(?P<ticker>[A-Z\d]+ [A-Z]{2})? ?(?P<ownership>\d,\d{2})? ?(?P<power>\d,\d{2})?!!!")

        #Create lists for matches (2 lists for 2 dataframes, in accordance to previous ap4 scraping.)
        entries_swedish = []
        entries_foreign = []
        #Search entire string
        matches = re.findall(listed_pattern, text)
        for match in matches:
            #Set variables equal to groups in match
            issuer, issuer_country, shares, value, isin, ticker, ownership, power = match
            #Create entry according to IDI schema
            append_list = [shareholder, issuer, issuer_country, isin, report_date, value, shares, ownership, power, url]
            #If country is Sweden, append to Swedish list. For others, append to foreign list.
            if issuer_country == "SE":
                entries_swedish.append(append_list)
            else:
                entries_foreign.append(append_list)


        
        #Create ande export swedish dataframe as tsv in accordance to IDI schema
        df_swedish = pd.DataFrame(entries_swedish, columns = ["Shareholder - Name", "Issuer - Name", "Issuer - Country Name", "Security - ISIN", "Security - Report Date", "Security - Market Value", "Stock - Number of Shares", "Stock - Percent Ownership", "Stock - Percent Voting Power", "Data Source URL"])
        functions.export_df(df_swedish, "ap4_swedish", path)
        #Create ande export foreign dataframe as tsv in accordance to IDI schema
        df_foreign = pd.DataFrame(entries_foreign, columns = ["Shareholder - Name", "Issuer - Name", "Issuer - Country Name", "Security - ISIN", "Security - Report Date", "Security - Market Value", "Stock - Number of Shares", "Stock - Percent Ownership", "Stock - Percent Voting Power", "Data Source URL"])
        functions.export_df(df_foreign, "ap4_foreign", path)




#---------/Scrape Locally/---------#
if __name__ == "__main__":
    scrape_ap4()