'''Scrapes Pensioenfonds Vervoer, a nonprofit pension fund in transport, based in the Netherlands. Scraper navigates to pdf preview and downloads, then extracts and formats text to be filtered out with regular expressions. Additional filtering and formatting done in tandum as entries are prepared to be exported. Exports to TSV. No manual steps needed unless the website or format changes.'''
#Python Modules
import re

#External Modules
from playwright.sync_api import sync_playwright
import pandas as pd
import pdfplumber

#If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions


'''Main Function'''
def scrape_vervoer():
    #Setup

    #Create directory for PDFs and CSVs (create_path returns path object used later)
    filename = "vervoer"
    path = functions.create_path(filename)

    #Columns for final dataframe (constants)
    shareholder = "Vervoer"
    URL = "https://www.pfvervoer.nl/over-ons/beleggen/spreiden-van-beleggingen"
    currency = "EUR"
    multiplier = "x1"



    #Playwright Start
    playwright = sync_playwright().start()

    #Establish page and browser
    browser = playwright.chromium.launch(headless=True, slow_mo=500, channel="chromium")
    page = browser.new_page()

    #Go to page that leads to PDF
    page.goto(URL)

    #Save button that leads to pdf preview
    link_button = page.get_by_role('link', name="overzicht van onze beleggingen (pdf)") #likely to break here on future updates of website

    #Get PDF
    pdf_path = functions.get_pdf(filename, page, link_button, browser, path)
    pdf = pdfplumber.open(pdf_path)
    playwright.stop()



    #Extract PDF
    #Fills list with strings containing the text of each page
    tabs = []
    toggle = True
    for page in pdf.pages:
        #Collate all text on a page into one string, with parameters telling python where to find text
        text = page.extract_text(layout = True, x_density=4) 
        #Strips text of new lines, and appends each page to empty list tabs
        tabs.append(text.strip()+"\n") 
        #Store page 1 for later use
        if toggle:
            text2 = text
            toggle = False

    #Combines list entries into one string
    tabs2 = " ".join(tabs)
    #Splits breaks in string into a list of strings
    tabs3 = tabs2.splitlines()
    

    #Setup report_date column
    report_date = re.search("(\d+-\d+-\d+)", text2)
    if report_date:
        report_date = report_date.group(1)
    else:
        report_date = "None"

    

    #Create regex, looking for 2 sets of groups of words followed by numbers, or a dash
    pattern = re.compile("(?P<l_key>[A-Za-z\s,]+?)\s+(?P<l_value>([\d\.]+( [\d\.]+)*,?)|(-))\s+(?P<r_key>[A-Za-z\s,]+?)\s+(?P<r_value>([\d\.]+( [\d\.]+)*,?)|(-))")


    #Create two empty lists so that the order may be maintained
    tabs4 = []
    tabs5 = []
    for tab in tabs3:
        var = None
        var = pattern.search(tab.strip())
        #If match is found in entry, further break it down
        if var:
            #Remove Entries with commas and dashes
            var2 = re.search("(?!\d+?[,-])\d+?", var.group(2))
            if var2:
                #Assemble entry in column order
                list = [shareholder, var.group(1), report_date, var.group(2), multiplier, currency, URL]
                tabs4.append(list)

            var2 = re.search("(?!\d+?[,-])\d+?", var.group(7))
            if var2:
                list = [shareholder, var.group(6), report_date, var.group(7), multiplier, currency, URL]
                tabs5.append(list)
    #Combine lists
    tabs6 = tabs4 + tabs5


    #Create dataframe
    df=pd.DataFrame(tabs6, columns=["Shareholder - Name", "Issuer - Name", "Security - Report Date", "Security - Market Value - Amount", "Security - Market Value - Multiplier", "Security - Market Value - Currency Code", "Data Source URL"])

    #export
    functions.export_df(df, filename, path)


#If run outside of main
if __name__ == "__main__":
    scrape_vervoer()
