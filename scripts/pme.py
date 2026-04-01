'''Scraper for PME pensioenfonds: A Dutch pensionfund for employees in metal and tech industries. Info stored in a dynamic HTML, which takes approximately 5-10 mins to scrape with playwright. Scraper establishes connection to data source page with playwright, and extracts and formats needed information such as the next button, the equity number of pages, and the report date. Then, a while loop is created both to save raw data, and loop through pages to format said data. This script is quite slow, so to combat that it adds its own progress to the main log. Additionally, this script sometimes fails due to a bad network. If failed, try to run again once or twice before giving up. If there is a better solution, it would involve extracting the raw json file that stores the data, which is currently unavailable. But, this script is fully automatic and won't need to be changed unless the format of the website does.'''
#Python modules
import re
from pathlib import Path
import logging

#External modules
from playwright.sync_api import sync_playwright
import pandas as pd

#If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions
    log_mode = "a" #Add to log
else:
    import functions
    log_mode = "w" #Create log




'''Main function'''
def scrape_pme():

    #-------------/Setup/------------#

    #Create and save directory
    path = functions.create_path("pme")
    #URL to Dynamic HTML
    url = "https://www.pmepensioen.nl/en/investments/we-do-invest-in"

    #Find directory of repository
    parent_dir = Path(__file__).parent.parent
    #Setup logging
    logging.basicConfig(filename=parent_dir/'log.log', level=logging.INFO, filemode=log_mode, format="%(asctime)s - %(message)s")




    #----------------/Get page text/-----------------#

    #Playwright Start
    playwright = sync_playwright().start()

    #Establish page and browser
    browser = playwright.chromium.launch(headless=True, slow_mo=1, channel="chromium")
    page = browser.new_page()

    #Go to page that leads to PDF
    page.goto(url)

    #Cookies (sometimes doesn't appear in headless mode)
    try:
        page.get_by_role('button', name="Decline").click()
    except:
        pass


    #Sometimes playwright struggles to find the button we need. This loop ensures the script runs until it is found.
    next_pension = ""
    while not next_pension:
        #Find all next buttons.
        locator = page.get_by_role('link', name="next", exact=False).all()
        try:
            #First next button is the one that corresponds to equities.
            next_pension = locator[0]
        except:
            pass

    #Scroll to next button to be able to click it.
    next_pension.scroll_into_view_if_needed()

    #Get all text on page
    text = page.inner_text('div')




    #--------------/Get constants/----------------#

    #Find the number of pages as an integer
    page_numbers = re.search("Page 1 of (?P<max>\d+)", text)
    page_numbers = int(page_numbers.group(1))


    #Report date not listed, however the next report date is, as well as the fact that they report every 3 months. Using this, we can approximate report dates.
    #Find date of next report using regex
    next_report_date = re.search("(?P<month>[A-Z][a-z]+) (?P<day>\d+)[a-z]+, (?P<year>\d{4})", text)
    if next_report_date:
        #Split groups into vars
        month, day, year = next_report_date.groups()

        # #Match statement, converts the listed month into the numeric representation of 3 months prior.
        # match month:
        #     case "January":
        #         month = "10"
        #     case "February":
        #         month = "11"
        #     case "March":
        #         month = "12"
        #     case "April":
        #         month = "01"
        #     case "May":
        #         month = "02"
        #     case "June":
        #         month = "03"
        #     case "July": 
        #         month = "04"
        #     case "August":
        #         month = "05"
        #     case "September":
        #         month = "06"
        #     case "October":
        #         month = "07"
        #     case "November":
        #         month = "08"
        #     case "December":
        #         month = "09"
        #     case _: #In case of anything else, month is NA
        #         month = ""

        #Converted to function, but untested in this script. Delete commented code above if works.
        month = functions.convert_month(month, -3)

        #Convert day to double digit if necessary
        if len(day) < 2:
            day = "0" + day

        #Piece together report date in desired format
        report_date = year + "-" + month + "-" + day
    else:
        #If not found, report date is NA
        report_date = ""

    #Establish other constants
    shareholder = "PME pensioenfonds"
    currency = "EUR"
    multiplier = "x1"

    #Setup variables for loop
    count = 0 #Progress loop
    new_text = "" #New text to parse
    old_text = "" #Old text to compare new text to (ensure that page has turned)
    entries = [] #Entries for DF

    #Regular expression to match entries. Follows schema of entries on site, with each category being seperated by a tab. 
    #Edge cases: Symbols and numbers in issuer names. Some country names have multiple words. Some countries have ", Republic of" as a suffix, which the expression does not capture. 
    entry_pattern = re.compile("(?P<issuer>[A-Za-z\d /&\-,]+)\t(?P<value>[\d\.]+)\t(?P<country>[A-Za-z]+(?: [A-Za-z ]+)?)(?:, Republic of)?\t(?P<sector>[A-Za-z ,\-]+)\t(?P<type>[A-Za-z ]+)")




    #-------------/Loop through pages, Scrape data/------------#

    #Open context manager to a text file. Essentially saves a snapshot of every page, just to have some documentation of raw data.
    with open(path/"raw_data_pme.txt", 'w') as file:

        logging.info(f"PME - Begin cycling through pages")

        #Loop through each page
        while count < page_numbers:
            
            #Collect text on a page
            new_text = page.inner_text('div')

            #If new text is not the same as old text, proceed with regex
            if new_text != old_text:

                #New text (unformatted) is now the old text
                old_text = new_text
                #Write unformatted new text to text file
                file.write(new_text)
                

                #Split text by lines
                new_text = new_text.splitlines()

                #For every line
                for line in new_text:
                    #Equity entries don't continue past this text. When reached, break loop.
                    if line == "Investments in the Netherlands":
                        break
                    #If it is a potential equity entry,
                    else:

                        #Apply pattern to 1 line
                        match = re.search(entry_pattern, line)

                        #If match is found,
                        if match:
                            #Split into variables
                            issuer, value, country, sector, sectype = match.groups()
                            #Append an entry according to IDI schema
                            entries.append([shareholder, issuer.strip(), country.strip(), sector.strip(), sectype.strip(), report_date, value.strip(), multiplier, currency, url])
                
                #After all lines are looped through, tick counter
                count += 1

                #Log every 10 pages flipped through
                if count%10 == 0:
                    logging.info(f"PME - Page {count} sucessfully looped through")


            #If new text and old text are the same, try to click again
            else:
                try:
                    next_pension.click()
                except:
                    pass

        
        #At end of pages, close browser, stop playwright, and log success
        browser.close()
        playwright.stop()
        logging.info(f"PME - Page {count} sucessfully looped through")
        logging.info(f"PME - End cycling through pages")



    #-----------/Create and export DF/--------------#

    #Create DF according to IDI schema
    df = pd.DataFrame(entries, columns=["Shareholder - Name", "Issuer - Name", "Issuer - Country Name", "Issuer - Sector", "Security - Type", "Security - Report Date", "Security - Market Value - Amount", "Security - Market Value - Multiplier", "Security - Market Value - Currency Code", "Data Source URL"])
    #Export as tsv
    functions.export_df(df, "pme", path)




#--------/Run function locally/--------#
if __name__ == "__main__":
    scrape_pme()