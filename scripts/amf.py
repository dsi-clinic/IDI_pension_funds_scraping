'''Scrapes AMF Pension, manager for over four million pension holders, based is Sweden. Scraper finds most recent pdf and downloads using requests. Then, it begins looping through each page of the document, using a combination of keywords, page cropping, font type, and text size to filter out desired entries. Additionally, the security type is stored using a match statement and applied to following entries until a new security type is listed, or the page ends. No manual steps needed unless the URL format or Pdf format changes. Note: This pdf has a lot of edge cases, so the likelyhood that this one needs to be tweaked or redone in the future is high.'''
#Python modules
import re
import datetime

#External modules
import pdfplumber
import pandas as pd
import requests

#Import our functions locally if script run locally
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions





'''Main function'''
def scrape_amf():

    #-------------------/Setup/-----------------#
    url = ""
    year = int(datetime.date.today().year)

    #Try new URLs until valid one found (Only works because site URL has consistent naming schema)
    while not url:

        #Format for URL
        year_string = str(year)

        #Request object using url
        req_url = requests.get(f"https://www.amf.se/globalassets/pdf/rapporter/innehav_{year_string}.pdf")
        #Status code
        code = req_url.status_code

        #If code is in succesful range, break the loop
        if code > 199 and code < 227:
            url = req_url.url
            break
        #Otherwise, try again with a year down
        year -= 1




    #--------------------/Download and load Pdf/-------------------#

    #Create data folder and path to Pdf
    path = functions.create_path("amf")
    pdf_path = path/"amf.pdf"

    #Copy Pdf data with binary
    with open(pdf_path, 'wb') as f:
        for chunk in req_url.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    #Open PDF
    pdf = pdfplumber.open(pdf_path)
    #Establish constants for entries
    shareholder = "AMF Pension"
    report_date = functions.get_pdf_date(pdf)




    #---------------------/Filter Entries/-------------------#
    
    #Entries in dataframe
    entries = []

    #Loop through every page
    for p in pdf.pages:

        #Extract text in a page
        keyword_check = p.extract_text()
        #Check to see if the page is something we care about
        match = re.search("Svenska aktier|Utländska aktier|Räntebärande tillgångar", keyword_check) #"Swedish shares", "Foreign shares", "Interest-bearing assets"
        
        #If yes,
        if match:

            #PDF Broken up into columns. Create list of sub-pages for each column.
            sections = [p.crop((0, 0, 215, p.height)), p.crop((215, 0, 385, p.height)), p.crop((385, 0, p.width, p.height))]
            #Sections of text to check
            text = ""
            
            #For every section in a page
            for s in sections:

                #Return detailed list of every line in a section
                text = s.extract_text_lines(return_chars=True)
                #Security type for entries - resets after for loop
                sec_type = ""
                
                #For every line of text (1 dictionary in list)
                for t in text:

                    #Access text of a line and check to see what security type, if any, and set sec_type variable equal to that.
                    line = t['text']
                    match line:
                        case "NOTERADE BOLAG": #listed companies
                            sec_type = "Swedish Stock"
                        case "ONOTERADE BOLAG": #unlisted companies
                            sec_type = "Private Swedish Stock"
                        case "FONDER": #funds
                            sec_type = "Swedish Stock"
                        case "LAND OCH BOLAG": #country and company
                            sec_type = "Foreign Stock"
                        case "FÖRETAGSOBLIGATION": #corporate bond
                            sec_type = "Foreign Stock"
                        case "STATSOBLIGATION": #government bond
                            sec_type = "Government Bond"
                        case _: #If anything else, account for edge cases and append to entries.

                            #Size of a word based on subtracting Larger y coord - Smaller y Coord
                            size = t['chars'][0]['bottom'] - t['chars'][0]['top']

                            #If the font name and the size match format of their entries
                            if t['chars'][0]['fontname'] == "TNBONQ+T-Star-Medium" and size == 7:
                                #Filter out watermark and page labels
                                if not re.search("sid|AMF", t['text']):
                                    #Append an entry, with t['text'] being the issuer
                                    entries.append([shareholder, t['text'], report_date, sec_type, url])




    #--------------/Export Data/---------------#

    #Format DF according to IDI schema
    df=pd.DataFrame(entries, columns=["Shareholder - Name", "Issuer - Name", "Security - Report Date", "Security - Type", "Data Source URL"])
    #Export function
    functions.export_df(df, "amf", path)




#Run function locally
if __name__ == "__main__":
    scrape_amf()