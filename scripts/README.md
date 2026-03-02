# Individual scripts
Purpose - To document the high level approach taken to each individual script. Sections ordered in alphabetical order.

# Scrapers
### ap2.py
Scrapes AP2, a Swedish based company that manages pension funds in buffers. Scraper navigates to the AP2 webpage with historical documents, searches for and downloads the most recent reports for Swedish and Foreign equities. Then, for each pdf it extracts the entries in two segments, as there is no simple/consistent way to seperate the columns "Number" and "Market value" with regular expressions. It then reformats and combines these extractions, and only then regular expressions are able to be used to sort through them. Lastly, a dataframe is created and exported to TSV. No manual steps needed unless the website or format changes. Note: This scraper creates two PDFs and two TSVs.

### ap3.py
Scrapes AP3, a manager for the Swedish public's pension assets. Scraper navigates to AP3 data page, searching for the most recent pdfs in the categories: Swedish, foreign, fixed, and private, downloading them, and returning a list of directories. Then, one-by-one the pdfs are parsed through to find matching entries, the entries are formatted into dataframes, and exported to TSVs.  No manual steps needed unless the website or format changes. Note: This scraper creates four PDFs and four TSVs.

### kpa.py
Scrapes KPA pensions, a group of companies based in Sweden that offer pension management, insurance, asset managment, and more. Scraper navigates to pdf preview and downloads, then filters for entries based on text size. Then, the data is formatted into a dictionary and exported as a TSV. No manual steps needed unless the website or format changes.

### pka.py
Scrapes Pensionskassernes Administration a/s, a Denmark based company that claims to invest in causes that comply with the EU's green and social agenda. Scraper begins by downloading pdf with playwright, while attempting to deny cookies if prompted. Then, in a context manager extracts entries and checks for matches with pdfplumber and regular expressions, before writing to a TSV. No manual steps needed unless the website or format changes.

### pmt.py
Scraper for PMT - The Metal and Technology Pension Fund from the Netherlands. This pension fund has their investments stored in the html of their website. This scraper navigates to the webpage, scrapes the html, writes it to an html file, and then finds and scrapes the tabular information, which is the issuing company and the market value in euros. In order to find the company name, we checked if the string 'Bedrijfsnaam' was part of the html, and was then removed. No manual steps will be needed unless PMT changes the url or the format of their html tables.

### vervoer.py
Scrapes Pensioenfonds Vervoer, a nonprofit pension fund in transport, based in the Netherlands. Scraper navigates to pdf preview and downloads, then extracts and formats text to be filtered out with regular expressions. Additional filtering and formatting done in tandum as entries are prepared to be exported. Exports to TSV. No manual steps needed unless the website or format changes.

# Other
### data_cleanup.py
Script to clean up data output folder. Sorts through directories to find data output folders specifically, then loops through each folder to group dates into quarters. Then, picks the earliest month from each quarter to keep, with the latest day used as a tiebreaker. Lastly, all other files are deleted. Currently not meant to be called as a function.

### functions.py
Custom functions module, to be called from other scripts to streamline aspects that repeat logic.

##### create_path(name)
Creates a folder and returns a path object in the format "name\YYYY-MM-DD"

##### get_pdf(filename, page, link_button, browser=None, path=None)
Downloads a pdf if given the button/link on a webpage that leads to a PDF preview. Returns path to pdf.

##### export_df(df, filename, path)
Exports dataframe to TSV with pandas.

#### get_pdf_date(pdf)
Uses pdfplumber to get pdf creation date. Takes pdf object, returns string in format of "YYYY-MM-DD."