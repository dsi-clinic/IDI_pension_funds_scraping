# Individual scripts
Purpose - To document the high level approach taken to each individual script. Sections ordered in alphabetical order.

# Scrapers
### amf.py
Scrapes AMF Pension, manager for over four million pension holders, based is Sweden. Scraper finds most recent pdf and downloads using requests. Then, it begins looping through each page of the document, using a combination of keywords, page cropping, font type, and text size to filter out desired entries. Additionally, the security type is stored using a match statement and applied to following entries until a new security type is listed, or the page ends. No manual steps needed unless the URL format or Pdf format changes. Note: This pdf has a lot of edge cases, so the likelyhood that this one needs to be tweaked or redone in the future is high.

### ap2.py
Scrapes AP2, a Swedish based company that manages pension funds in buffers. Scraper navigates to the AP2 webpage with historical documents, searches for and downloads the most recent reports for Swedish and Foreign equities. Then, for each pdf it extracts the entries in two segments, as there is no simple/consistent way to seperate the columns "Number" and "Market value" with regular expressions. It then reformats and combines these extractions, and only then regular expressions are able to be used to sort through them. Lastly, a dataframe is created and exported to TSV. No manual steps needed unless the website or format changes. Note: This scraper creates two PDFs and two TSVs.

### ap3.py
Scrapes AP3, a manager for the Swedish public's pension assets. Scraper navigates to AP3 data page, searching for the most recent pdfs in the categories: Swedish, foreign, fixed, and private, downloading them, and returning a list of directories. Then, one-by-one the pdfs are parsed through to find matching entries, the entries are formatted into dataframes, and exported to TSVs. No manual steps needed unless the website or format changes. Note: This scraper creates four PDFs and four TSVs.

### ap4.py
Scrapes the fourth Swedish National Pension fund, AP4. Scraper naviagtes to AP4 holdings page with playwright, grabs a list of all links on it, and searches with regex starting from the most recent link until it finds a match. The PDF downloads with requests, and then is scraped with pdfplumber and regex. While extracting the text, each line is split into to and then remerged with ! as a seperator, as the columns "No of Shares" and "Fair Value" would otherwise be difficult/unreliable to search through with regex. Matched data is added to either to a Swedish dataframe or Foreign one depending on country of origin, then 2 tsvs are created (as they were last time AP4 was scraped). No manual steps needed unless the website or format changes.

### ap7.py
Scrapes AP7, a "building block in the national pension system’s premium pension component" in Sweden. Downloads static HTML file with requests, remformats and matches using regular expressions, and saves as TSV. No manual steps needed unless the website or format changes.

### bpfbouw.py
Scrapes bpfBOUW, a Dutch company that manages pension funds in the construction industry. Scraper navigates to the bpfBOUW website and downloads the most recent PDF of their shareholder report. Scraper uses regular expression to extract information, and does additional reformatting including extracting the date. A csv file of dutch countries is imported, and removes countries the regex views as companies. Exports to tsv.

### bpl.py
Scrapes BPL pension, a Dutch pension company for employees in agriculture or green energy. Scraper navigates to the downloads page of the BPL website, and uses playwright and requests to download the pdf. Then, it loads the pdf with pdfplumber and sorts through entries with regular expressions. Lastly, the data is formatted and exported as a tsv. No manual steps needed unless the website or format changes.

### detailhandel.py
Scrapes Pensioen Funds Detailhandel, a company based in the Netherlands that manages the investments of non-food retail employees and those retired from the field. This scraper naviagtes to the website, finds the link to the pdf, saves it, and uses it to download the pdf (Note: get_pdf function was not used here, as window was not a popup.) Then, the pdf was split into two sections per page to account for edge cases, and desired attributes for each were saved into a list (Note: memory became an issue on this one, so only the desired attributes were saved, and all others were deleted). Then, regex is used to filter entries, the data is formatted, then exported as a tsv. No manual steps needed unless the website or format changes.

### kpa.py
Scrapes KPA pensions, a group of companies based in Sweden that offer pension management, insurance, asset managment, and more. Scraper navigates to pdf preview and downloads, then filters for entries based on text size. Then, the data is formatted into a dictionary and exported as a TSV. No manual steps needed unless the website or format changes.

### lacera.py
Scrapes LACERA, a pension fund for Los Angelos County's retirees. Scraper parses through html content of data source page and uses regex to find link to PDF. PDF is downloaded with requests. Searches for entries with Regex one page at a time for memory. Entries are formatted and converted to dataframe. DF saved as TSV. No manual steps needed unless the format of the website, pdf link, or format changes.

### nbim.py
Scrapes Norges Bank Investment Managment, a Norwegian government pension fund created in 1969 after and in direct reponse to the discovery of oil in the North Sea. The data is already formatted in a csv format, so this scraper simply searches for the most recent one with requests, downloads it, and then reformats it according to IDI schema. Downloads 1 CSV and saves 1 TSV. No manual steps needed unless the format of csvs or their URLS change.

### pka.py
Scrapes Pensionskassernes Administration a/s, a Denmark based company that claims to invest in causes that comply with the EU's green and social agenda. Scraper begins by downloading pdf with playwright, while attempting to deny cookies if prompted. Then, in a context manager extracts entries and checks for matches with pdfplumber and regular expressions, before writing to a TSV. No manual steps needed unless the website or format changes.

### pensiondanmark.py
Scrapes PensionDanmark: a Danish labor market pension fund. Scrapes a static html with BS4. Scraper downloads and format html data, then sorts through it using regex. Output exported as tsv. No manual steps needed unless the website or format changes.

### pmt.py
Scraper for PMT - The Metal and Technology Pension Fund from the Netherlands. This pension fund has their investments stored in the html of their website. This scraper navigates to the webpage, scrapes the html, writes it to an html file, and then finds and scrapes the tabular information, which is the issuing company and the market value in euros. In order to find the company name, we checked if the string 'Bedrijfsnaam' was part of the html, and was then removed. No manual steps will be needed unless PMT changes the url or the format of their html tables.

### pme.py
Scraper for PME pensioenfonds: A Dutch pensionfund for employees in metal and tech industries. Info stored in a dynamic HTML, which takes approximately 5-10 mins to scrape with playwright. Scraper establishes connection to data source page with playwright, and extracts and formats needed information such as the next button, the equity number of pages, and the report date. Then, a while loop is created both to save raw data, and loop through pages to format said data. This script is quite slow, so to combat that it adds its own progress to the main log. Additionally, this script sometimes fails due to a bad network. If failed, try to run again once or twice before giving up. If there is a better solution, it would involve extracting the raw json file that stores the data, which is currently unavailable. But, this script is fully automatic and won't need to be changed unless the format of the website does.

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

##### get_pdf_date(pdf)
Uses pdfplumber to get pdf creation date. Takes pdf object, returns string in format of "YYYY-MM-DD."

##### convert_month(month)
Convert month as a word into formatted digit (DD)

##### download_file(request, full_filename, path, chunk_size=8192)
Download a file by copying data with binary.