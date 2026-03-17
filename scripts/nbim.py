'''Scrapes Norges Bank Investment Managment, a Norwegian government pension fund created in 1969 after and in direct reponse to the discovery of oil in the North Sea. The data is already formatted in a csv format, so this scraper simply searches for the most recent one with requests, downloads it, and then reformats it according to IDI schema. Downloads 1 CSV and saves 1 TSV. No manual steps needed unless the format of csvs or their URLS change.'''
import datetime
import requests
import pandas as pd
if __name__ != "__main__":
    import scripts.functions as functions
else:
    import functions


'''Main Function'''
def scrape_nbim():

    #-----------/Locate CSV with requests/---------#

    #Setup variables
    url = ""
    day = ""
    year = int(datetime.date.today().year) + 1 #Offset as one so that the while loop works

    #Until URL is found, switch between the 2 report dates and subtract yea
    while not url:

        #First checks for December report and subtracts year (accounted for in offset), and if failed, checks for half year report.
        if day == "-12-31":
            day = "-06-30"
        else:
            day = "-12-31"
            year -= 1

        #Assemble date
        date = str(year) + day

        #Request object using url (only works because this website has very consistent url schema)
        req_url = requests.get(f"https://www.nbim.no/api/investments/v2/report/?assetType=eq&date={date}&fileType=csv")
        #Status code
        code = req_url.status_code

        #If code is in succesful range, break the loop
        if code > 199 and code < 227:
            url = req_url
            break




    #-----------/Download Raw Data/---------#

    #Create data folder and path to csv
    path = functions.create_path("nbim")
    csv_path = path/"raw_nbim.csv"

    #Copy csv data with binary
    with open(csv_path, 'wb') as f:
        for chunk in url.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)





    #-----------/Reformat csv/---------#

    #Read data (pandas doesn't like utf-8 encoding)
    data = pd.read_csv(csv_path, sep=";", encoding='utf-16')

    #Get length of a column
    length=len(data['Name'])

    #Setup dictionary according to IDI schema (Some values access columns from dataset directly, others take a variable set in this script and multiply it by the length of the dataset)
    entries = {
        "Shareholder - Name":["Norges Bank Investment Management"]*length,
        "Issuer - Name":data['Name'],
        "Issuer - Country Name":data['Country'],
        "Security - Report Date":[date]*length,
        "Issuer - Sector":data['Industry'],
        "Security - Type":["Equity"]*length,
        "Security - Market Value - Currency":["NOK"]*length,
        "Security - Market Value - Amount":data['Market Value(NOK)'],
        "Stock - Percent Ownership":data['Ownership'],
        "Stock - Percent Voting Power":data['Voting'],
        "Data Source URL":["https://www.nbim.no/en/investments/all-investments"]*length
    }




    #-----------/Export/---------#

    df = pd.DataFrame(entries)
    functions.export_df(df, "nbim", path)





#---------/Scrape Locally/---------#
if __name__ == "__main__":
    scrape_nbim()