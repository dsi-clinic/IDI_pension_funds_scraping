'''Imports'''
#python modules
import datetime
from pathlib import Path
import io
#external modules
import pdfplumber
import requests



'''Create path that goes data > name > time, with name being parameter'''
def create_path(name="no_name"):
    #Get and format time
    time = datetime.datetime.now()
    time = time.strftime("%Y-%m-%d")

    #Format directory
    script_dir = Path(__file__).parent.parent #Double parent as It goes functions, to scripts, to repository
    path = script_dir / 'data' / name / time

    #Create and return directory
    path.mkdir(parents=True, exist_ok=True)
    return path




'''
Download pdf and path ASSUMING you have defined link_button as the link that will open google pdf preview.
filename = org (Ex: vervoer, danica, etc)
page = browser page (playwright)
link_button = button to be clicked (playwright)
browser = browser instance (playwright)
path = file path (pathlib), if left undefined it will run within the function
'''
def get_pdf(filename, page, link_button, browser, path=None):
    #Clicks link_button and stores info
    with page.expect_popup() as popup_info:
        link_button.click()
    
    #Grabs url and ensures it is able to be accessed
    url = popup_info.value.url 
    r = requests.get(url)

    #Setup filename
    filename = "raw_" + filename + ".pdf"

    #Run create_path if path not provided
    if not path:
        path = create_path(filename)

    pdf_path = path/filename

    #Write pdf data to directory
    with open(pdf_path, 'wb') as f: #wb = with binary
        for chunk in r.iter_content(chunk_size=8192): #chunk size to slow download speed (avoid errors)
            if chunk:
                f.write(chunk)

    #Close browser
    browser.close()

    return pdf_path



'''Export as tsv given dataframe
df = dataframe object (pandas)
filename = string
path = path object (pathlib)'''
def export_df(df, filename, path=None):
    file_final = filename + ".tsv"

    if not path:
        path=create_path(filename)

    df.to_csv(path/file_final, sep="\t", index=False)



'''Input pdf object from pdfplumber. Return string'''
def get_pdf_date(pdf):
    report_date = pdf.metadata['CreationDate']
    report_date = report_date[2:6] + "-" + report_date[6:8] + "-" + report_date[8:10]
    return report_date