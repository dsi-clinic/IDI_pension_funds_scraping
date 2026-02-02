'''Python Modules'''
import datetime
import re
from pathlib import Path
import io

'''External Modules'''
from playwright.sync_api import sync_playwright
import pdfplumber
import pandas as pd
import requests



'''Create path that goes data > name > time, with name being parameter'''
def create_path(name="no_name"):
    #Get and format time
    time = datetime.datetime.now()
    time = time.strftime("%m %d %Y")

    #Format directory
    script_dir = Path(__file__).parent.parent #Double parent as It goes functions, to scripts, to repository
    path = script_dir / 'data' / name / time

    #Create and return directory
    path.mkdir(parents=True, exist_ok=True)
    return path




'''
Download pdf and return open instance of pdfplumber pdf class ASSUMING you have defined link_button as the link that will open google pdf preview.
filename = org (Ex: vervoer, danica, etc)
page = browser page (playwright)
link_button = button to be clicked (playwright)
browser = browser instance (playwright)
path = file path (pathlib), if left undefined it will run within the function
download = Toggle on wether or not it downloads
'''
def get_pdf(filename, page, link_button, browser, path=None, download=True):
    #Clicks link_button and stores info
    with page.expect_popup() as popup_info:
        link_button.click()
    
    #Grabs url and ensures it is able to be accessed
    url = popup_info.value.url 
    r = requests.get(url)

    if download == True:
        #Setup filename
        filename = "raw_" + filename + ".pdf"

        #Run create_path if path not provided
        if not path:
            path = create_path(filename)

        #Write pdf data to directory
        with open(path/filename, 'wb') as f: #wb = with binary
            for chunk in r.iter_content(chunk_size=8192): #chunk size to slow download speed (avoid errors)
                if chunk:
                    f.write(chunk)
        
        #Returns pdf object in pdfplumber
        pdf = pdfplumber.open(path/filename)

    else:
        #Save pdf to temporary memory (RAM)
        pdf = io.BytesIO(r.content)
        pdf = pdfplumber.open(pdf)

    #Close browser
    browser.close()

    return pdf

