'''Custom functions module, to be called from other scripts to streamline aspects that repeat logic.'''
#python modules
import datetime
from pathlib import Path
#external modules
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
Download pdf and path ASSUMING you have defined link_button as the link that will open google pdf preview. (this is pretty situational)
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




'''Input name of month, return month as number as string. Useful for formatting date off of a report. Offset used to calculate past/future reports.'''
def convert_month(month, offset=None):
    
    #Format month
    month = month.lower()

    #Match month to number
    match month:
        case "january":
            month = "1"
        case "february":
            month = "2"
        case "march":
            month = "3"
        case "april":
            month = "4"
        case "may":
            month = "5"
        case "june":
            month = "6"
        case "july": 
            month = "7"
        case "august":
            month = "8"
        case "september":
            month = "9"
        case "october":
            month = "10"
        case "november":
            month = "11"
        case "december":
            month = "12"
        case _: #In case of anything else, month is NA
            month = ""

    #Apply offset if provided (Careful to convert variable types to what we need)
    if offset:
        month = int(month) + int(offset)
        month = str(month)

    #Add 0 to beginning of month if necessary
    if len(month) < 2:
        month = "0" + month

    #Return month number
    return month




'''
Download a file by copying its data with binary.
request = Web request or other iterable content
full_filename = filename with suffix indcluded (pdf, html, etc)
path = path to download to
chunk_size (optional) = size of chunks to iterate at a time in bits
'''
def download_file(request, full_filename, path, chunk_size=8192):

    #Compile path
    file_path = path/full_filename

    #Copy data to location with binary
    with open(file_path, 'wb') as f: 
        #Iterate over chunks
        for chunk in request.iter_content(chunk_size=chunk_size): 
            if chunk:
                f.write(chunk)
    
    #Return location of file
    return file_path