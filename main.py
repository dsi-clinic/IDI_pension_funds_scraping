'''Create log file and loop through all listed scrapers.'''
#Python modules
import warnings
import logging
from pathlib import Path


#/------------------Setup-----------------/#

#Supress warnings. (For debug purposes, comment this line.)
warnings.filterwarnings('ignore')

#Ensure log appears in correct directory so that individual scripts may append messages to it
repository_dir = Path(__file__).parent

#Configure logging settings
logging.basicConfig(filename=repository_dir/'log_main.log', level=logging.INFO, filemode="w", format="%(asctime)s - %(message)s")

#Message used to track progress/debug
message = ""


#Dictionary of module:function (Note that the scripts.___ is important for python to find directory). Alphabetical Order.
import_list = {
    "scripts.amf":"scrape_amf",
    "scripts.ap2":"scrape_ap2",
    "scripts.ap3":"scrape_ap3",
    "scripts.ap4":"scrape_ap4",
    "scripts.ap7":"scrape_ap7",
    "scripts.bpfbouw":"scrape_bpfbouw",
    "scripts.bpl":"scrape_bpl",
    "scripts.detailhandel":"scrape_detailhandel",
    "scripts.kpa":"scrape_kpa",
    "scripts.nbim":"scrape_nbim",
    "scripts.pka":"scrape_pka",
    "scripts.pensiondanmark":"scrape_pension_danmark",
    "scripts.pme":"scrape_pme",
    "scripts.pmt":"scrape_pmt",
    "scripts.vervoer":"scrape_vervoer"
    }





#/----------------Run Scripts-----------------/#

logging.info(f"Begin scraping")

#For entry in import list
for x in import_list:
    #Import the module
    module_x=__import__(x, fromlist=[None])
    #Get function object
    function_x=getattr(module_x, import_list[x])

    try:
        #Run function
        function_x()

        message = f"Succesfully ran {import_list[x]}."
    except Exception as e:
        #Error message.
        message = f"Issue running {import_list[x]}. {e}"
        pass

    #log message
    logging.info(message)


logging.info(f"Finished running")