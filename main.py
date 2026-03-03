''''''
#Python modules
import warnings
import logging



#/-----Setup-----/#

#Supress warnings. (For debug purposes, comment this line.)
warnings.filterwarnings('ignore')

#Configure logging settings
logging.basicConfig(filename='log.log', level=logging.INFO, filemode="w", format="%(asctime)s - %(message)s")
#message used to track progress/debug
message = ""

#Dictionary of module:function (Note that the scripts.___ is important for python to find directory). Alphabetical Order.
import_list = {
    "scripts.ap2":"scrape_ap2",
    "scripts.ap3":"scrape_ap3",
    "scripts.ap4":"scrape_ap4",
    "scripts.bpfbouw":"scrape_bpfbouw",
    "scripts.kpa":"scrape_kpa",
    "scripts.pka":"scrape_pka",
    "scripts.pmt":"scrape_pmt",
    "scripts.vervoer":"scrape_vervoer"
    }





#/-----Run Scripts-----/#

#Log
logging.info(f"Begin scraping")

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