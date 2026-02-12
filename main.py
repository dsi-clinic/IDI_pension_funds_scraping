'''Imports'''
#python modules
import warnings
import logging



'''Setup'''
#Supress warnings. (For debug purposes, comment this line.)
warnings.filterwarnings('ignore')
#Configure logging settings
logging.basicConfig(filename='log.log', level=logging.INFO, filemode="w", format="%(asctime)s - %(message)s")

#Dictionary of module:function (Note that the scripts.___ is important for python to find directory)
import_list = {
    "scripts.vervoer":"scrape_vervoer"
    }


#message used to track progress/debug
message = ""



'''Run Scripts'''
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
        message = f"Unable to run {import_list[x]}. {e}"
        pass

        #log message
        logging.info(message)


logging.info(f"Finished running")