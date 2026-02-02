'''Imports'''
#Python modules
import datetime
import re
from pathlib import Path
import io
import warnings

#External Modules
from playwright.sync_api import sync_playwright
import pdfplumber
import pandas as pd
import requests

#Custom Module
import scripts.functions as functions


'''Setup'''

#Supress warnings. (For debug purposes, comment this line.)
warnings.filterwarnings('ignore')


#Dictionary of module:function (Note that the scripts.___ is important for python to find directory)
import_list = {
    "scripts.vervoer":"scrape_vervoer"
    }


#Input used for download
download = bool(input("Download pdf? (True or False)"))
#message used to track progress/debug (Ideally will be replaced with log system in future)
message = ""

'''Run Scripts'''

if download == True:
    for x in import_list:

        #Import the module
        module_x=__import__(x, fromlist=[None])
        
        #Get function object
        function_x=getattr(module_x, import_list[x])

        try:
            #Run function
            function_x(download)

            message = "Succesfully ran " + import_list[x] + " with download."
        except:
            try:
                #Run function without download
                download = False
                function_x(download)

                #Reset download to True
                download = True
                
                message = "Ran " + import_list[x] + " without download."
            except:
                #Error message.
                message = "Unable to run " + import_list[x]
                pass

        #print message
        print(message)

elif download == False:
    for x in import_list:

        #Import the module
        module_x=__import__(x, fromlist=[None])
        
        #Get function object
        function_x=getattr(module_x, import_list[x])

        try:
            #Run function
            function_x(download)
            message = "Succesfully ran " + import_list[x] + " without download."
        except:
            #Error message.
            message = "Unable to run " + import_list[x]
            pass

        #print message
        print(message)