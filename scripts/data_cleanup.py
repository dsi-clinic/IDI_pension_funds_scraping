'''Script to clean up data output folder. Sorts through directories to find data output folders specifically, then loops through each folder to group dates into quarters. Then, picks the earliest month from each quarter to keep, with the latest day used as a tiebreaker. Lastly, all other files are deleted. Currently not meant to be called as a function.'''
#Imports
import shutil
import os
import datetime
from pathlib import Path

'''Setup'''
#Set current directory to repository
os.chdir(Path(__file__).parent.parent)
#Get year
year = datetime.datetime.now().year
#Setup variables
data_folder = "./data/"
list = []


#Create list of named directories in data
for folder in os.listdir(data_folder):
    directory = data_folder + folder
    #If directory is a folder, append to list
    if os.path.isdir(directory):
        list.append(directory)

sub_list = []
for directory in list:

    sub_list = os.listdir(directory) #A single named directory
    group_1 = [] #Jan-March
    group_2 = [] #April-June
    group_3 = [] #July-September
    group_4 = [] #October-December

    '''Group dates in one directory based on quarter'''
    for sub_dir in sub_list:
        
        sd_month = int(sub_dir[5:7]) #The month as a number
        sd_day = int(sub_dir[8:]) #The day as a number

        #If not current year, don't touch the data
        if int(sub_dir[0:4]) != year:
            pass
        #Else, group based on quarter
        elif sd_month < 4:
            group_1.append([sub_dir, sd_month, sd_day])
        elif sd_month < 7:
            group_2.append([sub_dir, sd_month, sd_day])
        elif sd_month < 10:
            group_3.append([sub_dir, sd_month, sd_day])
        else:
            group_4.append([sub_dir, sd_month, sd_day])


    '''Determine files to remove'''
    #Group groups for for loop
    group = [group_1, group_2, group_3, group_4]
    for g in group:
        #Number of date folders in a named folder
        x = len(g)

        #If there is more than one folder in named directory, continue
        if x > 1:

            #Append all months to a list
            n = 0
            temp_list = []
            while n < x:
                temp_list.append(g[n][1])
                n += 1
            
            #Find minimum month, and count number of minimums
            minimum = min(temp_list)
            number_of_minimums = 0
            list_of_index = []
            for value in temp_list:
                if value == minimum:
                    temp_list.index(value) #Index/Indicies of potential values to exclude
                    number_of_minimums += 1

            #Tie break using day
            if number_of_minimums > 1:
                n = 0
                temp_list = []
                while n < x:
                    temp_list.append(g[n][2])
                    n += 1

                #Exclude latest day in earliest month from folders to remove
                g.remove(g[temp_list.index(max(temp_list))])

            #If no tie, exclude earliest month from folders to remove
            else:
                g.remove(g[temp_list.index(min(temp_list))])


            '''Remove files'''
            #For remaining entry in a given group, piece together directory and try to delete
            for entry in g:
                remove_directory = os.path.join(directory + "/" + entry[0])
                try:
                    shutil.rmtree(remove_directory, ignore_errors=True)
                except Exception as e:
                    print(e)

        #If there is only 1 or 0 date folders, do nothing
        else:
            pass
