'''Note: Will be rewritten to use regular expressions, and automatically fetch pdf.
Does not work with current PDF.'''

'''Setup'''
#Import modules
import pdfplumber
import re
import pandas as pd

#Opens file as instance of .PDF class
pdf = pdfplumber.open("vervoer.pdf")

'''Create list of entries'''
#Fills list with strings containing the text of each page
tabs = []
for page in pdf.pages:
    text = page.extract_text(layout = True, x_density=4) #Collate all text on a page into one string, with parameters telling python where to find text
    tabs.append(text.strip()+"\n") #Strips text of new lines, and appends each page to empty list tabs

#Combines list entries into one string
tabs2 = " ".join(tabs)

#Splits breaks in string into a list of strings
tabs3 = tabs2.splitlines()

#Further splits and strips entries, creating a list of lists.
tabs4 = [] 
for tab in tabs3:
    tab = tab.strip() #(tab is one string)
    tabs4.append(tab.split('  ')) #Appends strings split by any amount of space (tab is a list of every word that was in the string)

#Entries in each individual list turned into one big list
tabs4=sum(tabs4, [])

'''Formatting'''
#Remove empty strings in tabs 4
for tab in tabs4:
    if tab == '' or tab == ' ' or tab == '  ' or tab == '   ':
        tabs4.remove(tab)

#Remove heading (Investment overview as of 12/31/2024)
for tab in tabs4:
    if tab == 'Overzicht beleggingen per 31-12-2024':
        tabs4.remove(tab)

#Remove heading (Page)
for tab in tabs4:
    if 'Pagina' in tab:
        tabs4.remove(tab)

#Remove heading (Market Value)
for tab in tabs4:
    if 'Marktwaarde' in tab:
        tabs4.remove(tab)

#Removes first 13 entries (indicies 0-12)
tabs4 = tabs4[13:]

#If string is not empty, append to new list
tabs5 = []
for tab in tabs4:
    if tab != '':
        tabs5.append(tab)

#Delete several explanatory entries
del tabs5[19:23]

'''Creating and exporting dataframes'''
#Creates list of lists containing every fourth entry in tabs5 (dataframe)
chunks = [tabs5[i:i+4] for i in range(0, len(tabs5), 4)]
chunks = chunks[:1494]

#Organizes dataframe into columns
table = pd.DataFrame(chunks, columns=['1', '2', '3', '4'])

#Format columns 1 and 2 into table
shares = table[['1', '2']].copy()
shares.columns = ['issuer name', 'marketvalue x €1000']

#Format columns 3 and 4 into table
fixed_income_securities = table[['3', '4']].copy()
fixed_income_securities.columns = ['issuer name', 'marketvalue x €1000']

#Save tables as csv files
shares.to_csv('vervoershares.csv')
fixed_income_securities.to_csv('vervoerfixedincome.csv')