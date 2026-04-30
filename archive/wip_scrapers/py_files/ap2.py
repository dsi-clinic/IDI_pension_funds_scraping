"""library imports"""

import re

import pandas as pd
import pdfplumber

"""
import the pdfs: this will change, and become a requests function
both pdfs will be combined into one table
"""
pdf = pdfplumber.open("ap2swedish2025.pdf")
pdf2 = pdfplumber.open("ap2foreign2025.pdf")

"""scrapes first pdf, swedish equities"""
tabs = []
for page in pdf.pages:
    table_bbox = (0, 100, page.width, page.height)
    page = page.within_bbox(table_bbox)
    tabs.append(page.extract_text())
tabs2 = " ".join(tabs)

"""using regular expression to find rows of text that match the pattern of the table, adding that to list matches"""
pattern = re.compile(
    r"^(?P<Name>.+?)\s+(?P<ISIN>SE\d{10})\s+(?P<Country>[A-Z]{2})\s+(?P<Number>[\d\s]+)\s+(?P<MarketValue>[\d\s]+)\s+(?P<ShareCapital>[\d,.]+%)\s+(?P<VotingCapital>[\d,.]+%)$",
    re.MULTILINE,
)

matches = pattern.finditer(tabs2)

data = []
for match in matches:
    data.append(match.groupdict())

"""creating a dataframe from the extracted data"""
df = pd.DataFrame(data)


"""steps are repeated for second pdf, foreign equities"""
tabs3 = []
for page in pdf2.pages:
    tabs3.append(page.extract_text())

tabs4 = " ".join(tabs3)

pattern2 = re.compile(
    r"^(?P<Name>.+?)\s+(?P<ISIN>[A-Z]{2}[A-Z0-9]{9,})\s+(?P<Country>[A-Z]{2})\s+(?P<Number>[\d\s]+)\s+(?P<MarketValue>[\d\s]+)$",
    re.MULTILINE,
)
matches = pattern2.finditer(tabs4)

data2 = []
for match in matches:
    data2.append(match.groupdict())

df2 = pd.DataFrame(data2)

"""concatenating the dataframes that correspond to each pdf file"""
table = pd.concat([df, df2])

table.to_csv("ap2_2025.csv")
"""returns a csv file, combining both swedish and foreign equities to be one data set"""
