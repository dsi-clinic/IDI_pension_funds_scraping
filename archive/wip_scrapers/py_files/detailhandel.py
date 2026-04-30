"""library imports"""

import re

import pandas as pd
import pdfplumber

"""importing pdf, subject to change"""
pdf = pdfplumber.open("detailhandel.pdf")

"""extracting text from pdf"""
tabs = []
for page in pdf.pages:
    tabs.append(page.extract_text())
tabs2 = " ".join(tabs)

"""using regular expression to find rows that correspond to the known table rows"""
pattern = re.compile(r"(.+?)\s+([\d,]+)\s+(.+?)\s+([\d,]+)")
matches = pattern.findall(tabs2)

"""removing the page headers from the data"""
for item in matches:
    if item == (
        "Beleggingen Pensioenfonds Detailhandel per",
        "31",
        "december",
        "2024",
    ):
        matches.remove(item)


"""stripping extra space and removing commas"""
data = []
for name1, value1, name2, value2 in matches:
    data.append(
        {"Name": name1.strip(), "MarketValue(EUR)": value1.replace(",", "")}
    )
    data.append(
        {"Name": name2.strip(), "MarketValue(EUR)": value2.replace(",", "")}
    )


"""creating a data frame from list"""
df = pd.DataFrame(data[:-2])
print(df.head())

"""exporting csv file from dataframe"""
df.to_csv("detailhandel.csv")
