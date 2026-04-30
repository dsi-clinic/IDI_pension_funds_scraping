# Unautomated version of KPA
"""Imports"""

import functions
import pandas as pd
import pdfplumber

filename = "KPA"
path = functions.create_path(filename)


pdf = pdfplumber.open(path / "kpa.pdf")


"""Extract Entries Based on Font Size"""
entries = []
for page in pdf.pages:
    # Extract every line in a page, formatted in a list of dictionaries
    text = page.extract_text_lines()

    # For each line, calculate and check height
    for t in text:
        height = t["bottom"] - t["top"]
        # If height falls within range, it is a match
        if height > 9 and height < 10:
            entries.append(t["text"])


"""Formatting Data"""

shareholder_name = [filename]
report_date = functions.get_pdf_date(pdf)
report_date = [report_date]
url = [
    "https://www.kpa.se/om-kpa-pension/vart-hallbarhetsarbete/ansvarsfulla-investeringar/innehav-och-uteslutna-bolag/"
]

number_of_entries = len(entries)

shareholder_name = shareholder_name * number_of_entries
report_date = report_date * number_of_entries
url = url * number_of_entries

df = {
    "Shareholder - Name": shareholder_name,
    "Issuer - Name": entries,
    "Security - Report Date": report_date,
    "Data Source URL": url,
}


final_df = pd.DataFrame(df)
functions.export_df(final_df, filename, path)
