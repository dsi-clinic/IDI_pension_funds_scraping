"""Setup"""

# Import modules
import re

import pandas as pd
import pdfplumber

# Opens file as instance of .PDF class
pdf = pdfplumber.open("vervoer.pdf")


"""Create list of entries"""
# Fills list with strings containing the text of each page
tabs = []
for page in pdf.pages:
    text = page.extract_text(
        layout=True, x_density=4
    )  # Collate all text on a page into one string, with parameters telling python where to find text
    tabs.append(
        text.strip() + "\n"
    )  # Strips text of new lines, and appends each page to empty list tabs

# Combines list entries into one string
tabs2 = " ".join(tabs)

# Splits breaks in string into a list of strings
tabs3 = tabs2.splitlines()


"""Create and apply Regex"""
# Create regex, looking for 2 sets of groups of words followed by numbers, or a dash
pattern = re.compile(
    r"(?P<l_key>[A-Za-z\s,]+?)\s+(?P<l_value>([\d\.]+( [\d\.]+)*,?)|(-))\s+(?P<r_key>[A-Za-z\s,]+?)\s+(?P<r_value>([\d\.]+( [\d\.]+)*,?)|(-))"
)


# Create two empty lists so that the order may be maintained
tabs4 = []
tabs5 = []
for tab in tabs3:
    # Apply matches to list
    var = pattern.search(tab.strip())
    try:
        tabs4.append(var.group(1, 2))
        tabs5.append(var.group(6, 7))
    except:
        pass
# Combine list
tabs6 = tabs4 + tabs5

tabs7 = []
# Filter out entries with comma or - (not filtered out before so that lines with only one valid entry would not be discredited)
for tab in tabs6:
    var = re.search(r"(?!\d+?[,-])\d+?", tab[1])
    if var:
        print(var)
        tabs7.append(tab)


"""Create Dataframe and export"""
# Create two column dataframe
df = pd.DataFrame(tabs7, columns=["Company", "Market Value (x €1.000)"])

# export
df.to_csv("vervoer.csv")
