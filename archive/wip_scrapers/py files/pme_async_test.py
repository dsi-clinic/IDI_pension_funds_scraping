# Async misses pages a lot (though it is def faster). Remove grequests and abandon async approach
# takes ~3 mins to run


# Python modules
import logging
import re
from pathlib import Path

# import requests
#
import grequests
import pandas as pd

# External modules
from playwright.sync_api import sync_playwright

# If run from main, imports from scripts folder. Else, imports locally.
if __name__ != "__main__":
    import scripts.functions as functions

    log_mode = "a"  # Add to log
else:
    import functions

    log_mode = "w"  # Create log


# Create and save directory
path = functions.create_path("pme")
# URL to Dynamic HTML
data_url = "https://www.pmepensioen.nl/en/investments/we-do-invest-in"

# Find directory of repository
parent_dir = Path(__file__).parent.parent
# Setup logging
logging.basicConfig(
    filename=parent_dir / "log.log",
    level=logging.INFO,
    filemode=log_mode,
    format="%(asctime)s - %(message)s",
)


# ----------------/Get page text/-----------------#

# Playwright Start
playwright = sync_playwright().start()

# Establish page and browser
browser = playwright.chromium.launch(
    headless=True, slow_mo=1, channel="chromium"
)
page = browser.new_page()

# Go to page that leads to PDF
page.goto(data_url)

# Cookies (sometimes doesn't appear in headless mode)
try:
    page.get_by_role("button", name="Decline").click()
except:
    pass

# Get all text on page
text = page.inner_text("div")


browser.close()
playwright.stop()


# Find the number of pages as an integer (Currently equities are listed before bonds, so we can use re.search)
max_pages = re.search(r"Page 1 of (?P<max>\d+)", text)
max_pages = int(max_pages.group(1))


# Report date not listed, however the next report date is, as well as the fact that they report every 3 months. Using this, we can approximate report dates.
# Find date of next report using regex
next_report_date = re.search(
    r"(?P<month>[A-Z][a-z]+) (?P<day>\d+)[a-z]+, (?P<year>\d{4})", text
)
if next_report_date:
    # Split groups into vars
    month, day, year = next_report_date.groups()

    # Converted to function, but untested in this script. Delete commented code above if works.
    month = functions.convert_month(month, -3)

    # Convert day to double digit if necessary
    if len(day) < 2:
        day = "0" + day

    # Piece together report date in desired format
    report_date = year + "-" + month + "-" + day
else:
    # If not found, report date is NA
    report_date = ""

print(report_date)


# GET URLS
urls = []
for i in range(1, max_pages + 1):  # stop is exclusive
    urls.append(
        f"https://www.pmepensioen.nl/en/views/ajax?_wrapper_format=drupal_ajax&view_name=investments&view_display_id=shares&view_args=&view_path=%2Fnode%2F594&view_base_path=rest%2Fcontinent-investments&view_dom_id=5b380d85bc3f41b8e33a70d57fd946ac143fd842a84caebc7550ae957c7dd32e&pager_element=0&viewsreference%5Bcompressed%5D=eJxdkNsKgzAMQP8lzz5sDCfzZ0pGYw20VWp0iPjvS-lU2EMJyTm50A0sCkK7ASY3B4oCbZy9r6AntJSOzHNgRQAVDF030amN6C5LWDxBe9sroIhvT9aoKhzdlFccQ0o89V_c87CkFxh9LKuRdcxUi-gSjj38C2wV319Nc5GOyVsTMeTOkixMn0tItPDEQyzNzaOunwqLyULBWPL5Q277FzAaYdg&viewsreference%5Bcompressed%5D=eJxdkNsKgzAMQP8lzz5sDCfzZ0pGYw20VWp0iPjvS-lU2EMJyTm50A0sCkK7ASY3B4oCbZy9r6AntJSOzHNgRQAVDF030amN6C5LWDxBe9sroIhvT9aoKhzdlFccQ0o89V_c87CkFxh9LKuRdcxUi-gSjj38C2wV319Nc5GOyVsTMeTOkixMn0tItPDEQyzNzaOunwqLyULBWPL5Q277FzAaYdg&page={i}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=whitelabel&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=eJxVjVsOwyAMBC9E8ZGipVjBBVILQ9Pcvmr6SPvjHc2u5CDzpKJM4Q1O0TA3aDKKbSiKP4wfi45QxBJHZ5t1rhRg7HrW6ZzQ6QMeZtxtLyI6CjZux1ZllUxf2p1pGUumV5zaqO4mvBrt1-OC-5-o1zgKuzVJ54LAhRCrLL_i-e4B4bdX4w"
    )
print("urls done")

# GET DATA
reqs = [grequests.get(link) for link in urls]
resp = grequests.map(reqs)

print("past resp")


p_cut_data = re.compile("<tbody>.+</tbody>")
p_match_entry = re.compile(
    r"!!!(?P<issuer>[A-Za-z\d /&\-,]+)!!!(?P<value>[\d\.]+)!!!(?P<country>[A-Za-z]+(?: [A-Za-z ]+)?)(?:, Republic of)?!!!(?P<sector>[A-Za-z ,\-]+)!!!(?P<type>[A-Za-z ]+)"
)

# report date
shareholder = "PME pensioenfonds"
currency = "EUR"
multiplier = "x1"
# url


# PARSE DATA
entries = []
with open(path / "raw_data_pme.txt", "w") as file:  # path
    for i, r in enumerate(resp):  # can remove enum when done debugging
        if r and r.status_code == 200:
            print(i)

            json_data = str(r.json())

            # remove literal
            tbody_data = json_data.replace("\\n", "")
            # remove non-literal
            tbody_data = re.sub("\n", "", tbody_data)

            tbody_cut = re.search(p_cut_data, tbody_data)
            if tbody_cut:
                tbody_data = tbody_cut.group()

            file.write(tbody_data + "\n")

            # Remove HTML code
            text = re.sub("<[^<>]+>", "", tbody_data)
            text = re.sub(" {2,}", "!!!", text)

            matches = re.findall(p_match_entry, text)
            for match in matches:
                issuer, value, country, sector, sectype = match
                entry = [
                    shareholder,
                    issuer.strip(),
                    country.strip(),
                    sector.strip(),
                    sectype.strip(),
                    report_date,
                    value.strip(),
                    multiplier,
                    currency,
                    data_url,
                ]

                entries.append(entry)

# Create DF according to IDI schema
df = pd.DataFrame(
    entries,
    columns=[
        "Shareholder - Name",
        "Issuer - Name",
        "Issuer - Country Name",
        "Issuer - Sector",
        "Security - Type",
        "Security - Report Date",
        "Security - Market Value - Amount",
        "Security - Market Value - Multiplier",
        "Security - Market Value - Currency Code",
        "Data Source URL",
    ],
)
# Export as tsv
functions.export_df(df, "pme", path)
