"""AMF Pension Scraper.

Scrapes the most recent annual holdings PDF from AMF Pension (Sweden).
The PDF is laid out in three columns per page; security types appear as
all-caps headers and apply to subsequent entries until the next header.

Note: this PDF has many edge cases. Expect to revisit this scraper if
AMF changes the report's layout, fonts, or URL scheme.
"""

import datetime
import re

import pandas as pd
import pdfplumber
import requests

from pipeline import utils
from pipeline.registry import register

# The name of the pension fund.
_PENSION_NAME = "AMF Pension"

# AMF publishes one report per year at a predictable URL. If the current
# year's report isn't up yet we walk backwards a few years; bounded so a
# permanent URL change can't loop forever.
_URL_TEMPLATE = (
    "https://www.amf.se/globalassets/pdf/rapporter/innehav_{year}.pdf"
)
_MAX_YEARS_BACK = 5

# The PDF lays out holdings in three columns. These x-coordinates split a
# page into left/middle/right crops; re-measure if AMF changes the layout.
_COLUMN_SPLITS = (215, 385)

# Holdings rows are rendered in T-Star-Medium at 7pt. pdfplumber prefixes
# subsetted fonts with a random tag (e.g. "TNBONQ+"), so match the suffix.
_ENTRY_FONT_SUFFIX = "T-Star-Medium"
_ENTRY_FONT_SIZE = 7

# Section headers in the PDF, mapped to IDI security types.
# TODO(student): verify these mappings against the IDI schema.
#   - "FONDER" (funds) is currently mapped to "Swedish Stock" — funds usually
#     aren't equity. Should this be its own type?
#   - "FÖRETAGSOBLIGATION" (corporate bond) is mapped to "Foreign Stock" —
#     this looks wrong; it should probably be a bond type.
_SECTION_HEADERS = {
    "NOTERADE BOLAG": "Swedish Stock",  # listed companies
    "ONOTERADE BOLAG": "Private Swedish Stock",  # unlisted companies
    "FONDER": "Swedish Stock",  # funds — see TODO above
    "LAND OCH BOLAG": "Foreign Stock",  # country and company
    "FÖRETAGSOBLIGATION": "Foreign Stock",  # corporate bond — see TODO above
    "STATSOBLIGATION": "Government Bond",  # government bond
}

# Page-level noise to skip even when a row matches the entry font: page
# numbers ("sid 12") and the AMF watermark.
_NOISE_PATTERN = re.compile(r"sid|AMF")

_KEYWORD_PATTERN = re.compile(
    "Svenska aktier|Utländska aktier|Räntebärande tillgångar"
)


def _is_entry_line(line: dict) -> bool:
    """Return True if a pdfplumber line is rendered in the entry font/size.

    Args:
        line: A pdfplumber line dict, as returned by pdfplumber's `extract_text()`.

    Returns:
        `True` if the line is rendered in the entry font/size.
    """
    first_char = line["chars"][0]
    height = first_char["bottom"] - first_char["top"]
    return (
        first_char["fontname"].endswith(_ENTRY_FONT_SUFFIX)
        and height == _ENTRY_FONT_SIZE
    )


def _fetch_latest_report() -> requests.Response:
    """Return a successful response for the most recent AMF holdings PDF.

    Raises:
        RuntimeError: If no report can be found within ``_MAX_YEARS_BACK`` years.
    """
    year = datetime.date.today().year
    for _ in range(_MAX_YEARS_BACK):
        response = requests.get(_URL_TEMPLATE.format(year=year), stream=True)
        if response.ok:
            return response
        year -= 1
    raise RuntimeError(
        f"No AMF report found in the last {_MAX_YEARS_BACK} years — "
        "the URL scheme may have changed."
    )


@register("amf")
def scrape_amf() -> None:
    """Scrape AMF Pension (Sweden) and write a TSV under ``data/disclosures/amf/<YYYY-MM-DD>/``."""
    response = _fetch_latest_report()
    today = datetime.date.today()
    pdf_path = utils.download_file(response, "amf", today, "pdf")

    entries: list[list[str]] = []

    with pdfplumber.open(pdf_path) as pdf:
        report_date = utils.get_pdf_date(pdf)

        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if not _KEYWORD_PATTERN.search(page_text):
                continue

            left, mid = _COLUMN_SPLITS
            sections = [
                page.crop((0, 0, left, page.height)),
                page.crop((left, 0, mid, page.height)),
                page.crop((mid, 0, page.width, page.height)),
            ]

            for section in sections:
                # Security type carries downward within a column until the
                # next header; reset when crossing a column boundary.
                sec_type = ""
                for line in section.extract_text_lines(return_chars=True):
                    text = line["text"]

                    if text in _SECTION_HEADERS:
                        sec_type = _SECTION_HEADERS[text]
                        continue

                    if not sec_type or not _is_entry_line(line):
                        continue
                    if _NOISE_PATTERN.search(text):
                        continue

                    entries.append(
                        [
                            _PENSION_NAME,
                            text,
                            report_date,
                            sec_type,
                            response.url,
                        ]
                    )

    df = pd.DataFrame(
        entries,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - Report Date",
            "Security - Type",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "amf", today)


if __name__ == "__main__":
    scrape_amf()
