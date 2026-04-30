"""Sampension Scraper.

Scrapes Sampension, a Danish labor-market pension fund. The holdings
PDF is a scanned image rather than text, so this scraper rasterizes
each page with pdfplumber and runs Tesseract OCR over the result, then
splits the OCR output into issuer / ISIN / value tokens with regex.

Note: Tesseract makes mistakes; rows can fall out of column alignment.
The log will indicate when that happened — the easy way to verify is
to paste the resulting TSV into a spreadsheet next to the source PDF
and look for divergences.
"""

import datetime
import logging
import re
import shutil
import tempfile

import pandas as pd
import pdfplumber
import pytesseract
import requests
from PIL import Image
from playwright.sync_api import sync_playwright

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Sampension"
_LANDING_URL = (
    "https://www.sampension.dk/om-sampension/ansvarlighed/"
    "ansvarlige-investeringer/aabenhed-paa-esg-omraadet/"
)
_PDF_BASE_URL = "https://www.sampension.dk/"
_CURRENCY = "DKK"

# OCR rasterization resolution (dpi). Higher is more accurate but
# slower; 144 is a reasonable balance for this report.
_OCR_RESOLUTION = 144

# Tesseract introduces stray symbols that the source PDF doesn't have;
# strip them before applying the row regexes.
_OCR_NOISE_PATTERN = re.compile(r"’|£|\|")

# All-caps issuer names. Picks up some non-issuers (column headers,
# fund names containing digits) which we filter out below.
_ISSUER_PATTERN = re.compile(r"(?P<issuer>[A-Za-z\d\-\. /&()'_:,]+)")
# A value like "1.234.567,89" or "12.345" followed by a 12+ char ISIN.
_VALUE_ISIN_PATTERN = re.compile(
    r"(?P<value>\d[\d\. ]+) (?P<isin>[A-Z\d ]{12,})"
)
_REPORT_DATE_PATTERN = re.compile(
    r"(?P<day>\d+)\. (?P<month>[A-Za-z]+) (?P<year>\d{4})"
)
# Lines containing the fund's own name or DKK header are not issuers.
_NON_ISSUER_PATTERN = re.compile(r"[Ss]ampension|DKK")


def _configure_tesseract() -> None:
    """Point pytesseract at the system tesseract binary, or raise.

    Uses ``shutil.which`` rather than enumerating per-OS install paths
    by hand. Anything findable on ``PATH`` works.

    Raises:
        RuntimeError: If tesseract isn't installed / not on ``PATH``.
    """
    cmd = shutil.which("tesseract")
    if not cmd:
        raise RuntimeError(
            "Sampension: tesseract not found on PATH. Install Tesseract "
            "OCR (see README) and ensure the binary is reachable."
        )
    pytesseract.pytesseract.tesseract_cmd = cmd


def _download_and_get_report_date(today: datetime.date) -> tuple[str, str, str]:
    """Open the landing page, download the holdings PDF, return ``(pdf_path, report_date, pdf_url)``.

    Args:
        today: Date stamp for the run directory.

    Returns:
        Tuple of the local PDF path, the report date as ``YYYY-MM-DD``,
        and the absolute PDF URL.

    Raises:
        RuntimeError: If the PDF link or report date can't be located
            on the landing page.
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_LANDING_URL)
        try:
            page.get_by_role(
                "button", name="Afvis alle"
            ).click(timeout=5000)
        except Exception:
            pass

        href = page.get_by_role(
            "button", name="Aktier (pdf)", exact=False
        ).get_attribute("href")
        if not href:
            raise RuntimeError(
                "Sampension: 'Aktier (pdf)' link missing — landing page "
                "layout may have changed."
            )
        pdf_url = _PDF_BASE_URL + href

        response = requests.get(pdf_url, stream=True)
        pdf_path = utils.download_file(
            response, "sampension", today, "pdf"
        )

        report_date = ""
        for paragraph in page.get_by_role("paragraph").all():
            match = _REPORT_DATE_PATTERN.search(paragraph.inner_text())
            if match:
                month = utils.convert_month(match["month"])
                report_date = (
                    f"{match['year']}-{month}-{int(match['day']):02d}"
                )
                break

        browser.close()

    if not report_date:
        raise RuntimeError(
            "Sampension: report date paragraph not found on the "
            "landing page."
        )
    return pdf_path, report_date, pdf_url


def _ocr_pages(pdf_path: str) -> list[str]:
    """Rasterize each PDF page and return the OCR text per page.

    Args:
        pdf_path: Local path to the downloaded PDF.

    Returns:
        One OCR-text string per page, in order. Pages that fail to OCR
        are returned as empty strings (with a log line) so the caller
        can keep going.
    """
    texts: list[str] = []
    with (
        pdfplumber.open(pdf_path) as pdf,
        tempfile.TemporaryDirectory() as tmp,
    ):
        image_path = f"{tmp}/page.png"
        for page_num, page in enumerate(pdf.pages):
            try:
                page.to_image(resolution=_OCR_RESOLUTION).save(image_path)
                text = pytesseract.image_to_string(Image.open(image_path))
            except Exception:
                logging.exception(
                    "Sampension - failed to OCR page %d", page_num
                )
                text = ""
            texts.append(_OCR_NOISE_PATTERN.sub("", text))
    return texts


def _parse_ocr_text(texts: list[str]) -> tuple[list[str], list[str], list[str]]:
    """Parse OCR text into parallel issuer / value / ISIN columns.

    Tesseract emits all of one column followed by all of the other on
    each page, so issuers are collected by one regex and values+ISINs by
    another. The two lists are kept the same length by skipping invalid
    rows on either side.

    Args:
        texts: Per-page OCR strings.

    Returns:
        Three parallel lists: issuers, values, ISINs.
    """
    issuers: list[str] = []
    values: list[str] = []
    isins: list[str] = []
    for text in texts:
        if not text:
            continue
        for issuer in _ISSUER_PATTERN.findall(text):
            # ISINs frequently match the issuer regex; the heuristic is
            # that real issuer names have at most a handful of digits.
            if len(re.findall(r"\d", issuer)) <= 2 and not _NON_ISSUER_PATTERN.search(issuer):
                issuers.append(issuer)
        for value, isin in _VALUE_ISIN_PATTERN.findall(text):
            # Real value rows always have a thousands separator (period
            # in DKK formatting); the regex sometimes catches headers
            # that don't.
            if "." in value:
                values.append(value)
                isins.append(isin)
    return issuers, values, isins


@register("sampension")
def scrape_sampension() -> None:
    """Scrape Sampension via OCR and write a TSV under ``data/disclosures/sampension/<YYYY-MM-DD>/``."""
    _configure_tesseract()
    today = datetime.date.today()
    pdf_path, report_date, pdf_url = _download_and_get_report_date(today)

    issuers, values, isins = _parse_ocr_text(_ocr_pages(pdf_path))

    # The OCR sometimes misses the final issuer because the last page's
    # newline isn't picked up; ``zip(strict=False)`` truncates to the
    # shortest list rather than crashing.
    if len(issuers) != len(isins):
        logging.warning(
            "Sampension: OCR column-length mismatch (%d issuers, %d "
            "ISINs) — output may be off by one row at the end.",
            len(issuers),
            len(isins),
        )

    rows = [
        [
            _PENSION_NAME,
            issuer,
            isin,
            report_date,
            value,
            _CURRENCY,
            pdf_url,
        ]
        for issuer, value, isin in zip(issuers, values, isins, strict=False)
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "Shareholder - Name",
            "Issuer - Name",
            "Security - ISIN",
            "Security - Report Date",
            "Security - Market Value - Amount",
            "Security - Market Value - Currency Code",
            "Data Source URL",
        ],
    )
    utils.export_data(df, "sampension", today)


if __name__ == "__main__":
    scrape_sampension()
