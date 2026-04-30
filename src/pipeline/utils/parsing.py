"""Parsing helpers for dates and PDF metadata."""

import pdfplumber


def get_pdf_date(pdf: pdfplumber.PDF) -> str:
    """Extract a ``YYYY-MM-DD`` report date from a pdfplumber PDF's metadata.

    Args:
        pdf: Open ``pdfplumber.PDF`` whose ``metadata["CreationDate"]`` is set.

    Returns:
        Report date formatted as ``YYYY-MM-DD``.

    Raises:
        KeyError: If the PDF has no ``CreationDate`` metadata entry.
    """
    report_date = pdf.metadata["CreationDate"]
    report_date = (
        report_date[2:6] + "-" + report_date[6:8] + "-" + report_date[8:10]
    )
    return report_date


def convert_month(month: str, offset: int | str | None = None) -> str:
    """Convert an English month name to a zero-padded month number.

    Args:
        month: Month name (case-insensitive, e.g. ``"January"``).
        offset: Optional integer (or numeric string) added to the month
            number before zero-padding.

    Returns:
        Zero-padded two-digit month (``"01"``..``"12"``), or an empty
        zero-padded value (``"00"``) when ``month`` is unrecognized and no
        offset is supplied.
    """
    month = month.lower()

    match month:
        case "january":
            month = "1"
        case "february":
            month = "2"
        case "march":
            month = "3"
        case "april":
            month = "4"
        case "may":
            month = "5"
        case "june":
            month = "6"
        case "july":
            month = "7"
        case "august":
            month = "8"
        case "september":
            month = "9"
        case "october":
            month = "10"
        case "november":
            month = "11"
        case "december":
            month = "12"
        case _:
            month = ""

    if offset:
        month = int(month) + int(offset)
        month = str(month)

    if len(month) < 2:
        month = "0" + month

    return month
