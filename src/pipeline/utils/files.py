"""File and path helpers shared across scrapers."""

import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
from playwright.sync_api import Browser, Locator, Page

_REPO_ROOT = Path(__file__).resolve().parents[3]

FileType = Literal["pdf", "html", "json", "csv", "txt"]


def build_dir(
    scraper: str,
    date: datetime.date,
    kind: Literal["raw", "clean"],
) -> Path:
    """Create and return ``data/disclosures/{scraper}/{date}/{kind}/``.

    Most scrapers should not call this directly — use ``download_file`` or
    ``export_data`` instead. Reach for this only when writing a raw artifact
    that doesn't come from a single ``requests`` response (e.g. a multi-page
    scrape snapshot).

    Args:
        scraper: Scraper name (matches the registry key).
        date: Report date (e.g. ``datetime.date(2022, 1, 1)``).
        kind: ``"raw"`` or ``"clean"``.

    Returns:
        A ``Path`` to ``data/disclosures/{scraper}/{date}/{kind}/``.
    """
    path = (
        _REPO_ROOT / "data" / "disclosures" / scraper / date.isoformat() / kind
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _filename(scraper: str, ext: str, subname: str | None) -> str:
    """Build a filename from ``scraper`` and ``ext`` (e.g. ``"amf.pdf"``).

    Args:
        scraper: Scraper name (matches the registry key).
        ext: Source file extension (``"pdf"``, ``"html"``, ``"json"``, ...).
        subname: Optional discriminator when a scraper produces multiple raw
            files in a single run.

    Returns:
        A filename (e.g. ``"amf.pdf"``).
    """
    return f"{scraper}_{subname}.{ext}" if subname else f"{scraper}.{ext}"


def download_file(
    response: requests.Response,
    scraper: str,
    date: datetime.date,
    file_type: FileType,
    *,
    subname: str | None = None,
    chunk_size: int = 8192,
) -> Path:
    """Stream ``response`` to ``data/disclosures/{scraper}/{date}/raw/{scraper}.{file_type}``.

    With ``subname`` the filename becomes ``{scraper}_{subname}.{file_type}`` —
    used by scrapers that download multiple sources per run (e.g. ap3).

    Args:
        response: Streaming ``requests`` response to drain.
        scraper: Scraper name (matches the registry key, e.g. ``"amf"``).
        date: Date stamp for the run directory.
        file_type: Source file extension (``"pdf"``, ``"html"``, ``"json"``, ...).
        subname: Optional discriminator when a scraper produces multiple raw
            files in a single run.
        chunk_size: Bytes per chunk read from the response.

    Returns:
        Absolute path to the written file.
    """
    raw_dir = build_dir(scraper, date, "raw")
    file_path = raw_dir / _filename(scraper, file_type, subname)

    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)

    return file_path


def export_data(
    df: pd.DataFrame,
    scraper: str,
    date: datetime.date,
    *,
    subname: str | None = None,
) -> Path:
    """Write ``df`` to ``data/disclosures/{scraper}/{date}/clean/{scraper}.tsv``.

    With ``subname`` the filename becomes ``{scraper}_{subname}.tsv`` — used by
    scrapers that emit multiple TSVs per run (e.g. ap3).

    Args:
        df: DataFrame to write.
        scraper: Scraper name (matches the registry key).
        date: Date stamp for the run directory.
        subname: Optional discriminator when a scraper emits multiple TSVs.

    Returns:
        Absolute path to the written TSV.
    """
    clean_dir = build_dir(scraper, date, "clean")
    file_path = clean_dir / _filename(scraper, "tsv", subname)
    df.to_csv(file_path, sep="\t", index=False)
    return file_path


def get_pdf(
    scraper: str,
    date: datetime.date,
    page: Page,
    link_button: Locator,
    browser: Browser,
) -> Path:
    """Download a PDF opened by clicking ``link_button`` (Google PDF popup).

    Writes to ``data/disclosures/{scraper}/{date}/raw/{scraper}.pdf`` and
    closes ``browser`` before returning.

    Args:
        scraper: Scraper name (matches the registry key).
        date: Date stamp for the run directory.
        page: Playwright page where ``link_button`` lives.
        link_button: Locator that, when clicked, opens the PDF in a popup tab.
        browser: Playwright browser instance, closed before returning.

    Returns:
        Absolute path to the saved PDF.
    """
    with page.expect_popup() as popup_info:
        link_button.click()

    response = requests.get(popup_info.value.url, stream=True)
    try:
        return download_file(response, scraper, date, "pdf")
    finally:
        browser.close()
