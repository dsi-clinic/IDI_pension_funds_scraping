"""File and path helpers shared across scrapers."""

import datetime
from pathlib import Path

import pandas as pd
import requests
from playwright.sync_api import Browser, Locator, Page

_REPO_ROOT = Path(__file__).resolve().parents[3]


def create_path(name: str = "no_name") -> Path:
    """Create ``data/disclosures/<name>/<YYYY-MM-DD>/`` under the repo root and return it.

    Args:
        name: Shareholder/folder name to nest the dated directory under.

    Returns:
        Absolute path to the created directory.

    Raises:
        OSError: If the directory cannot be created (e.g. permission denied).
    """
    time = datetime.datetime.now().strftime("%Y-%m-%d")
    path = _REPO_ROOT / "data" / "disclosures" / name / time
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_pdf(
    filename: str,
    page: Page,
    link_button: Locator,
    browser: Browser,
    path: Path | None = None,
) -> Path:
    """Download a PDF opened by clicking ``link_button`` (Google PDF popup).

    Args:
        filename: Org name used as the saved file's stem (e.g. ``"vervoer"``).
        page: Playwright page where ``link_button`` lives.
        link_button: Playwright locator that, when clicked, opens the PDF in a
            new popup tab.
        browser: Playwright browser instance, closed before returning.
        path: Destination directory. If ``None``, ``create_path(filename)`` is
            used.

    Returns:
        Absolute path to the saved ``raw_<filename>.pdf``.

    Raises:
        requests.RequestException: If downloading the popup URL fails.
        OSError: If the file cannot be written.
    """
    with page.expect_popup() as popup_info:
        link_button.click()

    url = popup_info.value.url
    r = requests.get(url)

    filename = "raw_" + filename + ".pdf"

    if not path:
        path = create_path(filename)

    pdf_path = path / filename

    with open(pdf_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    browser.close()

    return pdf_path


def export_df(
    df: pd.DataFrame,
    filename: str,
    path: Path | None = None,
) -> None:
    """Export a pandas DataFrame to ``<path>/<filename>.tsv``.

    Args:
        df: DataFrame to write.
        filename: Output file stem (``.tsv`` is appended).
        path: Destination directory. If ``None``, ``create_path(filename)`` is
            used.

    Raises:
        OSError: If the file cannot be written.
    """
    file_final = filename + ".tsv"

    if not path:
        path = create_path(filename)

    df.to_csv(path / file_final, sep="\t", index=False)


def download_file(
    request: requests.Response,
    full_filename: str,
    path: Path,
    chunk_size: int = 8192,
) -> Path:
    """Stream a request's content to ``<path>/<full_filename>`` in binary chunks.

    Args:
        request: Streaming ``requests`` response to drain.
        full_filename: Filename including extension.
        path: Destination directory (must already exist).
        chunk_size: Bytes per chunk read from the response.

    Returns:
        Absolute path to the written file.

    Raises:
        OSError: If the file cannot be written.
    """
    file_path = path / full_filename

    with open(file_path, "wb") as f:
        for chunk in request.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)

    return file_path
