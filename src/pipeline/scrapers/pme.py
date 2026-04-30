"""PME Pensioenfonds Scraper.

Scrapes PME, the Dutch metal- and tech-industries pension fund. PME's
holdings are rendered into a paginated dynamic widget rather than served
as a file, so the scraper drives the page with Playwright, walks the
"next" button N times (where N is read off the "Page 1 of N" indicator),
and parses each page's text with a single tab-separated row regex.

The script can take 5–10 minutes to complete; if it fails midway,
re-running once or twice usually succeeds (a network-flakiness symptom).
"""

import datetime
import logging
import re

import pandas as pd
from playwright.sync_api import Locator, Page, sync_playwright
from playwright.sync_api import TimeoutError as PWTimeoutError

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "PME pensioenfonds"
_HOLDINGS_URL = "https://www.pmepensioen.nl/en/investments/we-do-invest-in"
_CURRENCY = "EUR"
_MULTIPLIER = "x1"
# PME doesn't publish a current report date, but does publish the *next*
# report date, and reports run on a quarterly cycle. Subtracting three
# months from the next report date approximates the current one.
_QUARTER_OFFSET_MONTHS = -3

# Each holding is a tab-separated row inside one of the paginated
# widget panels. Edge cases: issuer names contain symbols and digits;
# country names contain multiple words and may carry a ", Republic of"
# suffix that we deliberately drop.
_ROW_PATTERN = re.compile(
    r"(?P<issuer>[A-Za-z\d\. /&\-,]+)\t"
    r"(?P<value>[\d\.]+)\t"
    r"(?P<country>[A-Za-z]+(?: [A-Za-z ]+)?)(?:, Republic of)?\t"
    r"(?P<sector>[A-Za-z ,\-]+)\t"
    r"(?P<sectype>[A-Za-z ]+)"
)
_PAGE_COUNT_PATTERN = re.compile(r"Page 1 of (?P<max>\d+)")
_NEXT_REPORT_DATE_PATTERN = re.compile(
    r"(?P<month>[A-Z][a-z]+) (?P<day>\d+)[a-z]+, (?P<year>\d{4})"
)
# Once the rendered text reaches this label we're past equity holdings.
_END_OF_EQUITIES_LINE = "Investments in the Netherlands"

# The PME page lazy-loads the equity widget, so the "next" link can take
# tens of seconds to appear. Use Playwright's built-in wait_for with a
# generous timeout instead of polling.
_NEXT_BUTTON_TIMEOUT_MS = 60_000

# After clicking "next", how long to wait for the widget's inner text to
# actually change before declaring the page stuck.
_PAGE_ADVANCE_TIMEOUT_MS = 15_000
_PAGE_ADVANCE_POLL_MS = 200


def _wait_for_next_button(page: Page) -> Locator:
    """Wait for the equity widget's "next" link and return it.

    Args:
        page: Playwright page on the holdings URL.

    Returns:
        The Locator for the equity widget's "next" link.

    Raises:
        PWTimeoutError: If the link doesn't appear within
            ``_NEXT_BUTTON_TIMEOUT_MS`` ms.
    """
    locator = page.get_by_role("link", name="next", exact=False).first
    locator.wait_for(state="attached", timeout=_NEXT_BUTTON_TIMEOUT_MS)
    return locator


def _wait_for_text_change(page: Page, previous: str) -> str:
    """Poll ``page.inner_text("div")`` until it differs from ``previous``.

    Args:
        page: Playwright page hosting the widget.
        previous: The last-seen widget text. The poll returns as soon
            as the widget renders anything different.

    Returns:
        The new widget text.

    Raises:
        RuntimeError: If the text doesn't change within
            ``_PAGE_ADVANCE_TIMEOUT_MS`` ms — likely means the widget
            failed to advance.
    """
    elapsed = 0
    while elapsed < _PAGE_ADVANCE_TIMEOUT_MS:
        current = page.inner_text("div")
        if current != previous:
            return current
        page.wait_for_timeout(_PAGE_ADVANCE_POLL_MS)
        elapsed += _PAGE_ADVANCE_POLL_MS
    raise RuntimeError(
        "PME: page text did not change after clicking 'next' within "
        f"{_PAGE_ADVANCE_TIMEOUT_MS}ms — the widget may be stuck."
    )


def _approximate_report_date(text: str) -> str:
    """Subtract one quarter from the page's "next report" date.

    Args:
        text: Visible text of the holdings page.

    Returns:
        ``YYYY-MM-DD`` string, or ``""`` if no next-report date is
        visible on the page.
    """
    match = _NEXT_REPORT_DATE_PATTERN.search(text)
    if not match:
        return ""
    month = utils.convert_month(match["month"], _QUARTER_OFFSET_MONTHS)
    return f"{match['year']}-{month}-{int(match['day']):02d}"


@register("pme")
def scrape_pme() -> None:
    """Scrape PME and write a TSV under ``data/disclosures/pme/<YYYY-MM-DD>/``."""
    today = datetime.date.today()
    raw_dir = utils.build_dir("pme", today, "raw")

    rows: list[list[str]] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True, slow_mo=1, channel="chromium"
        )
        page = browser.new_page()
        page.goto(_HOLDINGS_URL)
        try:
            page.get_by_role("button", name="Decline").click(timeout=5000)
        except PWTimeoutError:
            pass

        next_button = _wait_for_next_button(page)
        next_button.scroll_into_view_if_needed()

        text = page.inner_text("div")
        page_count_match = _PAGE_COUNT_PATTERN.search(text)
        if not page_count_match:
            raise RuntimeError(
                "PME: page count ('Page 1 of N') not found on the holdings "
                "page — the widget layout may have changed."
            )
        page_count = int(page_count_match["max"])
        report_date = _approximate_report_date(text)

        last_text = ""
        with open(raw_dir / "pme.txt", "w") as snapshot:
            logging.info("PME - Begin cycling through pages")
            for page_idx in range(page_count):
                current_text = _wait_for_text_change(page, last_text)
                last_text = current_text
                snapshot.write(current_text)

                for line in current_text.splitlines():
                    if line == _END_OF_EQUITIES_LINE:
                        break
                    match = _ROW_PATTERN.search(line)
                    if not match:
                        continue
                    rows.append(
                        [
                            _PENSION_NAME,
                            match["issuer"].strip(),
                            match["country"].strip(),
                            match["sector"].strip(),
                            match["sectype"].strip(),
                            report_date,
                            match["value"].strip(),
                            _MULTIPLIER,
                            _CURRENCY,
                            _HOLDINGS_URL,
                        ]
                    )

                # Click for the next iteration unless we've just
                # processed the last page.
                if page_idx < page_count - 1:
                    next_button.click()

        browser.close()
        logging.info(
            "PME - Finished cycling through %d pages.", page_count
        )

    df = pd.DataFrame(
        rows,
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
    utils.export_data(df, "pme", today)


if __name__ == "__main__":
    scrape_pme()
