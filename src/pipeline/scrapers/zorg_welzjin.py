"""Pensioenfonds Zorg & Welzijn Scraper.

Scrapes Pensioenfonds Zorg & Welzijn ("Zorg & Welzijn" = Health &
Welfare), a Dutch pension fund. The fund publishes per-asset-class JSON
files at predictable URLs keyed off the report quarter; this scraper
finds the most recent quarter that's actually live, downloads each
asset class's JSON, normalizes the cell values, and emits one TSV per
class.

Note: only the December quarter has been verified end-to-end. The
other three fiscal quarters use the same URL scheme so they should
work, but expect to spot-check.

The registry key is intentionally ``zorg_welzjin`` (the original
typo'd transliteration) rather than the proper Dutch spelling
``zorg_welzijn``; renaming the registry key would change the on-disk
output path, so do that as a separate one-time migration.
"""

import datetime
import logging
import re
from pathlib import Path

import pandas as pd
import requests

from pipeline import utils
from pipeline.registry import register

_PENSION_NAME = "Pensioenfonds Zorg & Welzijn"
_DATA_SOURCE_URL = (
    "https://www.pfzw.nl/over-pfzw/beleggen-voor-een-goed-pensioen/"
    "soorten-beleggingen.html"
)
_BASE_URL = "https://www.pfzw.nl/content/dam/pfzw/web/transparantielijsten/"
_CURRENCY = "EUR"
_MULTIPLIER = "x1_000_000"

_MAX_YEARS_BACK = 5

# Quarters in the order we try them (newest within a year first).
# The mapping is {quarter_number: "DD-Month-YYYY" suffix used by Z&W}.
_QUARTER_DAY_LABELS = {
    "4": "31-december",
    "3": "30-september",
    "2": "30-june",
    "1": "31-march",
}

# Asset-class JSONs to fetch. Each becomes its own TSV.
_ASSET_LABELS = (
    "Private_Equity",
    "Listed_Real_Estate",
    "Equities_corporate",
    "Credit_Risk_Sharing",
    "Externe_vermogensbeheerders",
)

# Source-column → IDI-column mapping for the per-asset DataFrames.
# Ordered so that ambiguity (multiple source columns mapping to one IDI
# column) is resolved by first-match-wins.
_COLUMN_MAP: tuple[tuple[str, str], ...] = (
    ("Investeerder", "Issuer - Name"),
    ("CreditRiskSharingBank", "Issuer - Name"),
    ("Land", "Issuer - Country Name"),
    ("Sector", "Issuer - Sector"),
    ("Categorie", "Security - Type"),
    ("TypeInvestering", "Security - Type"),
    ("Marktwaarde", "Security - Market Value - Amount"),
    ("IndicatieMarktwaarde", "Security - Market Value - Amount"),
    ("IndicatieMarktwaardeEur", "Security - Market Value - Amount"),
    ("Aandeel", "Stock - Percent Ownership"),
)

_RANGE_VALUE_PATTERN = re.compile(r"\d+ - (\d+) M")
_SUFFIX_VALUE_PATTERN = re.compile(r"([.\d]+)M")
_RANGE_PERCENT_PATTERN = re.compile(r"\d+ - (\d+) %")


def _normalize_value(cell: object) -> str:
    """Strip HTML tags and reduce a Z&W market-value cell to a number.

    Args:
        cell: Raw cell value from the JSON; any value coercible to ``str``.

    Returns:
        The numeric portion of the cell. ``"100 - 500 M"`` becomes
        ``"500"``; ``"12.3M"`` becomes ``"12.3"``; otherwise the input
        is returned with HTML tags removed.
    """
    text = re.sub(r"<.+>+", "", str(cell))
    range_match = _RANGE_VALUE_PATTERN.search(text)
    if range_match:
        return range_match.group(1)
    suffix_match = _SUFFIX_VALUE_PATTERN.search(text)
    if suffix_match:
        return suffix_match.group(1)
    return text


def _normalize_percent(cell: str) -> str:
    """Convert a "5 - 10 %" range to a decimal string ("0.1").

    Args:
        cell: Raw cell value (typically already string-typed).

    Returns:
        Decimal-form percent (``"0.1"``) when the cell matches the range
        format, or the input unchanged otherwise.
    """
    match = _RANGE_PERCENT_PATTERN.search(cell)
    if not match:
        return str(cell)
    return str(int(match.group(1)) / 100)


def _candidate_quarters() -> list[tuple[str, str, int]]:
    """Yield ``(quarter_number, "DD-Month-YYYY", year)`` tuples newest-first.

    Returns:
        A list of candidate (quarter, date-suffix, year) triples in
        newest-first order, bounded to ``_MAX_YEARS_BACK`` years.
    """
    this_year = datetime.date.today().year
    triples: list[tuple[str, str, int]] = []
    for year in range(this_year, this_year - _MAX_YEARS_BACK, -1):
        for quarter, day_label in _QUARTER_DAY_LABELS.items():
            triples.append((quarter, f"{day_label}-{year}", year))
    return triples


def _find_report_quarter() -> tuple[str, str, int]:
    """Probe Z&W's transparency JSON URLs to find the most recent live quarter.

    Returns:
        A ``(quarter, date_suffix, year)`` triple whose Private_Equity
        JSON returns OK.

    Raises:
        RuntimeError: If no candidate quarter responds OK within
            ``_MAX_YEARS_BACK`` years.
    """
    for quarter, date_suffix, year in _candidate_quarters():
        probe_url = (
            f"{_BASE_URL}transparantielijsten-{date_suffix}/"
            f"Private_Equity_{year}Q{quarter}.json"
        )
        if requests.get(probe_url).ok:
            return quarter, date_suffix, year
    raise RuntimeError(
        f"Z&W: no transparency JSON found in the last {_MAX_YEARS_BACK} "
        "years — the URL scheme may have changed."
    )


def _format_report_date(date_suffix: str) -> str:
    """Convert a "DD-Month-YYYY" suffix to ``YYYY-MM-DD``.

    Args:
        date_suffix: A string like ``"31-december-2024"``.

    Returns:
        The same date in ISO format, e.g. ``"2024-12-31"``.
    """
    day = date_suffix[:2]
    year = date_suffix[-4:]
    month_word = re.sub(r"[\d\-]+", "", date_suffix)
    return f"{year}-{utils.convert_month(month_word)}-{day}"


def _download_assets(
    today: datetime.date,
    quarter: str,
    date_suffix: str,
    year: int,
) -> list[tuple[str, Path]]:
    """Download every available asset-class JSON for a given quarter.

    Args:
        today: Date stamp for the run directory.
        quarter: Quarter number ("1"-"4").
        date_suffix: ``"DD-Month-YYYY"`` suffix used in Z&W URLs.
        year: Four-digit year (matches ``date_suffix`` and is also part
            of the JSON URL, separately from the suffix).

    Returns:
        List of ``(label_lower, json_path)`` for each asset that
        responded OK. Missing assets are logged and skipped.
    """
    results: list[tuple[str, Path]] = []
    for label in _ASSET_LABELS:
        url = (
            f"{_BASE_URL}transparantielijsten-{date_suffix}/"
            f"{label}_{year}Q{quarter}.json"
        )
        response = requests.get(url, stream=True)
        if response.ok:
            path = utils.download_file(
                response,
                "zorg_welzjin",
                today,
                "json",
                subname=label.lower(),
            )
            results.append((label.lower(), path))
            logging.info("Z&W - downloaded %s", label)
        else:
            logging.info("Z&W - failed to download %s", label)
    return results


def _build_dataframe(raw: pd.DataFrame, report_date: str) -> pd.DataFrame:
    """Map a raw Z&W JSON DataFrame onto the IDI schema.

    Args:
        raw: DataFrame loaded directly from one asset-class JSON.
        report_date: ``YYYY-MM-DD`` report date for the run.

    Returns:
        A new DataFrame with IDI column names and constants filled in.
    """
    raw = raw.apply(
        lambda col: col.map(_normalize_percent)
        if col.name == "Aandeel"
        else col.map(_normalize_value)
    )

    out = pd.DataFrame({"Shareholder - Name": [_PENSION_NAME] * len(raw)})
    seen_targets: set[str] = set()
    for source, target in _COLUMN_MAP:
        if source in raw.columns and target not in seen_targets:
            out[target] = raw[source].values
            seen_targets.add(target)

    out["Security - Report Date"] = report_date
    if "Security - Market Value - Amount" in seen_targets:
        out["Security - Market Value - Multiplier"] = _MULTIPLIER
        out["Security - Market Value - Currency Code"] = _CURRENCY
    out["Data Source URL"] = _DATA_SOURCE_URL
    return out


@register("zorg_welzjin")
def scrape_zw() -> None:
    """Scrape Pensioenfonds Zorg & Welzijn and write per-asset TSVs."""
    today = datetime.date.today()
    quarter, date_suffix, year = _find_report_quarter()
    report_date = _format_report_date(date_suffix)

    for label, json_path in _download_assets(today, quarter, date_suffix, year):
        raw = pd.read_json(json_path)
        df = _build_dataframe(raw, report_date)
        utils.export_data(df, "zorg_welzjin", today, subname=label)


if __name__ == "__main__":
    scrape_zw()
