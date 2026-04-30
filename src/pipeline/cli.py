"""Command-line interface: run one, several, or all registered scrapers."""

import argparse
import logging
import sys
import warnings
from pathlib import Path

from pipeline import registry

warnings.filterwarnings("ignore")

logging.basicConfig(
    filename=Path.cwd() / "log.log",
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(message)s",
)


def _build_parser(available: list[str]) -> argparse.ArgumentParser:
    """Construct the argument parser used by ``main``.

    Args:
        available: Registered scraper names, used in help text.

    Returns:
        Configured argument parser with ``run`` and ``list`` subcommands.
    """
    parser = argparse.ArgumentParser(
        description="Run pension fund scrapers.",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="{run,list}",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run one, several, or all registered scrapers.",
    )
    run_parser.add_argument(
        "scrapers",
        nargs="*",
        help=(
            "Scrapers to run. If omitted, runs all. "
            f"Available: {', '.join(available)}."
        ),
    )

    subparsers.add_parser(
        "list",
        help="List registered scrapers and exit.",
    )

    return parser


def _run(selected: list[str], available: list[str]) -> None:
    """Execute the chosen scrapers and log the outcome of each.

    Args:
        selected: Scraper names requested on the CLI (empty means all).
        available: All registered scraper names.
    """
    targets = selected or available
    logging.info("Begin scraping")
    for name in targets:
        scrape = registry.get_scraper(name)
        try:
            scrape()
            logging.info(f"Successfully ran {name}.")
        except Exception as e:
            logging.info(f"Issue running {name}. {e}")
    logging.info("Finished running")


def main() -> int:
    """Parse arguments and dispatch to the requested subcommand.

    Returns:
        Process exit code (``0`` on success).
    """
    registry.discover()
    available = registry.list_scrapers()

    parser = _build_parser(available)
    args = parser.parse_args()

    if args.command == "list":
        for name in available:
            print(name)
        return 0

    unknown = [s for s in args.scrapers if s not in available]
    if unknown:
        parser.error(
            f"Unknown scraper(s): {', '.join(unknown)}. "
            f"Available: {', '.join(available)}."
        )

    _run(args.scrapers, available)
    return 0


if __name__ == "__main__":
    sys.exit(main())
