# IDI — Scraping Pension Funds

A command-line tool that scrapes shareholding disclosures from pension funds for [Inclusive Development International](https://www.inclusivedevelopment.net/).

## Getting started

You need [Python 3](https://www.python.org/downloads/) and [uv](https://docs.astral.sh/uv/getting-started/installation/). The `sampension` scraper additionally requires [Tesseract OCR](https://tesseract-ocr.github.io/tessdoc/Installation.html); skip it if you don't need that fund.

```bash
git clone <this-repo>
cd IDI_pension_funds_scraping
uv sync
```

## Running scrapers

| Action | Command |
|---|---|
| List every registered scraper | `uv run idi-scrape list` |
| Run every scraper | `uv run idi-scrape run` |
| Run a subset by name | `uv run idi-scrape run amf railov` |

For debugging, individual scrapers can also be invoked directly from [src/pipeline/scrapers/](src/pipeline/scrapers/).

## Output layout

```
data/disclosures/<scraper>/<YYYY-MM-DD>/
    raw/      original downloaded source (PDF, HTML, JSON, …)
    clean/    processed TSV(s)
```

Scrapers that produce multiple outputs (e.g. `ap3`) suffix each file with a sub-name — for example `clean/ap3_swedish.tsv`.

## Repository layout

- [src/pipeline/](src/pipeline/) — the CLI entry point, scraper registry, shared utilities, and one module per scraper.
- [data/](data/) — scraped output and supplemental reference data (e.g. the Dutch country list used by `bpfbouw`).
- [archive/wip_scrapers/](archive/wip_scrapers/) — works in progress, old, and un-automated scrapers.
- [log.log](log.log) — written by the CLI on every run; check this first when a scraper fails.
