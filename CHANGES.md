# What changed in this fork

This is a tour of how this fork of [`dsi-clinic/IDI_pension_funds_scraping`](https://github.com/dsi-clinic/IDI_pension_funds_scraping) differs from the version you wrote. It's organized by theme rather than by file, with **before / after** snippets so you can see the shape of each change. Everything here is meant as a worked example — the goal is "here's what 'good' looks like in this codebase," not "you should have known to do this."

If you want to read the cleanest examples directly:

- [src/pipeline/scrapers/amf.py](src/pipeline/scrapers/amf.py)
- [src/pipeline/scrapers/ap2.py](src/pipeline/scrapers/ap2.py)
- [src/pipeline/scrapers/ap3.py](src/pipeline/scrapers/ap3.py)

## 1. The project is now a package, not a folder of scripts

**Before.** Each scraper was a standalone `.py` file. Running everything meant `python amf.py && python ap2.py && …`, and there was no shared way to discover what scrapers existed.

**After.** Everything lives under [`src/pipeline/`](src/pipeline/) as a real Python package with three pieces:

- A **registry** ([src/pipeline/registry.py](src/pipeline/registry.py)) that lets each scraper opt in with a one-line decorator:

  ```python
  @register("amf")
  def scrape_amf() -> None:
      ...
  ```

- A **CLI** ([src/pipeline/cli.py](src/pipeline/cli.py)) that exposes one command for everything:

  ```bash
  uv run idi-scrape list             # see every registered scraper
  uv run idi-scrape run              # run all of them
  uv run idi-scrape run amf railov   # run a subset
  ```

- A **utils** package ([src/pipeline/utils/](src/pipeline/utils/)) for shared helpers — file I/O, date parsing, etc. Scrapers import from `pipeline.utils` instead of redefining helpers locally.

**Why it matters:** new scrapers plug in by adding a file and a decorator; the CLI picks them up automatically. There's one place to fix shared bugs. And things like "skip this scraper if it fails, but keep going" can be implemented once in the CLI rather than copy-pasted into every script.

## 2. Output paths are owned by the utilities, not the scrapers

This was the biggest single change. Before, every scraper built its own paths and filenames, which produced an inconsistent layout: some scrapers wrote `raw_amf.pdf`, others `amf.pdf`; some `<name>.tsv`, others `<name>_swedish.tsv`. One scraper (`danica`) accidentally passed a `Path` object where a filename string was expected, which had been silently broken for who knows how long.

**Before.** Scrapers were responsible for the path:

```python
path = utils.create_path("amf")            # data/amf/<today>/
pdf_path = path / "raw_amf.pdf"            # filename invented by the scraper
with open(pdf_path, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)
# … later …
df.to_csv(path / "amf.tsv", sep="\t", index=False)
```

**After.** Scrapers pass their *name and a date*; utilities figure out the rest:

```python
today = datetime.date.today()
pdf_path = utils.download_file(response, "amf", today, "pdf")
# … later …
utils.export_data(df, "amf", today)
```

Every scraper now writes to the same canonical layout:

```
data/disclosures/<scraper>/<YYYY-MM-DD>/
    raw/      <- original downloaded source (PDF, HTML, JSON, …)
    clean/    <- processed TSV(s)
```

Scrapers that produce multiple outputs (`ap2`, `ap3`, `ap4`, `zorg_welzjin`) pass an extra `subname=`:

```python
utils.export_data(df_swedish, "ap3", today, subname="swedish")
# → data/disclosures/ap3/<today>/clean/ap3_swedish.tsv
```

**The lesson:** it's better to give a utility *more* responsibility than to make every caller repeat the same path-building code. If five files have the same five lines of glue, that's a sign the glue belongs in a function.

## 3. Error handling: never trust an unbounded loop or an unchecked match

This pattern came up over and over and was the root cause of most "this scraper has been silently broken" bugs:

**Before.**

```python
url = ""
while not url:
    req = requests.get(f"https://.../report_{year}.pdf")
    if req.status_code > 199 and req.status_code < 227:   # what?!
        url = req.url
        break
    year -= 1
```

Two separate problems:

1. **Unbounded retry.** If the URL scheme ever changes, this walks the year backwards *forever*. There's no way for the loop to know it should stop.
2. **Hand-rolled status check.** `> 199 and < 227` accidentally accepts `226 IM Used` (a real HTTP status nobody uses) and rejects nothing useful. The intent was just "OK". Standard library has `response.ok` — use it.

**After.**

```python
_MAX_YEARS_BACK = 5  # bound the retry; raise a clear error if exhausted

def _fetch_latest_report() -> requests.Response:
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
```

**The lesson:** any retry loop should answer two questions:

1. **What's the upper bound?** Pick a reasonable number, then *raise a clear error* when you hit it. "Run forever" isn't an acceptable answer — it just hides a broken site behind a hung process.
2. **What's the right success check?** If the standard library has it (`response.ok`, `path.exists()`, etc.) — use it instead of hand-rolling.

The same pattern bit several scrapers in another way. **Match objects can be `None`.**

**Before.**

```python
match = re.search(pattern, text)
issuer, value = match.groups()  # AttributeError if no match
```

**After.**

```python
match = re.search(pattern, text)
if not match:
    raise RuntimeError(
        f"PensionDanmark: report date not found — page layout may have changed."
    )
issuer, value = match["issuer"], match["value"]
```

Either guard the match, or fail loudly with a useful error. *Never* let a `None` propagate — it just turns into an opaque crash on a line far from the actual problem.

## 4. Comments should say *why*, not *what*

This is the single biggest stylistic change across the codebase.

**Before.** Every step gets narrated:

```python
# Get groups into variables
issuer, ownership, value, shares, currency, P_I, ex_rate, sectype, report_date = match
# Order according to schema
entry = [
    "AP7",
    issuer,
    sectype,
    # … 7 more lines …
]
# Append
entries.append(entry)
```

The reader can already see that `match` is being unpacked, that `entry` is a list, and that you're calling `.append`. The comments don't add information; they just take up space and rot when the code changes.

**After.**

```python
rows.append(
    [_PENSION_NAME, m["issuer"], m["sectype"], ..., _HOLDINGS_URL]
    for m in _ROW_PATTERN.finditer(body)
)
```

When comments *do* appear in the cleaned-up code, they explain non-obvious facts:

```python
# Holdings rows are rendered in T-Star-Medium at 7pt. pdfplumber prefixes
# subsetted fonts with a random tag (e.g. "TNBONQ+"), so match the suffix.
_ENTRY_FONT_SUFFIX = "T-Star-Medium"
```

```python
# AP3 changed the layout of the equity reports at some point. Patterns
# below reflect the *current* format (sampled 2026-04-30); re-measure
# them if AP3 changes the layout again.
```

**The lesson:** before writing a comment, ask "would removing this confuse a future reader?" If the answer is no, don't write it. The good comments document hidden constraints, surprising choices, and "you'll want to update this when X happens" notes.

## 5. Magic numbers and inline regexes belong at module level

**Before.** Scattered through the function body:

```python
def scrape_amf():
    ...
    sections = [
        p.crop((0, 0, 215, p.height)),
        p.crop((215, 0, 385, p.height)),
        p.crop((385, 0, p.width, p.height)),
    ]
    ...
    if (t["chars"][0]["fontname"] == "TNBONQ+T-Star-Medium"
            and size == 7):
        ...
    pattern = re.compile(
        r"(?P<issuer>[A-Z\d][A-Za-z\d \-+&'/]+) ..."
    )
```

The numbers `215, 385` mean nothing without staring at the PDF. The font name `TNBONQ+T-Star-Medium` mixes a stable suffix (`T-Star-Medium`) with a random subsetting prefix (`TNBONQ+`) that changes every time the PDF is regenerated. The regex is recompiled on every call.

**After.**

```python
# The PDF lays out holdings in three columns. These x-coordinates split
# a page into left/middle/right crops; re-measure if AMF changes the
# layout.
_COLUMN_SPLITS = (215, 385)

# Holdings rows are rendered in T-Star-Medium at 7pt. pdfplumber prefixes
# subsetted fonts with a random tag (e.g. "TNBONQ+"), so match the suffix.
_ENTRY_FONT_SUFFIX = "T-Star-Medium"
_ENTRY_FONT_SIZE = 7
```

**The lesson:** every constant has two readers — the one writing it (who knows what it means) and the one updating it five months from now (who doesn't). Naming the constant at module level, with a comment explaining how to re-derive it, is how you tell the second reader what they need to know.

## 6. Named regex groups beat positional unpacking

**Before.**

```python
matches = re.findall(pattern, text)
for match in matches:
    issuer, ticker, sectype, currency, shares, value, isin = match
```

If you ever rearrange the regex's groups, the unpacking silently *succeeds* and gives you wrong data — `issuer` ends up holding the ticker, `ticker` holds the sectype, and so on. There's no error; the TSV just quietly contains garbage.

**After.**

```python
for m in _ROW_PATTERN.finditer(text):
    rows.append([
        _PENSION_NAME,
        m["issuer"], m["sectype"], m["isin"],
        ...
    ])
```

Named access (`m["issuer"]`) reads more clearly *and* fails loudly if a group is missing. `finditer` also avoids materializing the full list of tuples in memory.

## 7. Playwright must always close cleanly

This was a subtle bug that bit a couple of scrapers. The original pattern was:

**Before.**

```python
playwright = sync_playwright().start()
browser = playwright.chromium.launch(...)
# … work …
playwright.stop()
```

If anything between `start()` and `stop()` raised an exception, `playwright.stop()` never ran. The Playwright instance stayed alive in the same Python process, and **the next scraper to call `sync_playwright().start()` got a confusing error**: *"Sync API inside the asyncio loop"*. So a failure in `pme` would silently break `railov` and `vervoer` — even though `railov` and `vervoer` had nothing wrong with them.

**After.**

```python
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(...)
    # … work …
```

The `with` block guarantees `playwright.stop()` runs no matter how we leave the block. Every Playwright-using scraper now follows this pattern.

The same general lesson applies to `pdfplumber.open`, file handles, requests sessions, and anything else with a `close()` method: if you can use a context manager, use it. The runtime will clean up correctly even when your code crashes in the middle.

## 8. Cookie banners need a short timeout

**Before.**

```python
page.get_by_role("button", name="Accept all").click()
```

If the cookie banner never appears (which is *common* in headless Chromium), `.click()` waits 30 seconds for the locator before raising. That's both slow and confusing.

**After.**

```python
try:
    page.get_by_role("button", name="Accept all").click(timeout=5000)
except Exception:
    pass  # banner didn't appear; that's fine
```

A short explicit timeout + a swallowed exception turns "30s of mysterious hang" into "<5s of correctly skipped step."

## 9. Real bugs found and fixed in specific scrapers

These are the ones worth knowing about as cautionary tales:

- **`ap3.py`** — the link-finding regex used `[december0-9\-]+` (a *character class*) instead of `(?:december|june)` (an *alternation*). A character class matches any sequence of those letters and digits, so it accepted URLs that didn't contain "december" or "june" at all. Separately, AP3 had silently changed the PDF column order at some point — the original row regexes hadn't matched any rows for an unknown amount of time. The fork rewrites the regexes against the current layout and adds a word-position-based parser for the foreign-stocks PDF (which uses space-separated thousands, making text-only parsing ambiguous).
- **`sampension.py`** — the OS detection had `elif osys == "iOS":` for the macOS branch. `platform.system()` returns `"Darwin"` on macOS; iOS isn't a desktop OS and doesn't run Python. That entire branch was dead from the day it was written. The whole 60-line OS-specific path search is now `shutil.which("tesseract")`, which works on all three platforms and finds anything on `PATH`.
- **`bpfbouw.py`** — used `locale.setlocale(LC_ALL, "nl_NL.UTF-8")` to parse a Dutch month name. That locale isn't installed by default on most Linux/macOS machines, *and* the call mutates global state, which can break other scrapers running in the same process. Replaced with an explicit `_DUTCH_MONTHS` dict.
- **`pme.py`** — the page-advance loop fired up to 30 click retries back-to-back without waiting for the DOM to update, so it always thought the widget was stuck. Now polls `inner_text` every 200ms with a 15-second total timeout, which gives the actual page render time to land.
- **`nbim.py`** — same `code > 199 and code < 227` status-code check as several others; same `year + 1` offset trick for the year-walk. Replaced with `response.ok` and a clean candidate-list iteration.
- **`danica.py`** — passed `path / "raw_danica.pdf"` (a `Path`) where the utility expected a string filename. The new utility signature doesn't take a filename at all, so this class of bug becomes impossible.

## 10. Tooling and habits

A few things worth picking up that aren't visible in the diff but make the day-to-day better:

- **`uv` for everything.** Dependency install (`uv sync`), running scripts (`uv run idi-scrape ...`), running tools (`uv run ruff check src/`). One tool, one lockfile.
- **Ruff in the loop.** `uv run ruff check src/pipeline` catches dozens of small issues — unused imports, unsorted imports, missing type annotations — that a code review would otherwise spend time on. Run it before committing.
- **Pre-commit runs ruff for you.** A [.pre-commit-config.yaml](.pre-commit-config.yaml) is checked in that runs `ruff` (lint, with `--fix`) and `ruff-format` on every commit. After cloning, run `uv sync` to install the `pre-commit` package, then `uv run pre-commit install` once to register the git hook. From then on, every `git commit` reformats and lints staged files — if anything had to be fixed, the commit fails so you can review and re-stage. Both hooks read their config from `[tool.ruff]` in `pyproject.toml`, so there's exactly one place to change rules. The `archive/` folder is deliberately excluded so the historical work-in-progress notebooks aren't reformatted on every commit.
- **Read the log.** The CLI writes [log.log](log.log) on every run. If a scraper fails, that's the first place to look — the error messages added in this refactor are designed to point at the actual cause ("AP3: did not find a 'swedish' report within 5 years — the URL scheme may have changed."), so the log usually tells you where to start.

## What this fork *deliberately* didn't do

- **Reformatted everything to a different style.** The refactor stays close to the structure of each original scraper. Most files still walk the PDF page-by-page with regex, just with bounded loops, named constants, and proper error handling.
- **Added tests.** There's no test suite yet. Adding pytest fixtures with cached PDFs would be a great next step.
- **Changed the IDI schema.** Where new data fields became available (e.g. AP3's foreign report now exposes ownership/voting %), they're flagged as TODOs rather than silently merged into the output. Schema decisions are domain choices that need a human in the loop.

## Where to look next

If you're making future changes:

1. Skim [src/pipeline/scrapers/amf.py](src/pipeline/scrapers/amf.py) for the simple worked example.
2. Skim [src/pipeline/scrapers/ap3.py](src/pipeline/scrapers/ap3.py) for a more complex one (multi-output + a Playwright + word-position parsing).
3. Check the module docstring of any scraper you're editing for a "Cleanup TODO" block — those are issues I noticed but didn't fix, with pointers to where similar problems were resolved elsewhere.
4. After cloning, run `uv sync` and then `uv run pre-commit install` — that wires lint+format into every commit. (You can still run it manually with `uv run pre-commit run --all-files`.)
5. Run `uv run idi-scrape run <your-scraper>` and check `log.log` for the result.

## Possible next tasks

- Configure repository to use Docker, which will handle installation of binaries for Playwright and Tesseract more smoothly
- Use disclosure reporting dates for the file paths instead of today's date
- Consider how the scrapers should be updated and maintained over time; can we make them general enough to withstand year-to-year layout changes? Students can test their scraper modules on older disclosures if they are available online to test how robust their current approaches are.
