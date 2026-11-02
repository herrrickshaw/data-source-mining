# Under-used open data sources — World Bank and Agmarknet into DuckDB

A single collector script that pulls two fully open data sources — the World Bank Indicators API and the
Agmarknet daily mandi (APMC market) price feed on data.gov.in — into one local DuckDB file,
`underused_sources.duckdb`, with one table per source. Domain: Indian and comparative macro / agriculture /
energy / trade indicators, plus Indian wholesale farm-produce prices. The DuckDB file produced by the
2026-07-21 pull is tracked in the repo.

> Status: research-only snapshot. Both sources are open (no registration; data.gov.in's publicly documented
> sample key is used). Figures are the publishers' own; not investment advice.

<!-- how-to-use-with-ai -->
> **New here?** See [How to use this repo with AI (Claude, Gemini, ChatGPT)](HOW_TO_USE_WITH_AI.md) — clone it, paste it into an AI tool, and ask in plain English.

## What it does
- `mine_underused_sources.py` fetches 12 World Bank indicators (GDP, growth, CPI, agri value-added, fertiliser use, exports/imports % GDP, fossil/renewable energy shares, FDI, real interest rate, INR/USD) for 7 countries (IND, USA, CHN, BRA, DEU, JPN, KOR), all available years, into table `worldbank_indicators` (rebuilt on every run).
- It pages through the Agmarknet "current daily price of various commodities" resource on data.gov.in into table `mandi_prices`, deleting and re-inserting the current day's rows so a re-run on the same day is idempotent.
- Plain `urllib` with a browser User-Agent, 3 retries with linear back-off, and 0.3–0.5 s pauses between requests; the only third-party dependency is `duckdb`.
- `--wb-only` / `--mandi-only` select one source; the run ends by printing every table's row count.
- The tracked `underused_sources.duckdb` holds 4,532 World Bank rows (1960–2025) and 4 mandi rows (pull of 2026-07-21).

## Quick start
```bash
git clone https://github.com/herrrickshaw/data-source-mining.git
cd data-source-mining
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 mine_underused_sources.py            # both sources -> underused_sources.duckdb
python3 mine_underused_sources.py --wb-only  # or --mandi-only
```
Query the result with any DuckDB client, e.g. `duckdb underused_sources.duckdb "SELECT * FROM worldbank_indicators WHERE country='IND' AND indicator='gdp_growth_pct' ORDER BY year DESC LIMIT 5"`.

## How it works
| Stage | Function | Output |
|---|---|---|
| GET + JSON parse with retry/back-off | `get_json()` | parsed response |
| Loop countries × indicators, follow `pages` in the WB envelope, drop null values | `mine_worldbank()` | `CREATE OR REPLACE TABLE worldbank_indicators(country, indicator_code, indicator, year, value, source_last_updated)` |
| Page the data.gov.in resource by `offset`/`limit` (1,000), stamp `pulled_on` with today's date | `mine_mandi()` | `CREATE TABLE IF NOT EXISTS mandi_prices(state, district, market, commodity, variety, grade, arrival_date, min_price, max_price, modal_price, pulled_on)` after `DELETE … WHERE pulled_on = today` |
| Argument parsing, connection, summary print | `main()` | row counts on stdout |

Design notes: DuckDB rather than CSV so both sources sit in one queryable file; the World Bank table is
replaced wholesale (the API returns full history each time) while the mandi table is appended by pull day
because the feed is a rolling daily snapshot.

## Repository layout
| Path | Purpose |
|---|---|
| `mine_underused_sources.py` | the collector (138 lines, CLI) |
| `underused_sources.duckdb` | tracked output of the 2026-07-21 pull (~0.8 MB) |
| `requirements.txt` | `duckdb` |
| `.github/workflows/quality-gates.yml` | gitleaks secret scan of the working tree on push / PR |
| `graphify-out/` | tracked graphify knowledge-graph output for this repo's code (generated) |
| `HOW_TO_USE_WITH_AI.md`, `CLAUDE.md` | plain-language usage guide; agent guidance (source-fetching rules) |

## Data sources
| Source | What is taken | Access route | Licence / caveat |
|---|---|---|---|
| World Bank Indicators API — `https://api.worldbank.org/v2/country/{iso}/indicator/{code}?format=json` | 12 indicators × 7 countries, all years | public JSON API, no key | World Bank open data terms (CC BY-4.0); `source_last_updated` recorded per response |
| Agmarknet daily mandi prices on data.gov.in — resource `9ef84268-d588-465a-a308-a864a43d0070` at `https://api.data.gov.in/resource/<id>` | state, district, market, commodity, variety, grade, arrival date, min/max/modal price | public JSON API with data.gov.in's documented sample key | Government Open Data Licence – India. The sample key returns a small sample only — the tracked pull holds 4 rows; register for a personal key on data.gov.in for the full feed. |

## Configuration
None required. `DGI_KEY` in the script is data.gov.in's **public sample key** (documented on the site), not a
secret; replace it with your own key for full access. The output path `DB` is fixed to the script's directory.
No environment variables; no credentials are committed.

## Testing
```bash
python -m py_compile mine_underused_sources.py
python3 mine_underused_sources.py --help
```
There is no test suite; the CI workflow only runs gitleaks.

## Limitations and known issues
- The mandi pull with the sample key is effectively a sample (4 rows on 2026-07-21), not the full daily feed.
- `mandi_prices` keys on commodity *name*; the feed carries no commodity id, and `arrival_date` is stored as the API's `DD/MM/YYYY` string.
- A World Bank rate-limit or outage after the retries aborts the run; partial rows are not written because the table is created only after the loop.
- Re-running `mine_worldbank` always replaces the table, so history of previous pulls is not kept.

## Related repositories
- `india-mandi-arrivals` (same account) — fuller Agmarknet/UPAG collectors.

## Licence
No licence file yet — all rights reserved for the script; the data remain under their publishers' licences.
