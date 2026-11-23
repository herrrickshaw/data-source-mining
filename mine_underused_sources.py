#!/usr/bin/env python3
"""Mine the under-used open data sources identified from the provenance log
(2026-07-21): World Bank API and Agmarknet daily mandi prices.

Both are fully open — no registration, no key beyond data.gov.in's public
sample key. Output goes to underused_sources.duckdb (one table per source).

Usage:
    python3 mine_underused_sources.py               # pull everything
    python3 mine_underused_sources.py --wb-only     # just World Bank
    python3 mine_underused_sources.py --mandi-only  # just Agmarknet
"""

import argparse
import json
import time
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

BASE = Path(__file__).resolve().parent
DB = BASE / "underused_sources.duckdb"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
HTTP_TIMEOUT_S = 30
HTTP_RETRIES = 3
RETRY_BACKOFF_S = 2          # sleep 2s, 4s, ... between retries
WB_PER_PAGE = 1000           # World Bank API page size
WB_PAUSE_S = 0.3             # pause between indicators
MANDI_PAGE_SIZE = 1000       # data.gov.in page size
MANDI_PAUSE_S = 0.5          # pause between pages

# data.gov.in public sample key (documented on data.gov.in itself)
DGI_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"
MANDI_RESOURCE = "9ef84268-d588-465a-a308-a864a43d0070"

# India indicator panel: macro + agri + energy + trade
WB_INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_current_usd",
    "NY.GDP.MKTP.KD.ZG": "gdp_growth_pct",
    "FP.CPI.TOTL.ZG": "cpi_inflation_pct",
    "NV.AGR.TOTL.ZS": "agri_value_added_pct_gdp",
    "AG.CON.FERT.ZS": "fertilizer_kg_per_ha",
    "NE.EXP.GNFS.ZS": "exports_pct_gdp",
    "NE.IMP.GNFS.ZS": "imports_pct_gdp",
    "EG.USE.COMM.FO.ZS": "fossil_fuel_energy_pct",
    "EG.FEC.RNEW.ZS": "renewable_energy_pct",
    "BX.KLT.DINV.WD.GD.ZS": "fdi_inflow_pct_gdp",
    "FR.INR.RINR": "real_interest_rate_pct",
    "PA.NUS.FCRF": "inr_usd_official_rate",
}
WB_COUNTRIES = ["IND", "USA", "CHN", "BRA", "DEU", "JPN", "KOR"]


def get_json(url: str, retries: int = HTTP_RETRIES) -> Any:
    """GET `url` and parse JSON, retrying with linear back-off; re-raises the last error."""
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                return json.load(r)
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(RETRY_BACKOFF_S * (i + 1))


def mine_worldbank(con: duckdb.DuckDBPyConnection) -> int:
    """Pull WB_INDICATORS for WB_COUNTRIES (all years) into worldbank_indicators; returns row count."""
    rows: list[tuple] = []
    for iso in WB_COUNTRIES:
        for code, name in WB_INDICATORS.items():
            page = 1
            while True:
                url = (f"https://api.worldbank.org/v2/country/{iso}/indicator/"
                       f"{code}?format=json&per_page={WB_PER_PAGE}&page={page}")
                d = get_json(url)
                if not d or len(d) < 2 or not d[1]:
                    break
                for obs in d[1]:
                    if obs.get("value") is None:
                        continue
                    rows.append((iso, code, name, int(obs["date"]),
                                 float(obs["value"]), d[0].get("lastupdated")))
                if page >= d[0].get("pages", 1):
                    break
                page += 1
            time.sleep(WB_PAUSE_S)
        print(f"  worldbank: {iso} done ({len(rows)} rows cumulative)")
    con.execute("""CREATE OR REPLACE TABLE worldbank_indicators (
        country TEXT, indicator_code TEXT, indicator TEXT,
        year INTEGER, value DOUBLE, source_last_updated TEXT)""")
    con.executemany("INSERT INTO worldbank_indicators VALUES (?,?,?,?,?,?)", rows)
    return len(rows)


def mine_mandi(con: duckdb.DuckDBPyConnection) -> int:
    """Pull today's Agmarknet mandi prices (data.gov.in) into mandi_prices; returns rows pulled."""
    rows: list[tuple] = []
    offset, limit = 0, MANDI_PAGE_SIZE
    while True:
        url = (f"https://api.data.gov.in/resource/{MANDI_RESOURCE}"
               f"?api-key={DGI_KEY}&format=json&limit={limit}&offset={offset}")
        d = get_json(url)
        recs = d.get("records", [])
        for r in recs:
            rows.append((
                r.get("state"), r.get("district"), r.get("market"),
                r.get("commodity"), r.get("variety"), r.get("grade"),
                r.get("arrival_date"),
                r.get("min_price"), r.get("max_price"), r.get("modal_price"),
                date.today().isoformat(),
            ))
        if len(recs) < limit:
            break
        offset += limit
        time.sleep(MANDI_PAUSE_S)
    con.execute("""CREATE TABLE IF NOT EXISTS mandi_prices (
        state TEXT, district TEXT, market TEXT, commodity TEXT,
        variety TEXT, grade TEXT, arrival_date TEXT,
        min_price DOUBLE, max_price DOUBLE, modal_price DOUBLE,
        pulled_on TEXT)""")
    # idempotent per pull-day: replace today's rows
    con.execute("DELETE FROM mandi_prices WHERE pulled_on = ?", [date.today().isoformat()])
    con.executemany("INSERT INTO mandi_prices VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def main() -> None:
    """CLI entry: pull one or both sources into underused_sources.duckdb and print table sizes."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--wb-only", action="store_true")
    ap.add_argument("--mandi-only", action="store_true")
    args = ap.parse_args()
    con = duckdb.connect(str(DB))
    if not args.mandi_only:
        n = mine_worldbank(con)
        print(f"worldbank_indicators: {n} rows")
    if not args.wb_only:
        n = mine_mandi(con)
        print(f"mandi_prices: {n} rows pulled today")
    for (t,) in con.execute("SHOW TABLES").fetchall():
        c = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  table {t}: {c:,} rows")
    con.close()


if __name__ == "__main__":
    main()
