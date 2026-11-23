import io
import json
import sys
import urllib.parse
from datetime import date

import duckdb
import pytest

import mine_underused_sources as m


@pytest.fixture
def sleeps(monkeypatch):
    s = []
    monkeypatch.setattr(m.time, "sleep", lambda x: s.append(x))
    return s


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


class FakeCon:
    """Records statements instead of executing them (duckdb refuses executemany with no rows)."""

    def __init__(self):
        self.stmts, self.inserted = [], None

    def execute(self, sql, params=None):
        self.stmts.append((" ".join(sql.split()), params))

    def executemany(self, sql, rows):
        self.stmts.append((" ".join(sql.split()), "MANY"))
        self.inserted = rows


class Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


# ---------------- get_json ----------------
def test_get_json_success_sets_user_agent_and_timeout(monkeypatch):
    seen = {}

    def fake(req, timeout):
        seen.update(url=req.full_url, ua=req.get_header("User-agent"), timeout=timeout)
        return Resp(b'{"a": 1}')

    monkeypatch.setattr(m.urllib.request, "urlopen", fake)
    assert m.get_json("http://x/y") == {"a": 1}
    assert seen == {"url": "http://x/y", "ua": m.UA["User-Agent"], "timeout": 30}


def test_get_json_retries_with_linear_backoff(monkeypatch, sleeps):
    seq = iter([OSError("a"), ValueError("b"), Resp(b"[1]")])

    def fake(req, timeout):
        v = next(seq)
        if isinstance(v, Exception):
            raise v
        return v

    monkeypatch.setattr(m.urllib.request, "urlopen", fake)
    assert m.get_json("http://u") == [1]
    assert sleeps == [2, 4]


def test_get_json_raises_after_last_attempt(monkeypatch, sleeps):
    n = []

    def boom(req, timeout):
        n.append(1)
        raise OSError("down")

    monkeypatch.setattr(m.urllib.request, "urlopen", boom)
    with pytest.raises(OSError, match="down"):
        m.get_json("http://u")
    assert len(n) == 3 and sleeps == [2, 4]
    n.clear()
    sleeps.clear()
    with pytest.raises(OSError):
        m.get_json("http://u", retries=1)
    assert len(n) == 1 and sleeps == []


# ---------------- world bank ----------------
def wb_pages(pages_by_url):
    """get_json fake: url-> list of responses (consumed in order)."""
    calls = []

    def g(url):
        calls.append(url)
        key = next(k for k in pages_by_url if k in url)
        return pages_by_url[key].pop(0)

    g.calls = calls
    return g


def test_mine_worldbank_row_building(monkeypatch, con, sleeps, capsys):
    monkeypatch.setattr(m, "WB_COUNTRIES", ["IND", "USA"])
    monkeypatch.setattr(m, "WB_INDICATORS", {"NY.GDP.MKTP.CD": "gdp_current_usd", "FP.CPI.TOTL.ZG": "cpi_inflation_pct"})
    meta = {"pages": 1, "lastupdated": "2026-01-01"}
    data = {
        "IND/indicator/NY.GDP.MKTP.CD": [[meta, [{"date": "2020", "value": 1.5}, {"date": "2021", "value": None}, {"date": "2022", "value": 2}]]],
        "IND/indicator/FP.CPI.TOTL.ZG": [[meta, [{"date": "2019", "value": "4.5"}]]],
        "USA/indicator/NY.GDP.MKTP.CD": [[meta, []]],
        "USA/indicator/FP.CPI.TOTL.ZG": [None],
    }
    g = wb_pages(data)
    monkeypatch.setattr(m, "get_json", g)
    assert m.mine_worldbank(con) == 3
    got = con.execute("SELECT * FROM worldbank_indicators ORDER BY country, indicator_code, year").fetchall()
    assert got == [("IND", "FP.CPI.TOTL.ZG", "cpi_inflation_pct", 2019, 4.5, "2026-01-01"),
                   ("IND", "NY.GDP.MKTP.CD", "gdp_current_usd", 2020, 1.5, "2026-01-01"),
                   ("IND", "NY.GDP.MKTP.CD", "gdp_current_usd", 2022, 2.0, "2026-01-01")]
    assert g.calls[0] == "https://api.worldbank.org/v2/country/IND/indicator/NY.GDP.MKTP.CD?format=json&per_page=1000&page=1"
    assert sleeps == [0.3] * 4                                          # one polite pause per (country, indicator)
    out = capsys.readouterr().out
    assert "worldbank: IND done (3 rows cumulative)" in out and "worldbank: USA done (3 rows cumulative)" in out


@pytest.mark.parametrize("payload", [None, [], [{"pages": 1}], [{"pages": 1}, None], [{"pages": 1}, []]])
def test_mine_worldbank_stops_on_empty_or_short_payloads(monkeypatch, sleeps, payload):
    monkeypatch.setattr(m, "WB_COUNTRIES", ["IND"])
    monkeypatch.setattr(m, "WB_INDICATORS", {"X": "x"})
    calls = []
    monkeypatch.setattr(m, "get_json", lambda url: (calls.append(url), payload)[1])
    con = FakeCon()
    assert m.mine_worldbank(con) == 0
    assert con.inserted == [] and len(calls) == 1                 # no page 2 is requested


@pytest.mark.xfail(strict=True, raises=duckdb.InvalidInputException,
                   reason="mine_underused_sources.py mine_worldbank: DuckDB's executemany() rejects an empty row list, so a "
                          "pull that returns no observations raises AFTER `CREATE OR REPLACE TABLE` has already emptied "
                          "worldbank_indicators (verified on duckdb 0.10, 1.1 and 2.0)")
def test_mine_worldbank_with_no_rows_keeps_previous_table(monkeypatch, con, sleeps):
    monkeypatch.setattr(m, "WB_COUNTRIES", ["IND"])
    monkeypatch.setattr(m, "WB_INDICATORS", {"X": "x"})
    monkeypatch.setattr(m, "get_json", lambda url: None)
    con.execute("CREATE TABLE worldbank_indicators (country TEXT, indicator_code TEXT, indicator TEXT, year INTEGER, value DOUBLE, source_last_updated TEXT)")
    con.execute("INSERT INTO worldbank_indicators VALUES ('IND','X','x',2001,1,'u')")
    assert m.mine_worldbank(con) == 0
    assert con.execute("SELECT COUNT(*) FROM worldbank_indicators").fetchone()[0] == 1


def test_mine_worldbank_follows_pages(monkeypatch, con, sleeps):
    monkeypatch.setattr(m, "WB_COUNTRIES", ["IND"])
    monkeypatch.setattr(m, "WB_INDICATORS", {"X": "x"})
    p1 = [{"pages": 3, "lastupdated": "u"}, [{"date": "2001", "value": 1}]]
    p2 = [{"pages": 3, "lastupdated": "u"}, [{"date": "2002", "value": 2}]]
    p3 = [{"pages": 3, "lastupdated": "u"}, [{"date": "2003", "value": 3}]]
    pages = [p1, p2, p3]
    urls = []
    monkeypatch.setattr(m, "get_json", lambda url: (urls.append(url), pages.pop(0))[1])
    assert m.mine_worldbank(con) == 3
    assert [u.rsplit("page=", 1)[1] for u in urls] == ["1", "2", "3"] and not pages


def test_mine_worldbank_missing_pages_key_means_single_page(monkeypatch, con, sleeps):
    monkeypatch.setattr(m, "WB_COUNTRIES", ["IND"])
    monkeypatch.setattr(m, "WB_INDICATORS", {"X": "x"})
    calls = []
    monkeypatch.setattr(m, "get_json", lambda url: (calls.append(url), [{}, [{"date": "2001", "value": 1}]])[1])
    assert m.mine_worldbank(con) == 1 and len(calls) == 1
    assert con.execute("SELECT source_last_updated FROM worldbank_indicators").fetchone() == (None,)


def test_mine_worldbank_replaces_table_each_run(monkeypatch, con, sleeps):
    monkeypatch.setattr(m, "WB_COUNTRIES", ["IND"])
    monkeypatch.setattr(m, "WB_INDICATORS", {"X": "x"})
    monkeypatch.setattr(m, "get_json", lambda url: [{"pages": 1}, [{"date": "2001", "value": 1}]])
    m.mine_worldbank(con)
    m.mine_worldbank(con)
    assert con.execute("SELECT COUNT(*) FROM worldbank_indicators").fetchone()[0] == 1


def test_world_bank_panel_constants():
    assert len(m.WB_INDICATORS) == 12 and "IND" in m.WB_COUNTRIES and len(m.WB_COUNTRIES) == 7
    assert m.WB_INDICATORS["NY.GDP.MKTP.CD"] == "gdp_current_usd"


# ---------------- mandi ----------------
def rec(i, **kw):
    base = {"state": "KA", "district": "D", "market": f"M{i}", "commodity": "Rice", "variety": "V", "grade": "FAQ",
            "arrival_date": "01/01/2026", "min_price": "10", "max_price": "20", "modal_price": "15"}
    base.update(kw)
    return base


def test_mine_mandi_single_page(monkeypatch, con, sleeps):
    urls = []
    monkeypatch.setattr(m, "get_json", lambda url: (urls.append(url), {"records": [rec(1), rec(2, grade=None)]})[1])
    assert m.mine_mandi(con) == 2
    q = urllib.parse.urlparse(urls[0])
    qs = urllib.parse.parse_qs(q.query)
    assert q.path == f"/resource/{m.MANDI_RESOURCE}" and qs == {"api-key": [m.DGI_KEY], "format": ["json"], "limit": ["1000"], "offset": ["0"]}
    rows = con.execute("SELECT * FROM mandi_prices ORDER BY market").fetchall()
    assert rows[0] == ("KA", "D", "M1", "Rice", "V", "FAQ", "01/01/2026", 10.0, 20.0, 15.0, date.today().isoformat())
    assert rows[1][5] is None and sleeps == []


def test_mine_mandi_paginates_until_short_page(monkeypatch, con, sleeps):
    full = {"records": [rec(i) for i in range(1000)]}
    last = {"records": [rec(i) for i in range(1000, 1003)]}
    pages = [full, full, last]
    urls = []
    monkeypatch.setattr(m, "get_json", lambda url: (urls.append(url), pages.pop(0))[1])
    assert m.mine_mandi(con) == 2003
    assert [u.split("offset=")[1] for u in urls] == ["0", "1000", "2000"]
    assert sleeps == [0.5, 0.5]


def test_mine_mandi_exact_multiple_needs_one_more_empty_request(monkeypatch, con, sleeps):
    pages = [{"records": [rec(i) for i in range(1000)]}, {"records": []}]
    urls = []
    monkeypatch.setattr(m, "get_json", lambda url: (urls.append(url), pages.pop(0))[1])
    assert m.mine_mandi(con) == 1000 and len(urls) == 2


def test_mine_mandi_missing_records_key(monkeypatch):
    monkeypatch.setattr(m, "get_json", lambda url: {})
    con = FakeCon()
    assert m.mine_mandi(con) == 0 and con.inserted == []
    assert any(sql.startswith("DELETE FROM mandi_prices WHERE pulled_on = ?") for sql, _ in con.stmts)


@pytest.mark.xfail(strict=True, raises=duckdb.InvalidInputException,
                   reason="mine_underused_sources.py mine_mandi: an empty pull (no records) makes executemany() raise "
                          "after today's rows were already DELETEd, so a failed/empty pull wipes the day's earlier data")
def test_mine_mandi_empty_pull_does_not_lose_todays_rows(monkeypatch, con):
    monkeypatch.setattr(m, "get_json", lambda url: {"records": [rec(1)]})
    m.mine_mandi(con)
    monkeypatch.setattr(m, "get_json", lambda url: {"records": []})
    m.mine_mandi(con)
    assert con.execute("SELECT COUNT(*) FROM mandi_prices").fetchone()[0] == 1


def test_mine_mandi_replaces_only_todays_rows(monkeypatch, con):
    monkeypatch.setattr(m, "get_json", lambda url: {"records": [rec(1)]})
    m.mine_mandi(con)
    con.execute("INSERT INTO mandi_prices VALUES ('X','X','old','X','X','X','d',1,2,3,'2000-01-01')")
    m.mine_mandi(con)
    got = con.execute("SELECT market, pulled_on FROM mandi_prices ORDER BY pulled_on").fetchall()
    assert got == [("old", "2000-01-01"), ("M1", date.today().isoformat())]          # today's pull is replaced, history kept


def test_mandi_constants():
    assert m.MANDI_RESOURCE == "9ef84268-d588-465a-a308-a864a43d0070" and m.DGI_KEY.startswith("579b4")


# ---------------- main ----------------
def run(monkeypatch, tmp_path, *argv, wb=7, mandi=3):
    calls = []
    monkeypatch.setattr(m, "DB", tmp_path / "x.duckdb")
    monkeypatch.setattr(m, "mine_worldbank", lambda con: (calls.append("wb"), con.execute("CREATE OR REPLACE TABLE worldbank_indicators(a INT)"), wb)[2])
    monkeypatch.setattr(m, "mine_mandi", lambda con: (calls.append("mandi"), con.execute("CREATE OR REPLACE TABLE mandi_prices(a INT)"), mandi)[2])
    monkeypatch.setattr(sys, "argv", ["mine.py", *argv])
    m.main()
    return calls


def test_main_runs_both_and_reports_tables(monkeypatch, tmp_path, capsys):
    assert run(monkeypatch, tmp_path) == ["wb", "mandi"]
    out = capsys.readouterr().out
    assert "worldbank_indicators: 7 rows" in out and "mandi_prices: 3 rows pulled today" in out
    assert "table worldbank_indicators: 0 rows" in out and "table mandi_prices: 0 rows" in out


def test_main_wb_only(monkeypatch, tmp_path, capsys):
    assert run(monkeypatch, tmp_path, "--wb-only") == ["wb"]
    out = capsys.readouterr().out
    assert "mandi_prices:" not in out.replace("table mandi_prices", "")


def test_main_mandi_only(monkeypatch, tmp_path, capsys):
    assert run(monkeypatch, tmp_path, "--mandi-only") == ["mandi"]
    assert "worldbank_indicators: " not in capsys.readouterr().out.replace("table worldbank_indicators", "")


def test_main_with_both_flags_does_nothing(monkeypatch, tmp_path):
    assert run(monkeypatch, tmp_path, "--wb-only", "--mandi-only") == []


def test_main_creates_database_file(monkeypatch, tmp_path):
    run(monkeypatch, tmp_path)
    assert (tmp_path / "x.duckdb").exists()


def test_main_row_counts_use_thousands_separator(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(m, "DB", tmp_path / "y.duckdb")
    monkeypatch.setattr(m, "mine_worldbank", lambda con: (con.execute("CREATE TABLE t AS SELECT * FROM range(1500)"), 1500)[1])
    monkeypatch.setattr(sys, "argv", ["mine.py", "--wb-only"])
    m.main()
    assert "table t: 1,500 rows" in capsys.readouterr().out
