"""Tests for CopyTradeService: fetch/flatten, explicit save/load (csv & json), history, diff, rebuild, CLI."""
from __future__ import annotations

import json
import pandas as pd
import pytest

from binance_copytrade import LeadSet, flatten_lead
from binance_copytrade.testing import PID


# ------------------------------------------------------------- fetching
def test_fetch_lead_hits_every_endpoint(svc, fake_client):
    raw = svc.fetch_lead(PID)
    paths = [p for _, p, _ in fake_client.calls]
    assert paths.count("/friendly/future/copy-trade/lead-portfolio/detail") == 1
    assert paths.count("/public/future/copy-trade/lead-portfolio/performance") == 5  # 7D..365D
    assert set(raw["performance"]) == {"7D", "30D", "90D", "180D", "365D"}
    assert raw["chart"]["timeRange"] == "365D"
    assert len(raw["positionHistory"]) == 3
    assert raw["transferHistory"] and raw["copyTraders"]


def test_collect_full_returns_leadset_and_writes_nothing(svc, tmp_path):
    ls = svc.collect(pages=2)
    assert isinstance(ls, LeadSet) and len(ls) == 2 and set(ls.raws) == {PID, "2"}
    df = ls.df
    assert list(df.columns[:5]) == ["nickname", "portfolioId", "roi7d", "roi30d", "roi90d"]
    row = df.iloc[0]
    assert row.portfolioId == PID
    assert row.roi7d == pytest.approx(9.70) and row.roi30d == pytest.approx(199.83)
    assert row.roi365d == pytest.approx(1352.62)
    assert row.minMargin == 300 and row.currentCopiers == 1000
    assert not (tmp_path / "data").exists() or not any((tmp_path / "data").iterdir())   # nothing saved


def test_collect_quick_uses_only_leaderboard(svc, fake_client):
    ls = svc.collect(pages=1, full=False)
    assert len(ls) == 1 and ls.raws == {}
    assert ls.df.iloc[0].roi30d == pytest.approx(199.83)
    assert pd.isna(ls.df.iloc[0].roi7d)
    assert all(p.endswith("/query-list") for _, p, _ in fake_client.calls)


# ------------------------------------------------------------- flatten
def test_flatten_derived_metrics(svc):
    row = flatten_lead(svc.fetch_lead(PID))
    assert row["daysTrading"] in (93, 94, 95)
    assert row["histPositions"] == 3
    assert row["histAvgLeverage"] == pytest.approx(4.0)
    assert row["histWinRate"] == pytest.approx(66.67, abs=0.01)
    assert row["histPayoff"] == pytest.approx((45724.4 + 4911.4) / 2 / 14446.4, abs=0.01)
    assert row["leadWithdrawals"] == 1252210.0 and row["leadDeposits"] == 30000.0
    assert row["topAssets"].startswith("XAU:37.57%")
    assert row["chartPosDays"] == pytest.approx(100 * 6 / 9, abs=0.1)
    assert row["chartWorstDay"] == -2
    assert row["impliedStartEquity"] == pytest.approx(1643343.7 / 1352.62 * 100, rel=1e-3)
    assert row["leverageTag"] == "MEDIUM_LEVERAGE"


def test_flatten_handles_missing_sections():
    row = flatten_lead({"portfolioId": "x", "detail": {}, "performance": {}})
    assert row["portfolioId"] == "x" and row["roi30d"] is None and row["minMargin"] is None
    assert "histPositions" not in row


# ------------------------------------------------------------- explicit storage
@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_save_load_roundtrip(svc, tmp_path, fmt):
    ls = svc.collect(pages=2)
    d = svc.save(ls, fmt=fmt, date="2026-09-05")
    assert d == tmp_path / "data" / "daily" / "2026-09-05"
    files = {p.name for p in d.iterdir()}
    if fmt == "csv":
        assert {"leads.csv", "chart.csv", "history.csv", "transfers.csv", "copiers.csv", "meta.json"} <= files
        assert "positions.csv" not in files                     # fake returns no open positions
    else:
        assert {"leads.json.gz", "raw.json.gz", "meta.json"} <= files
    meta = json.loads((d / "meta.json").read_text(encoding="utf-8"))
    assert meta["format"] == fmt and meta["counts"]["leads"] == 2 and meta["stage"] == "collect"

    back = svc.load("2026-09-05")
    # ids must come back as text (object on pandas 2, the str dtype on pandas 3), never as numbers
    assert back.date == "2026-09-05" and len(back) == 2 and pd.api.types.is_string_dtype(back.df["portfolioId"])
    assert all(isinstance(v, str) for v in back.df["portfolioId"])
    assert back.df.iloc[0]["snapshotDate"] == "2026-09-05" and back.df.iloc[0]["roi30d"] == ls.df.iloc[0]["roi30d"]
    raw = back.raw(PID)
    assert len(raw["chart"]["data"]) == 10 and raw["chart"]["data"][0]["value"] == 0
    assert len(raw["positionHistory"]) == 3 and raw["positionHistory"][0]["symbol"] == "ZECUSDT"
    assert len(raw["transferHistory"]) == 3 and len(raw["copyTraders"]) == 2
    if fmt == "json":
        assert raw["detail"]["nickname"] == "熬鹰资本"                # json keeps everything
    else:
        assert raw["detail"] == {}                                # csv keeps tables only


def test_csv_much_smaller_than_pretty_json(svc, tmp_path):
    ls = svc.collect(pages=2)
    d = svc.save(ls, fmt="csv", date="2026-09-05")
    csv_bytes = sum(p.stat().st_size for p in d.iterdir())
    pretty = len(json.dumps(ls.raws, ensure_ascii=False, indent=1).encode())
    assert csv_bytes < pretty / 2


def test_save_bare_dataframe(svc):
    df = svc.collect(pages=1, full=False).df
    d = svc.save(df, fmt="csv", date="2026-09-06")
    assert (d / "leads.csv").exists() and not (d / "chart.csv").exists()
    assert svc.days() == ["2026-09-06"]


def test_history_and_diff_across_days(svc):
    ls = svc.collect(pages=1)
    svc.save(ls, fmt="csv", date="2026-09-04")
    ls2 = LeadSet(ls.df.copy(), ls.raws, "2026-09-05")
    ls2.df["roi30d"] += 10
    ls2.df["currentCopiers"] -= 3
    svc.save(ls2, fmt="json")
    assert svc.days() == ["2026-09-04", "2026-09-05"]
    assert svc.latest().iloc[0]["snapshotDate"] == "2026-09-05"
    h = svc.history(PID)
    assert list(h["snapshotDate"]) == ["2026-09-04", "2026-09-05"]
    d = svc.diff()
    assert d.attrs["dates"] == ("2026-09-04", "2026-09-05")
    assert d.iloc[0]["roi30d_delta"] == pytest.approx(10) and d.iloc[0]["currentCopiers_delta"] == -3
    assert not d.iloc[0]["new_entry"]


def test_diff_needs_two_days(svc):
    assert svc.diff().empty and svc.load() is None and svc.latest().empty


def test_rebuild_json_reflattens_csv_keeps_rows(svc):
    ls = svc.collect(pages=2)
    svc.save(ls, fmt="json", date="2026-09-05")
    rb = svc.rebuild("2026-09-05")
    assert len(rb) == 2 and rb.meta.get("rebuilt") and rb.df.iloc[0]["minMargin"] == 300
    assert "score" not in rb.df                                     # data layer never scores
    svc.save(ls, fmt="csv", date="2026-09-06")
    rb2 = svc.rebuild("2026-09-06")                                 # csv has no detail -> rows as saved
    assert len(rb2) == 2 and not rb2.meta.get("rebuilt")


# ------------------------------------------------------------- CLI smoke
def test_cli_prints_without_saving_unless_asked(tmp_path, capsys, fake_client):
    from binance_copytrade.cli import main
    data = str(tmp_path / "data")
    assert main(["--data-dir", data, "collect", "--pages", "1", "--top", "5"], client=fake_client) == 0
    out = capsys.readouterr().out
    assert out.startswith("# Leads") and "not saved" in out and "score" not in out
    assert not (tmp_path / "data" / "daily").exists()
    csv_out = tmp_path / "flat.csv"
    assert main(["--data-dir", data, "collect", "--pages", "1", "--save", "csv", "--date", "2026-09-05",
                 "--out", str(csv_out)], client=fake_client) == 0
    assert "saved:" in capsys.readouterr().out and (tmp_path / "data" / "daily" / "2026-09-05" / "leads.csv").exists()
    assert csv_out.exists()
    assert main(["--data-dir", data, "days"], client=fake_client) == 0 and "2026-09-05" in capsys.readouterr().out
    assert main(["--data-dir", data, "history", PID], client=fake_client) == 0
    assert main(["--data-dir", data, "diff"], client=fake_client) == 0
    assert main(["--data-dir", data, "rebuild", "--top", "3"], client=fake_client) == 0
    assert main(["--data-dir", data, "options"], client=fake_client) == 0
    capsys.readouterr()
    raw_out = tmp_path / "raw.json"
    assert main(["--data-dir", data, "lead", PID, "--save", "json", "--date", "2026-09-06", "--json", str(raw_out)],
                client=fake_client) == 0
    assert (tmp_path / "data" / "daily" / "2026-09-06" / "raw.json.gz").exists()
    assert json.loads(raw_out.read_text(encoding="utf-8"))[0]["portfolioId"] == PID
