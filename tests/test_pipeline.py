"""Two-stage pipeline (batch -> filter -> enrich) and explicit batch save/load."""
from __future__ import annotations

import numpy as np
import pytest

from binance_copytrade import FetchFilter
from binance_copytrade.testing import DAY, N_LEADS, NOW, POSITION_HISTORY, RANGES, BatchFakeClient


def n_query(client, tr=None):
    return len([c for c in client.calls if c[1].endswith("/query-list") and (tr is None or c[2]["timeRange"] == tr)])


# ------------------------------------------------------------------ stage 1
def test_batch_collect_merges_all_ranges_and_saves_nothing(bsvc, tmp_path):
    merged = bsvc.batch_collect(pages=5, sort="AUM")
    assert len(merged) == N_LEADS
    m = merged["L3"]
    assert set(m["performance"]) == set(RANGES)
    assert m["performance"]["365D"]["roi"] == pytest.approx(10 * 365 / 30)
    assert len(m["chart365"]) == 366 and m["chart_range"] == "365D"
    assert n_query(bsvc.client) == 2 * len(RANGES)                # 2 pages x 5 ranges, stopped at total
    assert not (tmp_path / "data").exists()


def test_batch_collect_rejects_range_dependent_sort(bsvc):
    with pytest.raises(AssertionError):
        bsvc.batch_collect(pages=1, sort="ROI")


def test_batch_ai_source_and_hide_full_forwarded(bsvc):
    bsvc.batch_collect(pages=1, source="ai", hide_full=True, ranges=("30D",))
    body = bsvc.client.calls[0][2]
    assert body["useAiRecommended"] is True and body["hideFull"] is True and body["dataType"] == "AUM"


def test_batch_early_stop(bsvc):
    # aum of lead i = 100000 - i*1000 ; page 1 ends at L29 (71,000) -> stop_when aum<80000 stops after page 1
    merged = bsvc.batch_collect(pages=5, stop_when={"aum": 80000}, ranges=("30D", "90D"))
    assert n_query(bsvc.client) == 2 and len(merged) == 30


def test_batch_checkpoint_resume(bsvc):
    date = "2026-09-05"
    bsvc.batch_collect(pages=5, ranges=("7D", "30D"), date=date, checkpoint=True, checkpoint_every=1)
    assert bsvc.store.load_batch(date, partial=True) is None            # cleaned after completion
    # simulate a crash after 7D/30D: write a partial file, then resume for all ranges
    merged = bsvc.batch_collect(pages=5, ranges=("7D", "30D"), date=date)
    bsvc.store.save_batch({"_merged": merged, "_done": ["7D:1", "7D:2", "7D:end", "30D:1", "30D:2", "30D:end"]},
                          date, partial=True)
    bsvc.client.calls.clear()
    merged2 = bsvc.batch_collect(pages=5, ranges=RANGES, date=date, checkpoint=True)
    assert {c[2]["timeRange"] for c in bsvc.client.calls if c[1].endswith("/query-list")} == {"90D", "180D", "365D"}
    assert set(merged2["L0"]["performance"]) == set(RANGES)


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_batch_save_load_roundtrip(bsvc, fmt):
    merged = bsvc.batch_collect(pages=5)
    p = bsvc.save_batch(merged, fmt=fmt, date="2026-09-05")
    assert p.exists() and bsvc.store.list_batches() == ["2026-09-05"]
    back = bsvc.store.load_batch("2026-09-05")
    assert set(back) == set(merged)
    m = back["L3"]
    assert m["performance"]["365D"]["roi"] == pytest.approx(10 * 365 / 30)
    assert m["performance"]["7D"]["roi"] == pytest.approx(10 * 7 / 30)
    assert len(m["chart365"]) == 366 and m["leaderboard"]["nickname"] == "lead3"
    assert m["leaderboard"]["currentCopyCount"] == 3 and m["leaderboard"]["startTime"] == NOW - 35 * DAY
    # frames from original and reloaded batch agree on the core metrics
    a = bsvc.batch_frame(merged).set_index("portfolioId")
    b = bsvc.batch_frame(back).set_index("portfolioId")
    for c in ("roi30d", "roi365d", "mdd", "aum", "daysTrading", "chartPosDays"):
        assert a[c].astype(float).round(2).equals(b[c].astype(float).round(2)), c


def test_batch_frame_and_leadset(bsvc):
    merged = bsvc.batch_collect(pages=5)
    ls = bsvc.batch_leadset(merged)
    assert len(ls) == N_LEADS and len(ls.raws) == N_LEADS
    r = ls.df.set_index("portfolioId").loc["L3"]
    assert r["roi365d"] == pytest.approx(10 * 365 / 30) and r["daysTrading"] == 35
    assert r["minMargin"] is None or np.isnan(r["minMargin"])
    assert r["chartDays"] > 30                                  # chart filtered to after start


# ------------------------------------------------------------------ stage 2
def test_pipeline_filters_then_enriches_only_candidates(bsvc, tmp_path):
    f = FetchFilter(min_days=30, max_mdd=30, min_copier_pnl=0)
    ls = bsvc.pipeline(f, pages=5, history_pages=1)
    merged = ls.batch
    expected = [pid for pid, m in merged.items() if f.keep(bsvc._batch_keep_row(m))]
    assert sorted(ls.ids) == sorted(expected) and 0 < len(expected) < N_LEADS
    assert len([c for c in bsvc.client.calls if c[1].endswith("/detail")]) == len(expected)
    assert len([c for c in bsvc.client.calls if c[1].endswith("/position-history")]) == len(expected)
    assert not any(c[1].endswith(("/transfer-history", "/copy-traders")) for c in bsvc.client.calls)
    row = ls.df.iloc[0]
    assert row["minMargin"] == 300 and row["histPositions"] == len(POSITION_HISTORY) and row["source"] == "batch_AUM"
    assert "score" not in ls.df and ls.meta["candidates"] == len(expected)
    assert not (tmp_path / "data").exists()                     # nothing written


def test_enrich_resume_from_saved_leadset(bsvc):
    merged = bsvc.batch_collect(pages=5)
    first = bsvc.enrich(merged, ["L1", "L2"])
    bsvc.save(first, fmt="json", date="2026-09-05")
    n = len([c for c in bsvc.client.calls if c[1].endswith("/detail")])
    again = bsvc.enrich(merged, ["L1", "L2", "L3"], resume_from=bsvc.load("2026-09-05"))
    assert len(again) == 3
    assert len([c for c in bsvc.client.calls if c[1].endswith("/detail")]) == n + 1


def test_enrich_optional_sections(bsvc):
    merged = bsvc.batch_collect(pages=5)
    ls = bsvc.enrich(merged, ["L1"], transfers=True, copiers=True)
    paths = {c[1].rsplit("/", 1)[-1] for c in bsvc.client.calls}
    assert {"transfer-history", "copy-traders"} <= paths
    assert ls.df.iloc[0]["leadWithdrawals"] > 0 and ls.df.iloc[0]["topCopiersAvgRoi"] > 0


def test_cli_batch_enrich_pipeline(tmp_path, capsys):
    from binance_copytrade.cli import main as cli
    data = str(tmp_path / "data")

    class m:  # keep the original call shape: m._cli([...])
        @staticmethod
        def _cli(argv):
            return cli(argv, client=BatchFakeClient())

    assert m._cli(["--data-dir", data, "batch", "--pages", "5"]) == 0
    out = capsys.readouterr().out
    assert "batch: 45 leads" in out and "not saved" in out
    assert m._cli(["--data-dir", data, "enrich", "--min-days", "30"]) == 1          # no saved batch yet
    capsys.readouterr()
    assert m._cli(["--data-dir", data, "batch", "--pages", "5", "--save", "csv", "--date", "2026-09-05",
                   "--stop-when", "aum<50000"]) == 0
    assert "saved:" in capsys.readouterr().out
    assert m._cli(["--data-dir", data, "enrich", "--min-days", "30", "--max-mdd", "30", "--top", "5",
                   "--save", "csv", "--date", "2026-09-05"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("# Leads") and "saved:" in out
    assert (tmp_path / "data" / "daily" / "2026-09-05" / "history.csv").exists()
    assert m._cli(["--data-dir", data, "pipeline", "--pages", "5", "--min-days", "300", "--top", "5",
                   "--source", "ai", "--save-batch", "json", "--date", "2026-09-06"]) == 0
    assert (tmp_path / "data" / "batch" / "2026-09-06.json.gz").exists()


def test_enrich_keeps_empty_sections_as_fetched(bsvc):
    """[] = fetched and empty; a missing key = not fetched. Analysis relies on the difference."""
    merged = bsvc.batch_collect(pages=5)
    ls = bsvc.enrich(merged, ["L1"], copiers=True)
    bsvc.client.history = []
    ls2 = bsvc.enrich(merged, ["L2"])
    assert ls.raw("L1")["copyTraders"] and "transferHistory" not in ls.raw("L1")
    assert ls2.raw("L2")["positionHistory"] == []
