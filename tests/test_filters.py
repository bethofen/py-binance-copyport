"""FetchFilter: sources, sort union, server params, client-side pre-filters."""
from __future__ import annotations

import pytest

from binance_copytrade import FetchFilter, RECOMMENDED_GROUPS, SOURCES
from binance_copytrade.testing import DAY, LEADERBOARD_ITEM, NOW, PID


def lb(**over):
    r = dict(LEADERBOARD_ITEM)
    r.update(over)
    return r


# ------------------------------------------------------------ validation / options
def test_options_lists_everything():
    o = FetchFilter.options()
    assert o["source"] == SOURCES and "COPIER_PNL" in o["sort"] and o["page_size"] == 30
    assert "min_days" in o["client_side"] and o["groups"] == RECOMMENDED_GROUPS


@pytest.mark.parametrize("bad", [dict(source="x"), dict(sort="FOO"), dict(time_range="1D"),
                                 dict(portfolio_type="Z"), dict(groups=("nope",)), dict(order="UP")])
def test_validate_rejects(bad):
    with pytest.raises(AssertionError):
        FetchFilter(**bad).validate()


def test_replace_rejects_unknown_option():
    with pytest.raises(TypeError, match="unknown filter option"):
        FetchFilter().replace(min_dayz=1)
    assert FetchFilter().replace(sort="AUM", pages=5).sorts == ["AUM"]


def test_sorts_parse_comma_list():
    assert FetchFilter(sort="roi, copier_pnl ,AUM").sorts == ["ROI", "COPIER_PNL", "AUM"]


# ------------------------------------------------------------ keep()
def test_keep_thresholds():
    f = FetchFilter(min_days=30, max_mdd=20, min_aum=1000, min_copiers=5, min_roi=10, min_win_rate=50,
                    min_sharpe=1, min_copier_pnl=0, exclude_full=True)
    assert f.keep(lb(currentCopyCount=10)) is True
    assert f.keep(lb(currentCopyCount=10, startTime=NOW - 5 * DAY)) is False        # too new
    assert f.keep(lb(currentCopyCount=10, mdd=25)) is False
    assert f.keep(lb(currentCopyCount=10, aum=100)) is False
    assert f.keep(lb(currentCopyCount=2)) is False                                   # min_copiers
    assert f.keep(lb(currentCopyCount=10, roi=1)) is False
    assert f.keep(lb(currentCopyCount=10, winRate=40)) is False
    assert f.keep(lb(currentCopyCount=10, sharpRatio=None)) is False                 # missing value fails min
    assert f.keep(lb(currentCopyCount=10, copierPnl=-1)) is False
    assert f.keep(lb(currentCopyCount=1000, maxCopyCount=1000)) is False             # full


def test_keep_badges_tags_ids():
    assert FetchFilter(badges=("LEGEND",)).keep(lb()) is True
    assert FetchFilter(badges=("EXPERT",)).keep(lb()) is False
    assert FetchFilter(require_api_key=False).keep(lb()) is False
    assert FetchFilter(require_tradfi=True).keep(lb()) is True
    assert FetchFilter(require_tradfi=True).keep(lb(tradFiTag=None)) is False
    assert FetchFilter(exclude_ids=(PID,)).keep(lb()) is False
    assert FetchFilter(include_ids=("other",)).keep(lb()) is False
    assert FetchFilter(max_days=10).keep(lb()) is False and FetchFilter(max_aum=10).keep(lb()) is False


def test_describe_only_set_values():
    d = FetchFilter(min_days=30, badges=("LEGEND",)).describe()
    assert "min_days=30" in d and "badges=" in d and "max_mdd" not in d


# ------------------------------------------------------------ service integration (fake client)
def test_fetch_leaderboard_all_sends_params(svc, fake_client):
    rows = svc.fetch_leaderboard(FetchFilter(sort="AUM", time_range="90D", portfolio_type="PUBLIC",
                                             hide_full=True, nickname="Joe", order="ASC", pages=1))
    body = fake_client.calls[0][2]
    assert body["dataType"] == "AUM" and body["timeRange"] == "90D" and body["portfolioType"] == "PUBLIC"
    assert body["hideFull"] is True and body["nickname"] == "Joe" and body["order"] == "ASC"
    assert "useAiRecommended" not in body
    assert rows[0]["_groups"] == ["AUM_90D"]


def test_fetch_leaderboard_ai_flag(svc, fake_client):
    svc.fetch_leaderboard(FetchFilter(source="ai", pages=1))
    assert fake_client.calls[0][2]["useAiRecommended"] is True


def test_fetch_leaderboard_sort_union_dedups(svc, fake_client):
    rows = svc.fetch_leaderboard(FetchFilter(sort="ROI,PNL", pages=2))
    assert len(rows) == 2                                   # same 2 leads from both sorts, de-duplicated
    assert rows[0]["_groups"] == ["ROI_30D", "PNL_30D"]
    assert len([c for c in fake_client.calls if c[1].endswith("/query-list")]) == 4


def test_fetch_leaderboard_recommended_groups(svc, fake_client):
    rows = svc.fetch_leaderboard(FetchFilter(source="recommended"))
    assert fake_client.calls[0][1].endswith("/recommend-lead-list")
    assert rows[0]["_groups"] == ["highestPnlLeads"]        # fake returns only that group
    rows = svc.fetch_leaderboard(FetchFilter(source="recommended", groups=("highestRoiLeads",)))
    assert rows == []


def test_fetch_leaderboard_prefilter_and_limit(svc):
    assert len(svc.fetch_leaderboard(FetchFilter(pages=2, min_roi=100))) == 1     # second lead has roi 5
    assert len(svc.fetch_leaderboard(FetchFilter(pages=2, limit=1))) == 1
    assert len(svc.fetch_leaderboard(pages=2, exclude_full=True)) == 1           # kw shortcut; first is 1000/1000


def test_collect_records_source_column(svc):
    df = svc.collect(FetchFilter(source="recommended"), full=False).df
    assert df.iloc[0]["source"] == "highestPnlLeads"
    df = svc.collect(pages=1, sort="COPIER_PNL", full=True).df
    assert df.iloc[0]["source"] == "COPIER_PNL_30D" and df.iloc[0]["roi365d"] == pytest.approx(1352.62)


def test_cli_collect_with_filters(tmp_path, capsys, fake_client):
    from binance_copytrade.cli import main
    data = str(tmp_path / "data")
    rc = main(["--data-dir", data, "collect", "--source", "ai", "--sort", "ROI,AUM", "--time-range", "90D",
                 "--pages", "1", "--min-days", "30", "--badges", "LEGEND", "--exclude-full", "--quick"], client=fake_client)
    assert rc == 0
    out = capsys.readouterr().out
    assert "_no data_" in out                                # only lead is FULL -> excluded
    assert main(["options"], client=fake_client) == 0 and "COPIER_PNL" in capsys.readouterr().out
