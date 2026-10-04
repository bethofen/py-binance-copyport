"""Live integration tests against binance.com (real network). Skipped unless RUN_LIVE=1.

    RUN_LIVE=1 pytest tests/test_live.py -v
"""
from __future__ import annotations

import os

import pytest

from binance_copytrade import BinanceCopyClient, CopyTradeService, ServiceConfig, TIME_RANGES, flatten_lead

pytestmark = pytest.mark.skipif(os.environ.get("RUN_LIVE") != "1", reason="set RUN_LIVE=1 to hit binance.com")


@pytest.fixture(scope="module")
def client():
    return BinanceCopyClient(delay=0.6)


@pytest.fixture(scope="module")
def top_pid(client):
    data = client.query_list(page=1, page_size=3, time_range="30D", sort="ROI")
    assert data["total"] > 100
    return str(data["list"][0]["leadPortfolioId"])


def test_leaderboard_shape(client):
    data = client.query_list(page=1, page_size=5, time_range="30D", sort="ROI")
    row = data["list"][0]
    for k in ("leadPortfolioId", "nickname", "roi", "pnl", "aum", "mdd", "winRate", "currentCopyCount", "maxCopyCount"):
        assert k in row, k
    assert len(data["list"]) == 5


@pytest.mark.parametrize("tr", TIME_RANGES)
def test_performance_all_ranges(client, top_pid, tr):
    p = client.performance(top_pid, tr)
    assert p["timeRange"] == tr and "roi" in p and "mdd" in p


def test_chart_365d_has_daily_points(client, top_pid):
    pts = client.chart_data(top_pid, "365D", "ROI")
    assert len(pts) >= 7
    assert {"value", "dateTime"} <= set(pts[0])


def test_detail_min_copy_amount(client, top_pid):
    d = client.detail(top_pid)
    assert d["leadPortfolioId"] == top_pid
    assert d.get("fixedAmountMinCopyUsd") is not None or d.get("fixedRadioMinCopyUsd") is not None


def test_service_full_fetch_and_flatten(tmp_path, client, top_pid):
    svc = CopyTradeService(ServiceConfig(data_dir=str(tmp_path), delay=0.6), client=client)
    raw = svc.fetch_lead(top_pid)
    assert set(raw["performance"]) == set(TIME_RANGES)
    row = flatten_lead(raw)
    assert row["portfolioId"] == top_pid and row["daysTrading"] is not None and row["roi30d"] is not None


def test_order_history_and_linked(client, top_pid):
    o = client.order_history(top_pid, page=1, page_size=100)
    assert "list" in o and len(o["list"]) <= 100
    if o["list"]:
        assert {"symbol", "side", "type", "executedQty", "avgPrice", "orderTime"} <= set(o["list"][0])
    linked = client.linked_portfolios(top_pid)
    assert isinstance(linked, dict)


def test_pnl_chart_and_big_transfer_pages(client, top_pid):
    pts = client.chart_data(top_pid, "365D", "PNL")
    assert pts and pts[0]["dataType"] == "PNL"
    t = client.transfer_history(top_pid, page=1, page_size=100)
    assert len(t["list"]) == min(100, int(t["total"]))


def test_full_sections_fetch(tmp_path, client, top_pid):
    from binance_copytrade import Sections
    svc = CopyTradeService(ServiceConfig(data_dir=str(tmp_path), delay=0.6), client=client)
    ls = svc.enrich({}, [top_pid], sections=Sections.full(history_days=30))
    raw = ls.raw(top_pid)
    assert not ls.meta["failed"] and raw["_fetch"]["transferHistory"]["truncated"] is False
    assert raw.get("linkedPortfolios") is not None and raw.get("chartPnl") and raw.get("orderHistory") is not None
