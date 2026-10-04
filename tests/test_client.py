"""Unit tests for binance_copytrade.client (HTTP layer mocked via requests.Session.request)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from binance_copytrade.client import (
    BinanceCopyClient, BinanceCopyError, MAX_PAGE_SIZE, EP_QUERY_LIST, EP_DETAIL, min_margin_from_detail,
)


def _resp(payload: dict, status: int = 200):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.json.return_value = payload
    r.raise_for_status.return_value = None
    return r


@pytest.fixture
def client():
    c = BinanceCopyClient(delay=0, jitter=0, max_retries=3, backoff=1.0)
    c._throttle = lambda: None
    return c


def test_headers_look_like_browser(client):
    h = client.s.headers
    assert "Chrome" in h["User-Agent"]
    assert h["clienttype"] == "web" and h["lang"] == "en"


def test_query_list_builds_body_and_caps_page_size(client):
    with patch.object(client.s, "request", return_value=_resp({"code": "000000", "data": {"total": 1, "list": []}})) as req:
        client.query_list(page=2, page_size=500, time_range="90D", sort="PNL", portfolio_type="PUBLIC")
    kw = req.call_args.kwargs
    assert req.call_args.args[0] == "POST" and req.call_args.args[1].endswith(EP_QUERY_LIST)
    body = kw["json"]
    assert body["pageNumber"] == 2 and body["pageSize"] == MAX_PAGE_SIZE
    assert body["timeRange"] == "90D" and body["dataType"] == "PNL" and body["portfolioType"] == "PUBLIC"
    assert "useAiRecommended" not in body


def test_query_list_rejects_bad_time_range(client):
    with pytest.raises(AssertionError):
        client.query_list(time_range="1D")


def test_detail_get_params(client):
    with patch.object(client.s, "request", return_value=_resp({"code": "000000", "data": {"x": 1}})) as req:
        assert client.detail("123") == {"x": 1}
    assert req.call_args.args == ("GET", "https://www.binance.com/bapi/futures/v1" + EP_DETAIL)
    assert req.call_args.kwargs["params"] == {"portfolioId": "123"}


def test_api_error_code_raises(client):
    with patch.object(client.s, "request", return_value=_resp({"code": "11012004", "message": "Invalid input."})):
        with pytest.raises(BinanceCopyError, match="11012004"):
            client.performance("1", "30D")


def test_auth_required_is_reported_not_retried(client):
    with patch.object(client.s, "request", return_value=_resp({"code": "100002001", "message": "please login"})) as req:
        with pytest.raises(BinanceCopyError, match="requires auth"):
            client.positions("1")
    assert req.call_count == 1


def test_network_error_retries_then_succeeds(client, monkeypatch):
    monkeypatch.setattr("binance_copytrade.client.time.sleep", lambda *_: None)
    ok = _resp({"code": "000000", "data": []})
    with patch.object(client.s, "request", side_effect=[requests.ConnectionError("boom"), ok]) as req:
        assert client.chart_data("1", "30D") == []
    assert req.call_count == 2


def test_network_error_gives_up(client, monkeypatch):
    monkeypatch.setattr("binance_copytrade.client.time.sleep", lambda *_: None)
    with patch.object(client.s, "request", side_effect=requests.ConnectionError("boom")):
        with pytest.raises(BinanceCopyError, match="after 3 retries"):
            client.chart_data("1", "30D")


def test_iter_leaderboard_stops_at_total(client):
    pages = {1: [{"leadPortfolioId": str(i)} for i in range(30)], 2: [{"leadPortfolioId": "30"}]}

    def fake(method, url, params=None, json=None, timeout=None):
        return _resp({"code": "000000", "data": {"total": 31, "list": pages.get(json["pageNumber"], [])}})

    with patch.object(client.s, "request", side_effect=fake) as req:
        rows = list(client.iter_leaderboard(max_pages=10))
    assert len(rows) == 31 and req.call_count == 2


def test_throttle_waits_between_calls(monkeypatch):
    c = BinanceCopyClient(delay=0.5, jitter=0)
    slept = []
    monkeypatch.setattr("binance_copytrade.client.time.sleep", lambda s: slept.append(s))
    t = iter([100.0, 100.0, 100.1, 100.1])
    monkeypatch.setattr("binance_copytrade.client.time.monotonic", lambda: next(t))
    c._throttle()
    c._throttle()
    assert slept and abs(slept[-1] - 0.4) < 1e-6


def test_min_margin_from_detail():
    assert min_margin_from_detail({"fixedRadioMinCopyUsd": 100, "fixedAmountMinCopyUsd": 10}) == 10
    assert min_margin_from_detail({"fixedRadioMinCopyUsd": None, "fixedAmountMinCopyUsd": "50"}) == 50
    assert min_margin_from_detail({}) is None
