"""Offline test kit: canned Binance payloads and fake clients (no network). Used by this package's tests and
available to downstream projects:

    from binance_copytrade.testing import FakeClient, BatchFakeClient, freeze_clock, NOW
    svc = CopyTradeService(ServiceConfig(data_dir=str(tmp_path), delay=0), client=BatchFakeClient())

Payloads are shaped like real responses observed on 2026-09-05. NOW is ~2026-09-05 UTC; freeze the package clock
at NOW (freeze_clock) so derived ages are stable.
"""
from __future__ import annotations

from . import clock
from .client import BinanceCopyClient, BinanceCopyError, PortfolioClosedError

PID = "5075281354358777856"
DAY = 86_400_000
NOW = 1_788_600_000_000  # ~2026-09-05 UTC

LEADERBOARD_ITEM = {
    "leadPortfolioId": PID, "nickname": "熬鹰资本", "currentCopyCount": 1000, "maxCopyCount": 1000,
    "roi": 199.83, "pnl": 230825.7, "aum": 6352533.0, "mdd": 9.12, "winRate": 69.7, "copierPnl": 460952.0,
    "apiKeyTag": "API_KEY_TRADE", "tradFiTag": "Tradfi", "sharpRatio": 3.97, "badgeName": "LEGEND",
    "portfolioType": "PUBLIC", "startTime": NOW - 94 * DAY, "aiSummary": "High win-rate",
    "chartItems": [],
}

DETAIL = {
    "leadPortfolioId": PID, "nickname": "熬鹰资本", "nicknameTranslate": "Aoying Capital", "status": "ACTIVE",
    "currentCopyCount": 1000, "maxCopyCount": 1000, "totalCopyCount": 1872, "closeLeadCount": 9,
    "marginBalance": "346333.47", "aumAmount": "6361547.86", "copierPnl": "6220607.94",
    "profitSharingRate": "10.00", "sharpRatio": "3.97428754", "startTime": NOW - 94 * DAY,
    "tag": ["MEDIUM_LEVERAGE", "API_KEY_TRADE"], "positionShow": False, "mockCopyCount": 35780,
    "favoriteCount": 22028, "lockPeriod": 0, "badgeName": "LEGEND",
    "fixedRadioMinCopyUsd": 300.0, "fixedAmountMinCopyUsd": 300.0, "portfolioType": "PUBLIC",
    "lastTradeTime": NOW - 1 * DAY, "aiSummary": "High win-rate, Multi-asset, Low drawdown",
}


def perf(tr, roi, pnl, mdd, win, wo, to, cp):
    return {"timeRange": tr, "roi": roi, "pnl": pnl, "mdd": mdd, "winRate": win,
            "winOrders": wo, "totalOrder": to, "copierPnl": cp, "sharpRatio": "3.97428754"}


PERFORMANCE = {
    "7D": perf("7D", 9.70, 30628.7, 7.35, 66.67, 4, 6, 460952.0),
    "30D": perf("30D", 199.83, 230825.7, 9.12, 69.70, 23, 33, 1_200_000.0),
    "90D": perf("90D", 1352.62, 1643343.7, 68.31, 69.03, 78, 113, 5_900_000.0),
    "180D": perf("180D", 1352.62, 1643343.7, 68.31, 69.03, 78, 113, 5_900_000.0),
    "365D": perf("365D", 1352.62, 1643343.7, 68.31, 69.03, 78, 113, 5_900_000.0),
}

CHART = [{"value": v, "dataType": "ROI", "dateTime": NOW - (10 - i) * DAY}
         for i, v in enumerate([0, 5, 3, 8, 12, 10, 15, 20, 18, 25])]

COINS = {"timeRange": "30D", "updateTime": NOW, "data": [{"asset": "XAU", "volume": 37.57}, {"asset": "ZEC", "volume": 11.75}]}

POSITION_HISTORY = [
    {"symbol": "ZECUSDT", "side": "Long", "leverage": "2", "closingPnl": 45724.4, "opened": NOW - 3 * DAY, "closed": NOW - 2 * DAY, "roi": "0.18"},
    {"symbol": "SPCXUSDT", "side": "Short", "leverage": "4", "closingPnl": -14446.4, "opened": NOW - 5 * DAY, "closed": NOW - 4 * DAY, "roi": "-0.09"},
    {"symbol": "BTCUSDT", "side": "Short", "leverage": "6", "closingPnl": 4911.4, "opened": NOW - 8 * DAY, "closed": NOW - 7 * DAY, "roi": "0.11"},
]

TRANSFERS = [
    {"time": NOW - 30 * DAY, "coin": "USDT", "amount": 1252210.0, "transType": "LEAD_WITHDRAW"},
    {"time": NOW - 90 * DAY, "coin": "USDT", "amount": 20000.0, "transType": "LEAD_DEPOSIT"},
    {"time": NOW - 94 * DAY, "coin": "USDT", "amount": 10000.0, "transType": "LEAD_INVEST"},
]

COPIERS = [{"copyPortfolioId": "1", "nickname": "南***", "marginBalance": 296454.7, "totalPnl": 193874.9, "totalRoi": 109.87},
           {"copyPortfolioId": "2", "nickname": "梅***", "marginBalance": 217315.1, "totalPnl": 169642.0, "totalRoi": 267.79}]


CHART_PNL = [{"value": v * 1000, "dataType": "PNL", "dateTime": NOW - (10 - i) * DAY}
             for i, v in enumerate([0, 5, 3, 8, 12, 10, 15, 20, 18, 25])]

ORDERS = [{"symbol": "ZECUSDT", "baseAsset": "ZEC", "quoteAsset": "USDT", "side": "SELL" if i % 2 else "BUY",
           "type": "LIMIT" if i % 4 else "MARKET", "positionSide": "LONG", "executedQty": 0.05, "avgPrice": 1300 + i,
           "totalPnl": 0.7 if i % 2 else 0.0, "orderTime": NOW - i * DAY // 4, "orderUpdateTime": NOW - i * DAY // 4}
          for i in range(12)]

LINKED = {"leadOwner": False, "futuresPublicLPId": PID, "futuresPublicLPStatus": "ACTIVE",
          "futuresPrivateLPId": "5043232486099187456", "futuresPrivateLPStatus": "ACTIVE",
          "futuresSignalLPId": None, "futuresSignalLPStatus": None}


def _page(items: list, body: dict) -> dict:
    size, page = int(body.get("pageSize") or 50), int(body.get("pageNumber") or 1)
    return {"total": len(items), "list": items[(page - 1) * size: page * size]}


class FakeClient(BinanceCopyClient):
    """Overrides the HTTP layer only; every public method of the real client is exercised.
    Lists page like the real server (pageNumber / pageSize, total). Knobs for tests:
        history / transfers / orders / open_positions   replace the canned lists
        closed_ids          detail() raises PortfolioClosedError (code 11012028) for these ids
        broken_paths        {path suffix: message} -> BinanceCopyError for that endpoint
    """

    def __init__(self):
        super().__init__(delay=0, jitter=0, max_retries=1)
        self.calls: list[tuple[str, str, dict]] = []
        self.history = list(POSITION_HISTORY)
        self.transfers = list(TRANSFERS)
        self.orders = list(ORDERS)
        self.open_positions: list = []
        self.closed_ids: set[str] = set()
        self.broken_paths: dict[str, str] = {}

    def _throttle(self):  # no sleeping in tests
        pass

    def _call(self, method, path, *, params=None, json=None):
        body = params or json or {}
        self.calls.append((method, path, body))
        for suffix, msg in self.broken_paths.items():
            if path.endswith(suffix):
                raise BinanceCopyError(f"API error 999: {msg} ({path})", "999")
        if path.endswith("/query-list"):
            page = json["pageNumber"]
            return {"total": 31, "list": [LEADERBOARD_ITEM] if page == 1 else [
                {**LEADERBOARD_ITEM, "leadPortfolioId": "2", "nickname": "second", "roi": 5.0, "currentCopyCount": 50}]}
        if path.endswith("/detail"):
            if str(params["portfolioId"]) in self.closed_ids:
                raise PortfolioClosedError("API error 11012028: The current portfolio is closed.", "11012028")
            return dict(DETAIL, leadPortfolioId=params["portfolioId"])
        if path.endswith("/performance"):
            return PERFORMANCE[params["timeRange"]]
        if path.endswith("/chart-data"):
            return CHART_PNL if params.get("dataType") == "PNL" else CHART
        if path.endswith("/performance/coin"):
            return COINS
        if path.endswith("/lead-data/positions"):
            return self.open_positions
        if path.endswith("/position-history"):
            return _page(self.history, json)
        if path.endswith("/transfer-history"):
            return _page(self.transfers, json)
        if path.endswith("/order-history"):
            return _page(self.orders, json)
        if path.endswith("/copy-traders"):
            return {"total": 2, "list": COPIERS}
        if path.endswith("/spot-futures-last-lead"):
            return dict(LINKED, futuresPublicLPId=params["portfolioId"])
        if path.endswith("/recommend-lead-list"):
            return {"highestPnlLeads": [LEADERBOARD_ITEM]}
        raise AssertionError(f"unexpected path {path}")


RANGES = ("7D", "30D", "90D", "180D", "365D")
N_LEADS = 45  # BatchFakeClient: 2 pages: 30 + 15


class BatchFakeClient(FakeClient):
    """query-list answers depend on timeRange and page; same lead set across ranges (AUM sort)."""

    def _call(self, method, path, *, params=None, json=None):
        if path.endswith("/query-list"):
            self.calls.append((method, path, json))
            page, tr = json["pageNumber"], json["timeRange"]
            days = {"7D": 7, "30D": 30, "90D": 90, "180D": 180, "365D": 365}[tr]
            start, end = (page - 1) * 30, min(page * 30, N_LEADS)
            items = []
            for i in range(start, end):
                age = 5 + i * 10                       # lead i is 5,15,25,... days old
                roi = (i % 7 - 2) * 10 * days / 30     # some negative, scales with range
                items.append({**LEADERBOARD_ITEM, "leadPortfolioId": f"L{i}", "nickname": f"lead{i}",
                              "roi": roi, "pnl": roi * 10, "mdd": 5 + (i % 5) * 10, "winRate": 50 + i % 40,
                              "sharpRatio": (i % 4) or None, "copierPnl": (i % 3 - 1) * 1000.0,
                              "aum": 100_000 - i * 1000, "currentCopyCount": i, "maxCopyCount": 200,
                              "startTime": NOW - age * DAY,
                              "chartItems": [{"value": roi * k / days, "dateTime": NOW - (days - k) * DAY}
                                             for k in range(days + 1)]})
            return {"total": N_LEADS, "list": items}
        if path.endswith("/detail") and str(params["portfolioId"]) not in self.closed_ids:
            self.calls.append((method, path, params))
            return dict(DETAIL, leadPortfolioId=params["portfolioId"], currentCopyCount=5)
        return super()._call(method, path, params=params, json=json)


def freeze_clock(monkeypatch, ms: float = NOW) -> None:
    """pytest helper: make binance_copytrade.clock return a fixed time."""
    monkeypatch.setattr(clock, "now_ms", lambda: float(ms))
