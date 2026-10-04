"""
binance_copytrade.client
------------------------
Thin client for the *unofficial* Binance Futures Copy-Trading web endpoints
(the same ones the https://www.binance.com/en/copy-trading page calls).

IMPORTANT
- These endpoints are NOT part of the official Binance API. No SLA, no docs,
  may change or disappear at any time. Do not build anything critical on them.
- Everything here works without login / cookies / signature. If Binance starts
  returning code != "000000" with an auth-style message, stop and re-inspect
  the page in DevTools rather than trying to bypass it.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

BASE = "https://www.binance.com/bapi/futures/v1"

# Paths observed in DevTools (2026-09-05)
EP_QUERY_LIST = "/friendly/future/copy-trade/home-page/query-list"            # POST
EP_RECOMMEND = "/friendly/future/copy-trade/home-page/recommend-lead-list"      # POST
EP_DETAIL = "/friendly/future/copy-trade/lead-portfolio/detail"                 # GET
EP_PERFORMANCE = "/public/future/copy-trade/lead-portfolio/performance"         # GET
EP_CHART = "/public/future/copy-trade/lead-portfolio/chart-data"                # GET
EP_COIN = "/public/future/copy-trade/lead-portfolio/performance/coin"           # GET
EP_POSITIONS = "/friendly/future/copy-trade/lead-data/positions"                # GET
EP_POSITION_HISTORY = "/friendly/future/copy-trade/lead-portfolio/position-history"  # POST
EP_TRANSFER_HISTORY = "/friendly/future/copy-trade/lead-portfolio/transfer-history"  # POST
EP_COPY_TRADERS = "/friendly/future/copy-trade/lead-portfolio/copy-traders"     # POST
EP_ORDER_HISTORY = "/friendly/future/copy-trade/lead-portfolio/order-history"   # POST  ("Latest Records" tab, 2026-10-04)
EP_LINKED = "/friendly/future/spot-copy-trade/common/spot-futures-last-lead"     # GET   owner's other portfolios

TIME_RANGES = ("7D", "30D", "90D", "180D", "365D")
SORT_TYPES = ("ROI", "PNL", "MDD", "AUM", "COPY_COUNT", "COPIER_PNL", "SHARP_RATIO", "WIN_RATE")
PORTFOLIO_TYPES = ("ALL", "PUBLIC", "PRIVATE", "SIGNAL")
MAX_PAGE_SIZE = 30  # leaderboard: server silently caps pageSize at 30
# per-lead lists: largest page sizes the server honoured in live tests (2026-10-04)
HISTORY_PAGE_SIZE = 100
TRANSFER_PAGE_SIZE = 100
ORDER_PAGE_SIZE = 100
COPIER_PAGE_SIZE = 30

CODE_PORTFOLIO_CLOSED = "11012028"          # "The current portfolio is closed."
TRANSIENT_CODES = ("11012005",)             # "The system is currently busy. Please try again later."

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "lang": "en",
    "clienttype": "web",
    "Origin": "https://www.binance.com",
    "Referer": "https://www.binance.com/en/copy-trading",
}


class BinanceCopyError(RuntimeError):
    """Any non-success answer. `code` is Binance's business code when there was one."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.code = code


class PortfolioClosedError(BinanceCopyError):
    """The lead closed this portfolio (code 11012028). Not retried; callers record it as closed."""


class AuthRequiredError(BinanceCopyError):
    """The endpoint started asking for login. Stop; do not try to bypass."""


class BinanceCopyClient:
    """requests.Session wrapper with retry/backoff and a polite delay between calls."""

    def __init__(
        self,
        delay: float = 1.0,
        jitter: float = 0.5,
        max_retries: int = 5,
        backoff: float = 1.5,
        timeout: float = 20.0,
    ):
        self.delay = delay
        self.jitter = jitter
        self.max_retries = max_retries
        self.backoff = backoff
        self.timeout = timeout
        self._last_call = 0.0

        self.s = requests.Session()
        self.s.headers.update(DEFAULT_HEADERS)
        # transport-level retries for connection errors / 429 / 5xx
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=4)
        self.s.mount("https://", adapter)

    # ---------------------------------------------------------------- core
    def _throttle(self) -> None:
        wait = self.delay + random.uniform(0, self.jitter) - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _call(self, method: str, path: str, *, params=None, json=None) -> Any:
        url = BASE + path
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                r = self.s.request(method, url, params=params, json=json, timeout=self.timeout)
                r.raise_for_status()
                body = r.json()
            except (requests.RequestException, ValueError) as e:
                last_exc = e
                sleep = self.backoff ** attempt + random.uniform(0, 1)
                log.warning("%s %s failed (%s), retry %d/%d in %.1fs", method, path, e, attempt, self.max_retries, sleep)
                time.sleep(sleep)
                continue

            code = body.get("code")
            if code == "000000":
                return body.get("data")
            msg = body.get("message") or body.get("messageDetail")
            # auth-style failures: do NOT retry, surface to caller
            if code in ("100001003", "100002001") or (msg and "login" in str(msg).lower()):
                raise AuthRequiredError(f"endpoint now requires auth: {code} {msg} ({path})", code)
            if code == CODE_PORTFOLIO_CLOSED:
                raise PortfolioClosedError(f"API error {code}: {msg} ({path} params={params} json={json})", code)
            if code in TRANSIENT_CODES or (msg and any(w in str(msg).lower() for w in ("busy", "try again", "too many"))):
                last_exc = BinanceCopyError(f"API error {code}: {msg}", code)
                sleep = self.backoff ** attempt + random.uniform(0, 1)
                log.warning("%s %s busy (%s), retry %d/%d in %.1fs", method, path, code, attempt, self.max_retries, sleep)
                time.sleep(sleep)
                continue
            raise BinanceCopyError(f"API error {code}: {msg} ({path} params={params} json={json})", code)
        raise BinanceCopyError(f"{method} {path} failed after {self.max_retries} retries: {last_exc}",
                               getattr(last_exc, "code", None))

    # ---------------------------------------------------------- leaderboard
    def query_list(
        self,
        page: int = 1,
        page_size: int = MAX_PAGE_SIZE,
        time_range: str = "30D",
        sort: str = "ROI",
        order: str = "DESC",
        portfolio_type: str = "ALL",
        hide_full: bool = False,
        nickname: str = "",
        ai_recommended: bool = False,
    ) -> dict:
        """One leaderboard page. Returns {'total': int, 'list': [...]}."""
        assert time_range in TIME_RANGES, time_range
        assert portfolio_type in PORTFOLIO_TYPES, portfolio_type
        body = {
            "pageNumber": page,
            "pageSize": min(page_size, MAX_PAGE_SIZE),
            "timeRange": time_range,
            "dataType": sort,
            "favoriteOnly": False,
            "hideFull": hide_full,
            "nickname": nickname,
            "order": order,
            "userAsset": 0,
            "portfolioType": portfolio_type,
        }
        if ai_recommended:
            body["useAiRecommended"] = True
        return self._call("POST", EP_QUERY_LIST, json=body)

    def iter_leaderboard(self, max_pages: int = 5, **kw) -> Iterable[dict]:
        """Yield lead-trader rows across pages (stops early when list is exhausted)."""
        for page in range(1, max_pages + 1):
            data = self.query_list(page=page, **kw)
            rows = data.get("list") or []
            log.info("leaderboard page %d: %d rows (total=%s)", page, len(rows), data.get("total"))
            if not rows:
                break
            yield from rows
            if page * MAX_PAGE_SIZE >= int(data.get("total") or 0):
                break

    def recommend_lead_list(self) -> dict:
        return self._call("POST", EP_RECOMMEND, json={})

    # ------------------------------------------------------ portfolio detail
    def detail(self, portfolio_id: str) -> dict:
        return self._call("GET", EP_DETAIL, params={"portfolioId": portfolio_id})

    def performance(self, portfolio_id: str, time_range: str = "30D") -> dict:
        assert time_range in TIME_RANGES, time_range
        return self._call("GET", EP_PERFORMANCE, params={"portfolioId": portfolio_id, "timeRange": time_range})

    def chart_data(self, portfolio_id: str, time_range: str = "30D", data_type: str = "ROI") -> list:
        return self._call(
            "GET", EP_CHART,
            params={"portfolioId": portfolio_id, "timeRange": time_range, "dataType": data_type},
        )

    def coin_preference(self, portfolio_id: str, time_range: str = "30D") -> dict:
        return self._call("GET", EP_COIN, params={"portfolioId": portfolio_id, "timeRange": time_range})

    def positions(self, portfolio_id: str) -> list:
        """Open positions. Empty list if the lead trader set positions to private."""
        return self._call("GET", EP_POSITIONS, params={"portfolioId": portfolio_id}) or []

    def position_history(self, portfolio_id: str, page: int = 1, page_size: int = HISTORY_PAGE_SIZE,
                         sort: str = "OPENING") -> dict:
        """Closed positions, newest opening first. {'total': int, 'list': [...]}."""
        return self._call(
            "POST", EP_POSITION_HISTORY,
            json={"pageNumber": page, "pageSize": page_size, "portfolioId": portfolio_id, "sort": sort},
        )

    def transfer_history(self, portfolio_id: str, page: int = 1, page_size: int = TRANSFER_PAGE_SIZE) -> dict:
        """Lead deposits / withdrawals, newest first. {'total': int, 'list': [...]}."""
        return self._call(
            "POST", EP_TRANSFER_HISTORY,
            json={"pageNumber": page, "pageSize": page_size, "portfolioId": portfolio_id},
        )

    def copy_traders(self, portfolio_id: str, page: int = 1, page_size: int = COPIER_PAGE_SIZE) -> dict:
        """Top copiers (server caps pageSize at 30)."""
        return self._call(
            "POST", EP_COPY_TRADERS,
            json={"pageNumber": page, "pageSize": page_size, "portfolioId": portfolio_id},
        )

    def order_history(self, portfolio_id: str, page: int = 1, page_size: int = ORDER_PAGE_SIZE) -> dict:
        """Individual orders ("Latest Records"), newest first: symbol, side, type (LIMIT/MARKET...), positionSide,
        executedQty, avgPrice, totalPnl, orderTime, orderUpdateTime. Page with pageNumber (the response's
        indexValue is a cursor the web page uses differently; pageNumber pages are contiguous)."""
        return self._call(
            "POST", EP_ORDER_HISTORY,
            json={"pageNumber": page, "pageSize": page_size, "portfolioId": portfolio_id},
        )

    def linked_portfolios(self, portfolio_id: str) -> dict:
        """The same owner's portfolios: futuresPublicLPId/Status, futuresPrivateLPId/Status, futuresSignalLPId/Status,
        spot..., leadOwner."""
        return self._call("GET", EP_LINKED, params={"portfolioId": portfolio_id}) or {}


# ----------------------------------------------------------------- helpers
def to_float(v) -> float | None:
    """float(v) or None for missing / non-numeric values (Binance sends numbers as strings)."""
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


_f = to_float  # short internal alias


def min_margin_from_detail(detail: dict) -> float | None:
    """Minimum copy amount (USD). Binance exposes two: fixed-ratio and fixed-amount mode."""
    a = to_float(detail.get("fixedRadioMinCopyUsd"))
    b = to_float(detail.get("fixedAmountMinCopyUsd"))
    vals = [x for x in (a, b) if x is not None]
    return min(vals) if vals else None
