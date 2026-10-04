"""FetchFilter: which leads to pull (server-side params) and which to keep before the expensive per-lead fetch."""
from __future__ import annotations

from dataclasses import asdict, dataclass

from . import clock
from .client import PORTFOLIO_TYPES, SORT_TYPES, TIME_RANGES, _f

SOURCES = ("all", "ai", "recommended")
RECOMMENDED_GROUPS = ("highestPnlLeads", "highestRoiLeads", "highestCopyPnlLeads",
                      "mostPopularLeads", "highestWinRateLeads", "primePicksLeads")
BADGES = ("LEGEND", "CHAMPION", "MASTER", "EXPERT")


@dataclass
class FetchFilter:
    """Which leads to pull, and which to keep before the (expensive) full fetch.

    Server-side (sent to Binance):
        source          "all" | "ai" (Binance AI-recommended subset) | "recommended" (curated groups)
        sort            ROI | PNL | MDD | AUM | COPY_COUNT | COPIER_PNL | SHARP_RATIO | WIN_RATE
                        (comma list allowed -> union of several leaderboards)
        time_range      7D | 30D | 90D | 180D | 365D   (window used for sort AND for roi/pnl/mdd values)
        order           DESC | ASC
        portfolio_type  ALL | PUBLIC | PRIVATE | SIGNAL
        hide_full       skip leads with no copier slots
        nickname        substring search
        pages           30 rows per page
        groups          (source=recommended only) subset of RECOMMENDED_GROUPS
    Client-side (applied to leaderboard rows, no extra requests):
        min_days, max_days, min_aum, max_aum, min_copiers, max_copiers, min_roi, max_roi,
        max_mdd, min_win_rate, min_sharpe, min_copier_pnl, badges, require_api_key, require_tradfi,
        exclude_full, exclude_ids, include_ids, limit
    """
    source: str = "all"
    sort: str = "ROI"
    time_range: str = "30D"
    order: str = "DESC"
    portfolio_type: str = "ALL"
    hide_full: bool = False
    nickname: str = ""
    pages: int = 2
    groups: tuple[str, ...] | None = None
    # client-side
    min_days: int | None = None
    max_days: int | None = None
    min_aum: float | None = None
    max_aum: float | None = None
    min_copiers: int | None = None
    max_copiers: int | None = None
    min_roi: float | None = None
    max_roi: float | None = None
    max_mdd: float | None = None
    min_win_rate: float | None = None
    min_sharpe: float | None = None
    min_copier_pnl: float | None = None
    badges: tuple[str, ...] | None = None
    require_api_key: bool | None = None
    require_tradfi: bool | None = None
    exclude_full: bool = False
    exclude_ids: tuple[str, ...] = ()
    include_ids: tuple[str, ...] = ()
    limit: int | None = None

    @property
    def sorts(self) -> list[str]:
        return [x.strip().upper() for x in self.sort.split(",") if x.strip()]

    def validate(self) -> "FetchFilter":
        assert self.source in SOURCES, f"source must be one of {SOURCES}"
        assert self.time_range in TIME_RANGES, f"time_range must be one of {TIME_RANGES}"
        assert self.portfolio_type in PORTFOLIO_TYPES, f"portfolio_type must be one of {PORTFOLIO_TYPES}"
        assert self.order in ("DESC", "ASC")
        for x in self.sorts:
            assert x in SORT_TYPES, f"sort {x!r} not in {SORT_TYPES}"
        if self.groups:
            for g in self.groups:
                assert g in RECOMMENDED_GROUPS, f"group {g!r} not in {RECOMMENDED_GROUPS}"
        return self

    def replace(self, **kw) -> "FetchFilter":
        bad = [k for k in kw if k not in self.__dataclass_fields__]
        if bad:
            raise TypeError(f"unknown filter option(s): {bad}; see FetchFilter.options()")
        return FetchFilter(**{**asdict(self), **kw})

    def keep(self, r: dict) -> bool:
        """Client-side pre-filter on a leaderboard row."""
        pid = str(r.get("leadPortfolioId"))
        if self.include_ids and pid not in self.include_ids:
            return False
        if pid in self.exclude_ids:
            return False
        days = (clock.now_ms() - r["startTime"]) / 86_400_000 if r.get("startTime") else None
        cur, mx = r.get("currentCopyCount"), r.get("maxCopyCount")
        ge = lambda v, t: v is not None and v >= t
        le = lambda v, t: v is not None and v <= t
        checks = [
            (self.min_days, days, ge), (self.max_days, days, le),
            (self.min_aum, _f(r.get("aum")), ge), (self.max_aum, _f(r.get("aum")), le),
            (self.min_copiers, cur, ge), (self.max_copiers, cur, le),
            (self.min_roi, _f(r.get("roi")), ge), (self.max_roi, _f(r.get("roi")), le),
            (self.max_mdd, _f(r.get("mdd")), le),
            (self.min_win_rate, _f(r.get("winRate")), ge),
            (self.min_sharpe, _f(r.get("sharpRatio")), ge),
            (self.min_copier_pnl, _f(r.get("copierPnl")), ge),
        ]
        for threshold, value, ok in checks:
            if threshold is not None and not ok(value, threshold):
                return False
        if self.badges and (r.get("badgeName") or "") not in self.badges:
            return False
        if self.require_api_key is not None and bool(r.get("apiKeyTag")) != self.require_api_key:
            return False
        if self.require_tradfi is not None and bool(r.get("tradFiTag")) != self.require_tradfi:
            return False
        if self.exclude_full and cur is not None and mx and cur >= mx:
            return False
        return True

    def describe(self) -> str:
        d = {k: v for k, v in asdict(self).items() if v not in (None, (), "", False)}
        return ", ".join(f"{k}={v}" for k, v in d.items())

    @staticmethod
    def options() -> dict:
        """Every choice a caller can make, for UIs / help text."""
        return {
            "source": SOURCES, "sort": SORT_TYPES, "time_range": TIME_RANGES, "order": ("DESC", "ASC"),
            "portfolio_type": PORTFOLIO_TYPES, "groups": RECOMMENDED_GROUPS, "badges": BADGES,
            "client_side": ["min_days", "max_days", "min_aum", "max_aum", "min_copiers", "max_copiers",
                            "min_roi", "max_roi", "max_mdd", "min_win_rate", "min_sharpe", "min_copier_pnl",
                            "badges", "require_api_key", "require_tradfi", "exclude_full",
                            "exclude_ids", "include_ids", "limit"],
            "page_size": 30,
        }
