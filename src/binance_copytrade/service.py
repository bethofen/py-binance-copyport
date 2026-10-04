"""CopyTradeService: fetch leads from the public Binance copy-trading web endpoints into LeadSets,
and save / load them through LocalStore. Nothing is written unless you call save()/save_batch()
(enrich's optional checkpoint file is the one exception, and it is removed after a successful save).

    from binance_copytrade import CopyTradeService, FetchFilter, Sections
    svc = CopyTradeService()
    ls  = svc.collect(FetchFilter(source="ai", sort="AUM"))         # leaderboard + full per-lead fetch
    svc.save(ls, fmt="json")                                         # data/daily/<date>/
    merged = svc.batch_collect(pages=167, sort="AUM")                # stage 1: whole market, 5 requests / 30 leads
    ls = svc.pipeline(FetchFilter(min_days=30, max_mdd=40), batch=merged, sections=Sections.full())
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence

import pandas as pd

from . import clock
from .client import (COPIER_PAGE_SIZE, HISTORY_PAGE_SIZE, ORDER_PAGE_SIZE, TIME_RANGES, TRANSFER_PAGE_SIZE,
                     BinanceCopyClient, BinanceCopyError, PortfolioClosedError, _f)
from .filters import RECOMMENDED_GROUPS, FetchFilter
from .leadset import LeadSet
from .normalize import DAY_MS, batch_to_raw, flatten_lead, order_columns
from .store import LocalStore

log = logging.getLogger("binance_copytrade")


@dataclass
class Sections:
    """Which per-lead lists to fetch on top of `detail`, and how deep.

    history_days   page position history until it reaches this many days back (None = only history_pages)
    history_pages  hard cap on position-history pages (100 positions each)
    transfers      ALL lead deposits/withdrawals (paged until total; transfer_pages cap)
    positions      open positions (skipped when the lead hides them: raw gets positionsPrivate=True)
    copiers        top copiers (30)
    orders         individual orders ("Latest Records"), back order_days, at most order_pages x 100
    linked         the same owner's other portfolios (public / private / signal)
    pnl_chart      daily PnL curve in USDT (chart_range), next to the ROI curve
    """
    history_days: int | None = None
    history_pages: int = 2
    transfers: bool = False
    transfer_pages: int = 20
    positions: bool = False
    copiers: bool = False
    orders: bool = False
    order_days: int = 30
    order_pages: int = 10
    linked: bool = False
    pnl_chart: bool = False

    @classmethod
    def full(cls, history_days: int = 90) -> "Sections":
        """Everything the analysis layer uses (orders included)."""
        return cls(history_days=history_days, history_pages=30, transfers=True, positions=True, copiers=True,
                   orders=True, linked=True, pnl_chart=True)

    def wanted(self) -> set[str]:
        """Raw keys a lead must hold to count as fetched with these sections (used by resume)."""
        w = {"detail", "positionHistory"}
        w |= {k for k, on in (("transferHistory", self.transfers), ("copyTraders", self.copiers),
                              ("orderHistory", self.orders), ("linkedPortfolios", self.linked),
                              ("chartPnl", self.pnl_chart)) if on}
        if self.positions:
            w.add("positions")      # satisfied by positionsPrivate too, see has_sections
        return w


def has_sections(raw: dict | None, sections: Sections) -> bool:
    if not raw or not raw.get("detail"):
        return False
    need = sections.wanted()
    if "positions" in need and raw.get("positionsPrivate"):
        need = need - {"positions"}
    return all(raw.get(k) is not None for k in need)


@dataclass
class ServiceConfig:
    data_dir: str = "data"
    delay: float = 1.0
    ranges: tuple[str, ...] = TIME_RANGES          # 7D,30D,90D,180D,365D
    chart_range: str = "365D"                      # daily ROI / PnL curve length
    history_pages: int = 2                         # fetch_lead: position-history pages (100 per page)
    history_days: int | None = None                # fetch_lead: page back this many days (within history_pages)
    include_copiers: bool = True
    include_transfers: bool = True
    include_positions: bool = True
    include_linked: bool = True
    include_pnl_chart: bool = True
    include_orders: bool = False                   # heavier (up to order_pages requests); off by default
    order_days: int = 30

    def sections(self) -> Sections:
        return Sections(history_days=self.history_days, history_pages=self.history_pages,
                        transfers=self.include_transfers, positions=self.include_positions,
                        copiers=self.include_copiers, orders=self.include_orders, order_days=self.order_days,
                        linked=self.include_linked, pnl_chart=self.include_pnl_chart)


class CopyTradeService:
    """Fetch -> LeadSet (in memory). Save only when you decide: svc.save(result, fmt="csv"|"json").
    Data only: scoring / analysis live in downstream projects (e.g. copytrade-quant)."""

    BATCH_SORTS = ("AUM", "COPY_COUNT")   # range-independent sorts -> same lead set for every timeRange

    def __init__(self, config: ServiceConfig | None = None, client: BinanceCopyClient | None = None,
                 store: LocalStore | None = None):
        self.cfg = config or ServiceConfig()
        self.client = client or BinanceCopyClient(delay=self.cfg.delay)
        self.store = store or LocalStore(self.cfg.data_dir)

    # ---------------------------------------------------------- fetching
    def fetch_leaderboard(self, filt: "FetchFilter | None" = None, **kw) -> list[dict]:
        """Raw leaderboard rows according to a FetchFilter (or keyword overrides, e.g. sort="AUM").

        source="all"          query-list (whole database, sorted by `sort` within `time_range`)
        source="ai"           query-list with useAiRecommended=true (Binance "Smart Filter", ~750 leads)
        source="recommended"  recommend-lead-list (6 curated groups x 10, see RECOMMENDED_GROUPS)
        Several sorts can be combined ("ROI,COPIER_PNL,AUM") -> union, de-duplicated, first-seen order.
        Client-side pre-filters (min_days, max_mdd, min_aum, badges, ...) are applied afterwards.
        """
        f = filt or FetchFilter()
        if kw:
            f = f.replace(**kw)
        f.validate()
        rows: dict[str, dict] = {}

        def _add(item: dict, group: str):
            pid = str(item.get("leadPortfolioId"))
            if pid in rows:
                rows[pid].setdefault("_groups", []).append(group)
                return
            item = dict(item)
            item["_groups"] = [group]
            rows[pid] = item

        if f.source == "recommended":
            data = self.client.recommend_lead_list() or {}
            for g in (f.groups or RECOMMENDED_GROUPS):
                for item in data.get(g) or []:
                    _add(item, g)
        else:
            for sort in f.sorts:
                for item in self.client.iter_leaderboard(
                    max_pages=f.pages, time_range=f.time_range, sort=sort, order=f.order,
                    portfolio_type=f.portfolio_type, hide_full=f.hide_full, nickname=f.nickname,
                    ai_recommended=(f.source == "ai"),
                ):
                    _add(item, f"{sort}_{f.time_range}")
        out = [r for r in rows.values() if f.keep(r)]
        log.info("leaderboard: %d fetched, %d after pre-filter (%s)", len(rows), len(out), f.describe())
        return out[: f.limit] if f.limit else out

    def _paged(self, fetch: Callable[..., dict], pid: str, page_size: int, max_pages: int,
               since_ms: float | None = None, time_key: str | None = None) -> tuple[list, dict]:
        """Page a newest-first list until it is exhausted, `total` is reached, it goes back past since_ms,
        or max_pages is hit. Returns (items, info); info.truncated = stopped by max_pages with more left."""
        items: list = []
        total = None
        pages = 0
        reached = more = False
        for page in range(1, max_pages + 1):
            data = fetch(pid, page=page, page_size=page_size) or {}
            lst = data.get("list") or []
            pages = page
            total = data.get("total", total)
            items.extend(lst)
            more = len(lst) == page_size and (total is None or len(items) < int(total))
            if since_ms and time_key and lst and min((_f(x.get(time_key)) or 0) for x in lst) < since_ms:
                reached = True
                break
            if not more:
                break
        oldest = min(((_f(x.get(time_key)) or 0) for x in items), default=None) if time_key else None
        return items, {"n": len(items), "total": total, "pages": pages, "oldest": oldest,
                       "windowReached": reached, "truncated": more and not reached}

    def _fetch_sections(self, pid: str, raw: dict, s: Sections) -> dict:
        """Fill raw with the requested per-lead lists. A section that fails is recorded in raw['_errors'];
        paging facts (total / truncated / oldest) in raw['_fetch']. detail must already be in raw."""
        c = self.client
        errors: dict = raw.setdefault("_errors", {})
        info: dict = raw.setdefault("_fetch", {})
        now = clock.now_ms()

        def _try(key: str, fn: Callable[[], object]):
            try:
                raw[key] = fn()
            except PortfolioClosedError:
                raise
            except BinanceCopyError as e:
                errors[key] = str(e)[:200]
                log.warning("%s %s: %s", key, pid, e)

        def _list(key: str, fetch, page_size, max_pages, since_days=None, time_key=None):
            def run():
                items, meta = self._paged(fetch, pid, page_size, max_pages,
                                          now - since_days * DAY_MS if since_days else None, time_key)
                info[key] = meta
                return items
            _try(key, run)

        _list("positionHistory", c.position_history, HISTORY_PAGE_SIZE, max(1, s.history_pages),
              s.history_days, "opened")
        if s.transfers:
            _list("transferHistory", c.transfer_history, TRANSFER_PAGE_SIZE, s.transfer_pages, None, "time")
        if s.positions:
            if (raw.get("detail") or {}).get("positionShow") is False:
                raw["positionsPrivate"] = True       # the endpoint would answer [] — unknown, not flat
            else:
                _try("positions", lambda: c.positions(pid))
        if s.copiers:
            _try("copyTraders", lambda: (c.copy_traders(pid, page_size=COPIER_PAGE_SIZE) or {}).get("list") or [])
        if s.orders:
            _list("orderHistory", c.order_history, ORDER_PAGE_SIZE, s.order_pages, s.order_days, "orderTime")
        if s.linked:
            _try("linkedPortfolios", lambda: c.linked_portfolios(pid))
        if s.pnl_chart:
            _try("chartPnl", lambda: {"timeRange": self.cfg.chart_range, "dataType": "PNL",
                                      "data": c.chart_data(pid, self.cfg.chart_range, "PNL")})
        if not errors:
            raw.pop("_errors")
        return raw

    def fetch_lead(self, portfolio_id: str, leaderboard_row: dict | None = None,
                   sections: Sections | None = None) -> dict:
        """Everything public about one lead: detail, 7D..365D performance, ROI chart, coins, plus the
        ServiceConfig sections (positions, history, transfers, copiers, linked portfolios, PnL chart, orders).
        Raises PortfolioClosedError for closed portfolios."""
        pid = str(portfolio_id)
        c, cfg = self.client, self.cfg
        raw: dict = {"portfolioId": pid, "timestamp": clock.now_iso(), "leaderboard": leaderboard_row or {}}
        raw["detail"] = c.detail(pid)
        perf = {}
        for tr in cfg.ranges:
            try:
                perf[tr] = c.performance(pid, tr)
            except PortfolioClosedError:
                raise
            except BinanceCopyError as e:
                raw.setdefault("_errors", {})[f"performance_{tr}"] = str(e)[:200]
                log.warning("performance %s %s: %s", pid, tr, e)
        raw["performance"] = perf
        for key, fn in (("chart", lambda: {"timeRange": cfg.chart_range, "dataType": "ROI",
                                           "data": c.chart_data(pid, cfg.chart_range, "ROI")}),
                        ("coinPreference", lambda: c.coin_preference(pid, "30D"))):
            try:
                raw[key] = fn()
            except PortfolioClosedError:
                raise
            except BinanceCopyError as e:
                raw.setdefault("_errors", {})[key] = str(e)[:200]
                log.warning("%s %s: %s", key, pid, e)
        return self._fetch_sections(pid, raw, sections or cfg.sections())

    def _fetch_many(self, items: list[tuple[str, dict | None]], label: str) -> tuple[list, dict, dict]:
        rows, raws, failed, closed = [], {}, {}, []
        for i, (pid, lb) in enumerate(items, 1):
            log.info("%s %d/%d %s %s", label, i, len(items), pid, (lb or {}).get("nickname") or "")
            try:
                raw = self.fetch_lead(pid, lb)
            except PortfolioClosedError:
                closed.append(pid)
                continue
            except BinanceCopyError as e:
                failed[pid] = str(e)[:200]
                log.error("lead %s failed: %s", pid, e)
                continue
            raws[pid] = raw
            rows.append((pid, flatten_lead(raw)))
        return rows, raws, {"failed": failed, "closed": closed}

    def collect(self, filt: "FetchFilter | None" = None, full: bool = True, **kw) -> LeadSet:
        """Leaderboard (per FetchFilter) -> optionally full fetch per lead -> LeadSet (unscored, unsaved)."""
        f = filt or FetchFilter()
        if kw:
            f = f.replace(**kw)
        lb = self.fetch_leaderboard(f)
        groups = {str(it["leadPortfolioId"]): ",".join(it.get("_groups", [])) for it in lb}
        meta: dict = {"stage": "collect", "full": full, "filter": f.describe()}
        if full:
            pairs, raws, problems = self._fetch_many([(str(it["leadPortfolioId"]), it) for it in lb], "lead")
            meta.update(problems, sections=asdict(self.cfg.sections()))
            rows = [dict(r, source=groups[pid]) for pid, r in pairs]
        else:
            raws = {}
            rows = [dict(flatten_lead({"portfolioId": str(it["leadPortfolioId"]), "leaderboard": it,
                                       "timestamp": clock.now_iso(), "performance": {f.time_range: it}}),
                         source=groups[str(it["leadPortfolioId"])]) for it in lb]
        return LeadSet(order_columns(pd.DataFrame(rows)), raws, meta=meta)

    def fetch_leads(self, portfolio_ids: Iterable[str]) -> LeadSet:
        ids = list(dict.fromkeys(str(p) for p in portfolio_ids))
        pairs, raws, problems = self._fetch_many([(pid, None) for pid in ids], "lead")
        return LeadSet(order_columns(pd.DataFrame([r for _, r in pairs])), raws,
                       meta={"stage": "fetch_leads", "ids": ids, **problems, "sections": asdict(self.cfg.sections())})

    # ---------------------------------------------------------- two-stage pipeline
    def batch_collect(self, pages: int = 167, sort: str = "AUM", portfolio_type: str = "ALL", source: str = "all",
                      hide_full: bool = False, ranges: Sequence[str] = TIME_RANGES, stop_when: dict | None = None,
                      checkpoint: bool = False, date: str | None = None, checkpoint_every: int = 20) -> dict:
        """Stage 1: leaderboard pages for every timeRange (5 requests per 30 leads), no per-lead requests.

        sort         AUM | COPY_COUNT (range-independent, so pages line up across ranges)
        source       "all" | "ai" (Binance Smart Filter subset)
        stop_when    early stop on the sorted key, e.g. {"aum": 10000} -> stop paging once a page's last
                     lead has aum < 10000 (sort=AUM) or {"currentCopyCount": 5} (sort=COPY_COUNT)
        checkpoint   write data/batch/<date>.partial.json.gz every N pages and resume from it next run
        Returns merged {pid: {leaderboard, performance{tr}, chart365, chart_start, chart_range, timestamp}} — NOT saved.
        `timestamp` = when the lead was first seen, so ages stay correct when the batch is re-read later.
        """
        assert sort in self.BATCH_SORTS, f"batch sort must be one of {self.BATCH_SORTS}"
        assert source in ("all", "ai")
        date = date or clock.today()
        merged: dict[str, dict] = {}
        done: set[str] = set()
        if checkpoint:
            part = self.store.load_batch(date, partial=True)
            if part and "_merged" in part:
                merged, done = part["_merged"], set(part.get("_done", []))
                log.info("batch resume: %d leads, %d (range,page) done", len(merged), len(done))
        stop_key, stop_val = (next(iter(stop_when.items())) if stop_when else (None, None))
        n_since = 0
        for tr in ranges:
            if f"{tr}:end" in done:
                continue
            for page in range(1, pages + 1):
                key = f"{tr}:{page}"
                if key in done:
                    continue
                data = self.client.query_list(page=page, time_range=tr, sort=sort, portfolio_type=portfolio_type,
                                              hide_full=hide_full, ai_recommended=(source == "ai"))
                items = data.get("list") or []
                seen = clock.now_iso()
                for it in items:
                    pid = str(it["leadPortfolioId"])
                    m = merged.setdefault(pid, {"portfolioId": pid, "leaderboard": {}, "performance": {},
                                                "timestamp": seen})
                    lb = {k: v for k, v in it.items() if k != "chartItems"}
                    if not m["leaderboard"] or tr == "30D":
                        m["leaderboard"] = lb
                    m["performance"][tr] = {k: it.get(k) for k in ("roi", "pnl", "mdd", "winRate", "sharpRatio", "copierPnl")}
                    m["performance"][tr]["timeRange"] = tr
                    if tr == "365D" or "chart365" not in m:
                        pts = it.get("chartItems") or []
                        if pts:
                            m["chart365"] = [round(_f(c.get("value")) or 0.0, 4) for c in pts]
                            m["chart_start"] = pts[0].get("dateTime")
                            m["chart_range"] = tr
                done.add(key)
                n_since += 1
                log.info("batch %s page %d/%d: %d rows (total %s) leads=%d", tr, page, pages, len(items), data.get("total"), len(merged))
                if checkpoint and n_since >= checkpoint_every:
                    self.store.save_batch({"_merged": merged, "_done": sorted(done)}, date, partial=True)
                    n_since = 0
                last_val = _f(items[-1].get(stop_key)) if (items and stop_key) else None
                stop = stop_key is not None and last_val is not None and last_val < stop_val
                if not items or page * 30 >= int(data.get("total") or 0) or page == pages or stop:
                    done.add(f"{tr}:end")
                    if stop:
                        log.info("batch %s: early stop at page %d (%s=%s < %s)", tr, page, stop_key, last_val, stop_val)
                    break
        if checkpoint:
            part = self.store.batch_dir / f"{date}.partial.json.gz"
            if part.exists():
                part.unlink()
        log.info("batch complete: %d leads (not saved — call save_batch())", len(merged))
        return merged

    batch_to_raw = staticmethod(batch_to_raw)   # kept on the class for backwards compatibility

    def batch_frame(self, merged: dict) -> pd.DataFrame:
        """Flat (unscored) DataFrame for every lead in a batch — stage-1 data only."""
        rows = [flatten_lead(self.batch_to_raw(m)) for m in merged.values()]
        return order_columns(pd.DataFrame(rows))

    def batch_leadset(self, merged: dict) -> LeadSet:
        raws = {pid: self.batch_to_raw(m) for pid, m in merged.items()}
        return LeadSet(self.batch_frame(merged), raws, meta={"stage": "batch"})

    def enrich(self, merged: dict, ids: Iterable[str], history_pages: int = 1, transfers: bool = False,
               copiers: bool = False, positions: bool = False, resume_from: LeadSet | None = None,
               sections: Sections | None = None, checkpoint: bool = False, checkpoint_every: int = 25,
               date: str | None = None) -> LeadSet:
        """Stage 2: per-lead detail + `sections` for `ids` only (performance / chart come from the batch).

        sections        what to fetch; the history_pages/transfers/copiers/positions flags are the short form
        resume_from     a LeadSet (e.g. today's saved day): leads already holding every requested section are reused
        checkpoint      keep data/partial/<date>.enrich.json.gz up to date every checkpoint_every leads and resume
                        from it after a crash; LocalStore.clear_partial(date) removes it (the CLI does after saving)
        meta records failed {pid: error}, closed [pid] and the sections used.
        """
        s = sections or Sections(history_pages=history_pages, transfers=transfers, copiers=copiers, positions=positions)
        date = date or clock.today()
        c = self.client
        ids = [str(i) for i in dict.fromkeys(ids)]
        previous: dict = {}
        if checkpoint:
            part = self.store.load_partial(date)
            if part:
                previous.update(part.get("raws") or {})
                log.info("enrich resume from checkpoint: %d leads", len(previous))
        rows, raws, failed, closed = [], {}, {}, []
        n_new = 0
        for i, pid in enumerate(ids, 1):
            prev = previous.get(pid) or (resume_from.raw(pid) if resume_from else None)
            if has_sections(prev, s):
                raws[pid] = prev
                rows.append(flatten_lead(prev))
                continue
            m = dict(merged.get(pid) or {"portfolioId": pid, "leaderboard": {}, "performance": {}})
            log.info("enrich %d/%d %s %s", i, len(ids), pid, (m.get("leaderboard") or {}).get("nickname"))
            try:
                raw = self.batch_to_raw(dict(m, timestamp=clock.now_iso()))
                raw["detail"] = c.detail(pid)
                self._fetch_sections(pid, raw, s)
            except PortfolioClosedError:
                closed.append(pid)
                continue
            except BinanceCopyError as e:
                failed[pid] = str(e)[:200]
                log.error("enrich %s failed: %s", pid, e)
                continue
            raws[pid] = raw
            rows.append(flatten_lead(raw))
            n_new += 1
            if checkpoint and n_new % checkpoint_every == 0:
                self.store.save_partial(date, {"raws": {**previous, **raws}})
        if checkpoint and n_new:
            self.store.save_partial(date, {"raws": {**previous, **raws}})
        return LeadSet(order_columns(pd.DataFrame(rows)), raws, date=date,
                       meta={"stage": "enrich", "historyPages": s.history_pages, "historyDays": s.history_days,
                             "transfers": s.transfers, "copiers": s.copiers, "positions": s.positions,
                             "orders": s.orders, "linked": s.linked, "pnlChart": s.pnl_chart,
                             "sections": asdict(s), "failed": failed, "closed": closed})

    def pipeline(self, filt: "FetchFilter | None" = None, pages: int = 167, sort: str = "AUM", source: str = "all",
                 stop_when: dict | None = None, history_pages: int = 1, transfers: bool = False, copiers: bool = False,
                 batch: dict | None = None, resume_from: LeadSet | None = None, checkpoint: bool = False,
                 positions: bool = False, sections: Sections | None = None, date: str | None = None) -> LeadSet:
        """batch_collect (or reuse `batch`) -> FetchFilter.keep on stage-1 rows -> enrich. Nothing saved, nothing scored.
        checkpoint applies to both stages."""
        f = filt or FetchFilter()
        date = date or clock.today()
        merged = batch or self.batch_collect(pages=pages, sort=sort, portfolio_type=f.portfolio_type, source=source,
                                             hide_full=f.hide_full, stop_when=stop_when, checkpoint=checkpoint,
                                             date=date)
        cands = [pid for pid, m in merged.items() if f.keep(self._batch_keep_row(m))]
        if f.limit:
            cands = cands[: f.limit]
        log.info("pipeline: %d leads in batch, %d candidates after filter (%s)", len(merged), len(cands), f.describe())
        ls = self.enrich(merged, cands, history_pages=history_pages, transfers=transfers, copiers=copiers,
                         positions=positions, resume_from=resume_from, sections=sections, checkpoint=checkpoint,
                         date=date)
        if not ls.df.empty:
            ls.df["source"] = f"batch_{sort}" + ("_ai" if source == "ai" else "")
        ls.meta.update({"stage": "pipeline", "filter": f.describe(), "sort": sort, "source": source,
                        "batchLeads": len(merged), "candidates": len(cands)})
        ls.batch = merged  # type: ignore[attr-defined]  (kept so the caller can save_batch() too)
        return ls

    @staticmethod
    def _batch_keep_row(m: dict) -> dict:
        """Leaderboard-shaped row for FetchFilter.keep(): 30D values + copierPnl."""
        lb = dict(m.get("leaderboard") or {})
        p30 = (m.get("performance") or {}).get("30D") or {}
        for k in ("roi", "pnl", "mdd", "winRate", "sharpRatio", "copierPnl"):
            if p30.get(k) is not None:
                lb[k] = p30[k]
        lb.setdefault("leadPortfolioId", m.get("portfolioId"))
        return lb

    # ---------------------------------------------------------- storage (explicit)
    def save(self, result: "LeadSet | pd.DataFrame", fmt: str = "csv", date: str | None = None) -> Path:
        """Persist a LeadSet (or a bare DataFrame) as csv tables or json.gz under data/daily/<date>/."""
        ls = result if isinstance(result, LeadSet) else LeadSet(result)
        return self.store.save_leadset(ls, fmt=fmt, date=date)

    def save_batch(self, merged: dict, fmt: str = "json", date: str | None = None) -> Path:
        return self.store.save_batch(merged, date=date, fmt=fmt)

    def load(self, date: str | None = None, raws: bool = True) -> LeadSet | None:
        """Saved day -> LeadSet. raws=False skips the (large) raw payloads."""
        return self.store.load_leadset(date, raws=raws)

    def days(self) -> list[str]:
        return self.store.list_days()

    def latest(self) -> pd.DataFrame:
        ls = self.store.load_leadset(raws=False)
        return ls.df if ls else pd.DataFrame()

    def history(self, portfolio_id: str) -> pd.DataFrame:
        return self.store.history(portfolio_id)

    def check(self, date: str | None = None) -> dict:
        """Data-quality report for a saved day (see LocalStore.check)."""
        return self.store.check(date)

    def diff(self, date_a: str | None = None, date_b: str | None = None,
             cols: Sequence[str] = ("roi30d", "roi7d", "mdd", "aum", "marginBalance", "currentCopiers",
                                    "copierPnl30d")) -> pd.DataFrame:
        """Compare two saved days (default: latest vs previous). Returns joined frame with *_prev / *_delta.
        Pass extra `cols` (e.g. score, grade) if your saved rows carry them."""
        dates = self.days()
        if len(dates) < 2 and not (date_a and date_b):
            return pd.DataFrame()
        date_b = date_b or dates[-1]
        date_a = date_a or dates[-2]
        a, b = self.load(date_a, raws=False).df, self.load(date_b, raws=False).df
        cols = [c for c in cols if c in a.columns and c in b.columns]
        a = a.set_index("portfolioId")[cols].add_suffix("_prev")
        b = b.set_index("portfolioId")[["nickname"] + cols]
        j = b.join(a, how="left")
        for c in cols:
            if pd.api.types.is_numeric_dtype(j[c]):
                j[f"{c}_delta"] = j[c] - j[f"{c}_prev"]
        j["new_entry"] = j[f"{cols[0]}_prev"].isna() if cols else False
        j.attrs["dates"] = (date_a, date_b)
        return j.reset_index()

    def rebuild(self, date: str | None = None) -> LeadSet | None:
        """Re-flatten a saved day from its raws (json format keeps full raws; csv keeps tables only)."""
        ls = self.load(date)
        if ls is None:
            return None
        if ls.raws and all((r.get("detail") or r.get("performance")) for r in ls.raws.values()):
            df = order_columns(pd.DataFrame([flatten_lead(r) for r in ls.raws.values()]))
            if "source" in ls.df.columns:
                df = df.merge(ls.df[["portfolioId", "source"]], on="portfolioId", how="left")
            ls = LeadSet(df, ls.raws, ls.date, dict(ls.meta, rebuilt=True))
        return ls

    _order_columns = staticmethod(order_columns)


__all__ = ["CopyTradeService", "ServiceConfig", "Sections", "has_sections"]
