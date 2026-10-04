"""binance-copytrade CLI — fetch, inspect and store lead data. Prints; writes only with --save / --save-batch.

    binance-copytrade collect --source ai --sort AUM --pages 2 --save json
    binance-copytrade batch --pages 167 --save json                       # stage 1, whole market
    binance-copytrade enrich --min-days 30 --max-mdd 40 --min-aum 10000 --history-pages 4 --transfers --positions --save json
    binance-copytrade pipeline ... (batch + enrich in one go)
    binance-copytrade lead <portfolioId> ... --save json
    binance-copytrade days | options | history <id> | diff | rebuild
(or: python -m binance_copytrade ...)
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from pathlib import Path

import pandas as pd

from . import clock
from .client import BinanceCopyClient, PORTFOLIO_TYPES, SORT_TYPES, TIME_RANGES
from .filters import BADGES, RECOMMENDED_GROUPS, SOURCES, FetchFilter
from .leadset import LeadSet
from .service import CopyTradeService, Sections, ServiceConfig

SUMMARY_COLUMNS = ["nickname", "portfolioId", "roi7d", "roi30d", "roi90d", "roi365d", "mdd", "winRate", "aum",
                   "marginBalance", "currentCopiers", "maxCopiers", "minMargin", "daysTrading", "histAvgLeverage", "status"]


def summary(df: "pd.DataFrame | LeadSet", top: int = 25, sort_by: str = "aum") -> str:
    """Markdown table of the core data columns (no scoring — that belongs to the analysis layer)."""
    if isinstance(df, LeadSet):
        df = df.df
    if df is None or df.empty:
        return "_no data_"
    d = df.sort_values(sort_by, ascending=False) if sort_by in df else df
    cols = [c for c in SUMMARY_COLUMNS if c in d.columns]
    d = d[cols].head(top).copy()
    for c in cols:
        if pd.api.types.is_float_dtype(d[c]):
            fmt = "{:,.0f}" if c in ("aum", "marginBalance", "minMargin") else "{:,.1f}"
            d[c] = d[c].map(lambda x: "" if x is None or (isinstance(x, float) and math.isnan(x)) else fmt.format(x))
    lines = [f"# Leads — {len(df)} rows (top {min(top, len(df))} by {sort_by})", "",
             "| " + " | ".join(d.columns) + " |", "|" + "---|" * len(d.columns)]
    lines += ["| " + " | ".join(str(v) for v in r.values) + " |" for _, r in d.iterrows()]
    return "\n".join(lines)


def _filter_from_args(a, server: bool = True) -> "FetchFilter":
    kw = dict(
        min_days=a.min_days, max_days=a.max_days, min_aum=a.min_aum, max_aum=a.max_aum,
        min_copiers=a.min_copiers, max_copiers=a.max_copiers, min_roi=a.min_roi, max_roi=a.max_roi,
        max_mdd=a.max_mdd, min_win_rate=a.min_win_rate, min_sharpe=a.min_sharpe, min_copier_pnl=a.min_copier_pnl,
        badges=tuple(a.badges.split(",")) if a.badges else None, exclude_full=a.exclude_full, limit=a.limit,
        exclude_ids=tuple(a.exclude_ids.split(",")) if getattr(a, "exclude_ids", None) else (),
        include_ids=tuple(a.include_ids.split(",")) if getattr(a, "include_ids", None) else (),
        require_api_key=getattr(a, "require_api_key", None), require_tradfi=getattr(a, "require_tradfi", None),
    )
    if server:
        kw.update(source=a.source, sort=a.sort, time_range=a.time_range, order=a.order, portfolio_type=a.portfolio_type,
                  hide_full=a.hide_full, nickname=a.nickname, pages=a.pages,
                  groups=tuple(a.groups.split(",")) if a.groups else None)
    elif hasattr(a, "portfolio_type"):
        kw.update(portfolio_type=a.portfolio_type, hide_full=getattr(a, "hide_full", False))
    return FetchFilter(**kw)


def _add_client_filter_args(p):
    g = p.add_argument_group("client-side pre-filters (no extra requests; stage-1 / 30D values)")
    g.add_argument("--min-days", type=int); g.add_argument("--max-days", type=int)
    g.add_argument("--min-aum", type=float); g.add_argument("--max-aum", type=float)
    g.add_argument("--min-copiers", type=int); g.add_argument("--max-copiers", type=int)
    g.add_argument("--min-roi", type=float); g.add_argument("--max-roi", type=float)
    g.add_argument("--max-mdd", type=float)
    g.add_argument("--min-win-rate", type=float); g.add_argument("--min-sharpe", type=float)
    g.add_argument("--min-copier-pnl", type=float)
    g.add_argument("--badges", help="comma list of %s" % ",".join(BADGES))
    g.add_argument("--api-key", dest="require_api_key", action="store_const", const=True)
    g.add_argument("--no-api-key", dest="require_api_key", action="store_const", const=False)
    g.add_argument("--tradfi", dest="require_tradfi", action="store_const", const=True)
    g.add_argument("--no-tradfi", dest="require_tradfi", action="store_const", const=False)
    g.add_argument("--exclude-full", action="store_true")
    g.add_argument("--exclude-ids", help="comma list"); g.add_argument("--include-ids", help="comma list")
    g.add_argument("--limit", type=int)


def _add_save_arg(p):
    p.add_argument("--save", choices=("csv", "json"), help="persist under data/daily/<date>/ (default: don't save)")
    p.add_argument("--date", help="override snapshot date (YYYY-MM-DD)")
    p.add_argument("--top", type=int, default=25)


def _add_section_args(p, default_history_pages: int = 1):
    g = p.add_argument_group("per-lead sections (what to fetch besides detail)")
    g.add_argument("--history-pages", type=int, default=default_history_pages, help="cap, 100 positions per page")
    g.add_argument("--history-days", type=int, help="page position history back this many days (within the cap)")
    g.add_argument("--transfers", action="store_true", help="all lead deposits/withdrawals")
    g.add_argument("--positions", action="store_true", help="open positions (private leads are marked, not fetched)")
    g.add_argument("--copiers", action="store_true", help="top 30 copiers")
    g.add_argument("--orders", action="store_true", help="individual orders (Latest Records)")
    g.add_argument("--order-days", type=int, default=30)
    g.add_argument("--linked", action="store_true", help="the owner's other portfolios")
    g.add_argument("--pnl-chart", action="store_true", help="daily PnL (USDT) curve")
    g.add_argument("--all-sections", action="store_true",
                   help="= --history-days 90 --history-pages 30 --transfers --positions --copiers --orders --linked --pnl-chart")


def _sections_from_args(a) -> Sections:
    if a.all_sections:
        return Sections.full()
    return Sections(history_days=a.history_days, history_pages=a.history_pages, transfers=a.transfers,
                    positions=a.positions, copiers=a.copiers, orders=a.orders, order_days=a.order_days,
                    linked=a.linked, pnl_chart=a.pnl_chart)


def _add_output_args(p):
    p.add_argument("--out", help="also write the flat rows to this CSV")
    p.add_argument("--json", help="also write the raw payloads to this JSON file")


def main(argv=None, client: BinanceCopyClient | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):  # Windows cp1252 consoles crash on CJK nicknames
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(prog="binance-copytrade",
                                 description="Binance copy-trading lead data (public endpoints; saves only with --save)")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("collect", help="leaderboard (with filters) + per-lead fetch [-> --save]")
    p.add_argument("--source", default="all", choices=SOURCES)
    p.add_argument("--sort", default="ROI", help="one of %s, comma list = union" % ",".join(SORT_TYPES))
    p.add_argument("--time-range", default="30D", choices=TIME_RANGES)
    p.add_argument("--order", default="DESC", choices=("DESC", "ASC"))
    p.add_argument("--portfolio-type", default="ALL", choices=PORTFOLIO_TYPES)
    p.add_argument("--pages", type=int, default=2, help="30 rows per page")
    p.add_argument("--hide-full", action="store_true")
    p.add_argument("--nickname", default="")
    p.add_argument("--groups", help="recommended only: comma list of %s" % ",".join(RECOMMENDED_GROUPS))
    _add_client_filter_args(p)
    p.add_argument("--quick", action="store_true", help="leaderboard only, no per-lead fetch")
    _add_save_arg(p)
    _add_output_args(p)

    p = sub.add_parser("batch", help="stage 1: leaderboard for all ranges, 5 requests per 30 leads [-> --save]")
    p.add_argument("--pages", type=int, default=167, help="167 pages ~ 5,000 leads")
    p.add_argument("--sort", default="AUM", choices=CopyTradeService.BATCH_SORTS)
    p.add_argument("--source", default="all", choices=("all", "ai"))
    p.add_argument("--portfolio-type", default="ALL", choices=PORTFOLIO_TYPES)
    p.add_argument("--hide-full", action="store_true")
    p.add_argument("--stop-when", help="early stop, e.g. aum<10000 or currentCopyCount<5")
    p.add_argument("--checkpoint", action="store_true", help="write/resume data/batch/<date>.partial.json.gz")
    p.add_argument("--save", choices=("csv", "json"), help="persist to data/batch/")
    p.add_argument("--date")

    for name, hlp in (("enrich", "stage 2: filter a saved batch, detail+history for candidates [-> --save]"),
                      ("pipeline", "batch + enrich in one go [-> --save]")):
        p = sub.add_parser(name, help=hlp)
        if name == "enrich":
            p.add_argument("--batch-date")
            p.add_argument("--resume", action="store_true",
                           help="reuse leads already saved for the same date (only if fetched with the same sections)")
        else:
            p.add_argument("--pages", type=int, default=167)
            p.add_argument("--sort", default="AUM", choices=CopyTradeService.BATCH_SORTS)
            p.add_argument("--source", default="all", choices=("all", "ai"))
            p.add_argument("--portfolio-type", default="ALL", choices=PORTFOLIO_TYPES)
            p.add_argument("--hide-full", action="store_true")
            p.add_argument("--stop-when")
            p.add_argument("--checkpoint", action="store_true")
            p.add_argument("--save-batch", choices=("csv", "json"), help="also persist stage-1 batch")
        if name == "enrich":
            p.add_argument("--checkpoint", action="store_true",
                           help="save progress to data/partial/<date>.enrich.json.gz and resume from it")
        _add_section_args(p)
        _add_client_filter_args(p)
        _add_save_arg(p)

    p = sub.add_parser("lead", help="full fetch for given portfolio ids [-> --save]")
    p.add_argument("ids", nargs="*")
    p.add_argument("--from-csv", help="CSV with a portfolioId column")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--chart-range", default="365D", choices=TIME_RANGES)
    p.add_argument("--no-extras", action="store_true", help="skip transfers, positions, copiers, linked, PnL chart")
    _add_section_args(p, default_history_pages=2)
    _add_save_arg(p)
    _add_output_args(p)

    p = sub.add_parser("history", help="one lead across all saved days"); p.add_argument("id")
    p = sub.add_parser("diff", help="compare latest vs previous saved day")
    p.add_argument("--from", dest="date_a"); p.add_argument("--to", dest="date_b")
    p.add_argument("--cols", help="comma list of columns to compare")
    sub.add_parser("days", help="list saved days and batches")
    p = sub.add_parser("check", help="data-quality report for a saved day (coverage, truncation, failures)")
    p.add_argument("--date")
    sub.add_parser("options", help="print every filter/sort/source choice")
    p = sub.add_parser("rebuild", help="re-flatten a saved day from its raws (json format) [-> --save]")
    _add_save_arg(p)

    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = ServiceConfig(data_dir=a.data_dir, delay=a.delay)
    if a.cmd == "lead":
        cfg.history_pages, cfg.history_days, cfg.chart_range = a.history_pages, a.history_days, a.chart_range
        cfg.include_orders, cfg.order_days = a.orders or a.all_sections, a.order_days
        if a.all_sections:
            cfg.history_days, cfg.history_pages = 90, 30
        if a.no_extras:
            cfg.include_transfers = cfg.include_positions = cfg.include_copiers = False
            cfg.include_linked = cfg.include_pnl_chart = False
    svc = CopyTradeService(cfg, client=client)
    pd.set_option("display.width", 250, "display.max_columns", 40, "display.max_rows", 200)

    def _stop(s):
        if not s:
            return None
        k, v = s.split("<")
        return {k.strip(): float(v)}

    def _finish(ls: LeadSet):
        # save before printing so a console error can never lose a long fetch
        saved = svc.save(ls, fmt=a.save, date=a.date) if getattr(a, "save", None) else None
        if saved:
            svc.store.clear_partial(ls.date)          # the checkpoint is no longer needed once the day is saved
        if getattr(a, "out", None):
            ls.df.to_csv(a.out, index=False, encoding="utf-8-sig")
        if getattr(a, "json", None):
            Path(a.json).write_text(json.dumps(list(ls.raws.values()), ensure_ascii=False, indent=1), encoding="utf-8")
        print(summary(ls, top=a.top))
        failed, closed = (ls.meta or {}).get("failed") or {}, (ls.meta or {}).get("closed") or []
        if failed or closed:
            print(f"closed portfolios: {len(closed)} | failed fetches: {len(failed)}"
                  + (" -> " + ", ".join(list(failed)[:10]) if failed else ""))
        print("saved:", saved) if saved else print("(not saved — add --save csv|json)")

    if a.cmd == "collect":
        _finish(svc.collect(_filter_from_args(a), full=not a.quick))
    elif a.cmd == "batch":
        merged = svc.batch_collect(pages=a.pages, sort=a.sort, source=a.source, portfolio_type=a.portfolio_type,
                                   hide_full=a.hide_full, stop_when=_stop(a.stop_when), checkpoint=a.checkpoint, date=a.date)
        print(f"batch: {len(merged)} leads")
        if a.save:
            print("saved:", svc.save_batch(merged, fmt=a.save, date=a.date))
        else:
            print("(not saved — add --save csv|json)")
    elif a.cmd == "enrich":
        merged = svc.store.load_batch(a.batch_date)
        if not merged:
            print("no saved batch found — run `batch --save ...` first"); return 1
        prev = svc.load(a.date or clock.today()) if a.resume else None
        _finish(svc.pipeline(_filter_from_args(a, server=False), batch=merged, sections=_sections_from_args(a),
                             resume_from=prev, checkpoint=a.checkpoint, date=a.date))
    elif a.cmd == "pipeline":
        ls = svc.pipeline(_filter_from_args(a, server=False), pages=a.pages, sort=a.sort, source=a.source,
                          stop_when=_stop(a.stop_when), sections=_sections_from_args(a), checkpoint=a.checkpoint,
                          date=a.date)
        if a.save_batch:
            print("saved batch:", svc.save_batch(getattr(ls, "batch", {}), fmt=a.save_batch, date=a.date))
        _finish(ls)
    elif a.cmd == "lead":
        ids = list(a.ids)
        if a.from_csv:
            ids += pd.read_csv(a.from_csv, dtype={"portfolioId": str})["portfolioId"].dropna().astype(str).tolist()
        if a.limit:
            ids = ids[: a.limit]
        if not ids:
            ap.error("give portfolio ids or --from-csv")
        _finish(svc.fetch_leads(ids))
    elif a.cmd == "history":
        h = svc.history(a.id)
        cols = [c for c in ("snapshotDate", "roi7d", "roi30d", "roi90d", "mdd", "aum", "marginBalance",
                            "currentCopiers", "copierPnl30d", "status") if c in h.columns]
        print(h[cols].to_string(index=False) if not h.empty else "no history")
    elif a.cmd == "diff":
        d = svc.diff(a.date_a, a.date_b, **({"cols": tuple(a.cols.split(","))} if a.cols else {}))
        if d.empty:
            print("need at least two saved days")
        else:
            print("dates:", d.attrs.get("dates"))
            show = [c for c in d.columns if c.endswith("_delta") or c in ("portfolioId", "nickname", "new_entry")]
            print(d[show].to_string(index=False))
    elif a.cmd == "days":
        print("daily:", ", ".join(svc.days()) or "none")
        print("batch:", ", ".join(svc.store.list_batches()) or "none")
    elif a.cmd == "check":
        r = svc.check(a.date)
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
        return 1 if r.get("error") else 0
    elif a.cmd == "options":
        print(json.dumps(FetchFilter.options(), indent=1))
    elif a.cmd == "rebuild":
        ls = svc.rebuild(a.date)
        if ls is None:
            print("nothing saved yet"); return 1
        _finish(ls)
    return 0


if __name__ == "__main__":
    sys.exit(main())
