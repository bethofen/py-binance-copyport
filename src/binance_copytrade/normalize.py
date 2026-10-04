"""Raw Binance payloads -> one flat row per lead.

Raw layout (what fetch_lead / enrich / batch_to_raw produce and LocalStore persists):
    {portfolioId, timestamp (ISO, fetch time), leaderboard{}, detail{}, performance{7D..365D},
     chart{timeRange, dataType: ROI, data:[{value, dateTime}]}, chartPnl{... dataType: PNL}, coinPreference{},
     positionHistory[], transferHistory[], copyTraders[], positions[], orderHistory[], linkedPortfolios{},
     positionsPrivate (lead hides positions: open book unknown), _fetch{section: {n,total,pages,oldest,truncated}},
     _errors{section: message}}
A section that was fetched but empty is kept as [] ("nothing there"); a missing key means "not fetched".
All derived ages use the payload's own `timestamp`, so re-reading an old snapshot gives the same row.
"""
from __future__ import annotations

import pandas as pd

from . import clock
from .client import _f

DAY_MS = 86_400_000
RAW_SECTIONS = ("positionHistory", "transferHistory", "copyTraders", "positions", "orderHistory")
ID_COLUMNS = ("portfolioId", "ownerKey", "linkedPortfolioIds")   # 19-digit ids: always str, never float
EXTRA_KEYS = ("linkedPortfolios", "chartPnl", "positionsPrivate", "_fetch", "_errors")   # optional raw keys
LEADERBOARD_KEYS = ("leadPortfolioId", "nickname", "currentCopyCount", "maxCopyCount", "aum", "startTime",
                    "portfolioType", "badgeName", "apiKeyTag", "tradFiTag", "aiSummary", "sharpRatio", "copierPnl")
CORE_COLUMNS = [
    "nickname", "portfolioId", "roi7d", "roi30d", "roi90d", "roi180d", "roi365d",
    "pnl", "aum", "mdd", "winRate", "sharpeRatio", "currentCopiers", "maxCopiers",
    "minMargin", "timestamp",
]


def flatten_lead(raw: dict) -> dict:
    """Turn one full lead payload (detail + performance{...} + history ...) into a flat row."""
    d = raw.get("detail") or {}
    lb = raw.get("leaderboard") or {}
    perf = raw.get("performance") or {}
    p30 = perf.get("30D") or {}
    p7 = perf.get("7D") or {}
    now_ms = clock.iso_ms(raw.get("timestamp")) or clock.now_ms()   # point-in-time: age as of the fetch, not as of re-reading
    start = d.get("startTime") or lb.get("startTime")

    row = {
        "nickname": d.get("nickname") or lb.get("nickname"),
        "nicknameTranslate": d.get("nicknameTranslate"),
        "portfolioId": str(raw.get("portfolioId")),
        "roi7d": _f(p7.get("roi")),
        "roi30d": _f(p30.get("roi")),
        "roi90d": _f((perf.get("90D") or {}).get("roi")),
        "roi180d": _f((perf.get("180D") or {}).get("roi")),
        "roi365d": _f((perf.get("365D") or {}).get("roi")),
        "pnl": _f(p30.get("pnl")),
        "aum": _f(d.get("aumAmount")) if d else _f(lb.get("aum")),
        "mdd": _f(p30.get("mdd")),
        "winRate": _f(p30.get("winRate")),
        "sharpeRatio": _f(d.get("sharpRatio")) if d else _f(lb.get("sharpRatio")),
        "currentCopiers": d.get("currentCopyCount", lb.get("currentCopyCount")),
        "maxCopiers": d.get("maxCopyCount", lb.get("maxCopyCount")),
        "minMargin": None,
        "minMarginFixedRatio": _f(d.get("fixedRadioMinCopyUsd")),
        "minMarginFixedAmount": _f(d.get("fixedAmountMinCopyUsd")),
        "timestamp": raw.get("timestamp") or clock.now_iso(),
        # --- detail extras
        "status": d.get("status"),
        "portfolioType": d.get("portfolioType") or lb.get("portfolioType"),
        "badge": d.get("badgeName") or lb.get("badgeName"),
        "marginBalance": _f(d.get("marginBalance")),
        "profitSharingRate": _f(d.get("profitSharingRate")),
        "copierPnlTotal": _f(d.get("copierPnl")),
        "copierPnl30d": _f(p30.get("copierPnl")),
        "totalCopiers": d.get("totalCopyCount"),
        "closedPortfolios": d.get("closeLeadCount"),
        "mockCopiers": d.get("mockCopyCount"),
        "favoriteCount": d.get("favoriteCount"),
        "lockPeriod": d.get("lockPeriod"),
        "positionShow": d.get("positionShow"),
        "tags": ",".join(d.get("tag") or []),
        "leverageTag": next((t for t in d.get("tag") or [] if "LEVERAGE" in t), None),
        "apiKeyTag": lb.get("apiKeyTag") or ("API_KEY_TRADE" if "API_KEY_TRADE" in (d.get("tag") or []) else None),
        "tradFiTag": lb.get("tradFiTag"),
        "aiSummary": d.get("aiSummary") or lb.get("aiSummary"),
        "startTime": start,
        "lastTradeTime": d.get("lastTradeTime"),
        "daysTrading": round((now_ms - start) / 86_400_000) if start else None,
        "daysSinceLastTrade": round((now_ms - d["lastTradeTime"]) / 86_400_000, 1) if d.get("lastTradeTime") else None,
    }
    mins = [v for v in (row["minMarginFixedRatio"], row["minMarginFixedAmount"]) if v is not None]
    row["minMargin"] = min(mins) if mins else None

    for tr, v in perf.items():
        k = tr.lower()
        row[f"pnl_{k}"] = _f(v.get("pnl"))
        row[f"mdd_{k}"] = _f(v.get("mdd"))
        row[f"winRate_{k}"] = _f(v.get("winRate"))
        row[f"winOrders_{k}"] = v.get("winOrders")
        row[f"totalOrders_{k}"] = v.get("totalOrder")
        row[f"copierPnl_{k}"] = _f(v.get("copierPnl"))

    # implied starting equity: pnl / roi
    for k in ("365d", "90d", "30d"):
        pnl_k, roi_k = row.get(f"pnl_{k}"), row.get(f"roi{k}")
        if pnl_k and roi_k:
            row["impliedStartEquity"] = round(abs(pnl_k / roi_k * 100), 2)
            break

    hist = raw.get("positionHistory") or []
    if hist:
        lev = [_f(h.get("leverage")) for h in hist if h.get("leverage") is not None]
        hold = [(h["closed"] - h["opened"]) / 3_600_000 for h in hist if h.get("closed") and h.get("opened")]
        wins = [h for h in hist if (_f(h.get("closingPnl")) or 0) > 0]
        losses = [h for h in hist if (_f(h.get("closingPnl")) or 0) < 0]
        row["histPositions"] = len(hist)
        row["histAvgLeverage"] = round(sum(lev) / len(lev), 2) if lev else None
        row["histMaxLeverage"] = max(lev) if lev else None
        row["histAvgHoldHours"] = round(sum(hold) / len(hold), 1) if hold else None
        row["histWinRate"] = round(100 * len(wins) / len(hist), 2)
        avg_win = sum(_f(h["closingPnl"]) for h in wins) / len(wins) if wins else 0
        avg_loss = abs(sum(_f(h["closingPnl"]) for h in losses) / len(losses)) if losses else 0
        row["histAvgWin"] = round(avg_win, 2)
        row["histAvgLoss"] = round(avg_loss, 2)
        row["histPayoff"] = round(avg_win / avg_loss, 2) if avg_loss else None
        row["histWorstTrade"] = round(min((_f(h.get("closingPnl")) or 0) for h in hist), 2)
        row["histSymbols"] = ",".join(sorted({h.get("symbol") for h in hist if h.get("symbol")})[:25])
        row["histLongShare"] = round(100 * sum(1 for h in hist if h.get("side") == "Long") / len(hist), 1)

    tx = raw.get("transferHistory") or []
    if tx:
        row["leadDeposits"] = sum(_f(t.get("amount")) or 0 for t in tx if t.get("transType") in ("LEAD_DEPOSIT", "LEAD_INVEST"))
        row["leadWithdrawals"] = sum(_f(t.get("amount")) or 0 for t in tx if t.get("transType") == "LEAD_WITHDRAW")

    coins = ((raw.get("coinPreference") or {}).get("data") or [])
    row["topAssets"] = ",".join(f"{c['asset']}:{c['volume']}%" for c in coins[:5])
    row["assetCount"] = len(coins)
    row["positionsPrivate"] = bool(raw.get("positionsPrivate") or d.get("positionShow") is False)
    row["openPositions"] = None if raw.get("positionsPrivate") or raw.get("positions") is None else len(
        [p for p in raw["positions"] if _f(p.get("positionAmount"))])

    fetch_info = raw.get("_fetch") or {}
    ts_ms = clock.iso_ms(raw.get("timestamp")) or clock.now_ms()
    for section, prefix in (("positionHistory", "hist"), ("transferHistory", "transfers"), ("orderHistory", "orders")):
        fi = fetch_info.get(section) or {}
        if fi:
            row[f"{prefix}Truncated"] = bool(fi.get("truncated"))
            if fi.get("oldest"):
                row[f"{prefix}CoverDays"] = round((ts_ms - fi["oldest"]) / 86_400_000, 1)
    if raw.get("transferHistory") is not None:
        row["transfersN"] = len(raw["transferHistory"])

    orders = raw.get("orderHistory") or []
    if orders:
        times = [_f(o.get("orderTime")) for o in orders if o.get("orderTime")]
        span = max(1.0, (max(times) - min(times)) / 86_400_000) if times else None
        types = [str(o.get("type") or "").upper() for o in orders]
        row["ordersN"] = len(orders)
        row["ordersPerDay"] = round(len(orders) / span, 2) if span else None
        row["ordersLimitShare"] = round(100 * sum(t == "LIMIT" for t in types) / len(types), 1)
        row["ordersMarketShare"] = round(100 * sum(t == "MARKET" for t in types) / len(types), 1)
        row["ordersSymbols"] = len({o.get("symbol") for o in orders})

    linked = raw.get("linkedPortfolios") or {}
    if linked:
        ids = {str(v) for k, v in linked.items() if k.endswith("LPId") and v}
        active = {str(v) for k, v in linked.items()
                  if k.endswith("LPId") and v and str(linked.get(k[:-2] + "Status") or "").upper() == "ACTIVE"}
        own = row["portfolioId"]
        row["linkedPortfolioIds"] = ",".join(sorted(ids - {own}))
        row["ownerActivePortfolios"] = len(active | {own})
        row["ownerKey"] = min(ids | {own})              # same value for every portfolio of one owner

    pnl = [_f(c.get("value")) for c in ((raw.get("chartPnl") or {}).get("data") or []) if c.get("value") is not None]
    if pnl:
        row["chartPnlFinal"] = round(pnl[-1], 2)
        row["chartPnlMax"] = round(max(pnl), 2)
        row["chartPnlDays"] = len(pnl)
    if raw.get("_errors"):
        row["sectionErrors"] = ",".join(sorted(raw["_errors"]))

    chart = (raw.get("chart") or {}).get("data") or []
    if chart and start:
        chart = [c for c in chart if (c.get("dateTime") or 0) >= start - 86_400_000]
    if chart:
        vals = [_f(c.get("value")) for c in chart if c.get("value") is not None]
        if len(vals) > 2:
            # daily changes of cumulative ROI → rough volatility / worst day
            deltas = [b - a for a, b in zip(vals, vals[1:])]
            row["chartDays"] = len(vals)
            row["chartWorstDay"] = round(min(deltas), 2)
            row["chartBestDay"] = round(max(deltas), 2)
            row["chartPosDays"] = round(100 * sum(1 for x in deltas if x > 0) / len(deltas), 1)

    copiers = raw.get("copyTraders") or []
    if copiers:
        rois = [_f(c.get("totalRoi")) for c in copiers if c.get("totalRoi") is not None]
        row["topCopiersAvgRoi"] = round(sum(rois) / len(rois), 2) if rois else None
    return row


def batch_to_raw(m: dict) -> dict:
    """Convert one merged batch entry into the raw layout flatten_lead() understands."""
    pts = m.get("chart365") or []
    start = m.get("chart_start") or 0
    chart = [{"value": v, "dateTime": start + i * DAY_MS} for i, v in enumerate(pts)]
    raw = {"portfolioId": m["portfolioId"], "timestamp": m.get("timestamp") or clock.now_iso(),
           "leaderboard": m.get("leaderboard") or {}, "detail": m.get("detail") or {},
           "performance": m.get("performance") or {},
           "chart": {"timeRange": m.get("chart_range", "365D"), "dataType": "ROI", "data": chart}}
    for k in (*RAW_SECTIONS, *EXTRA_KEYS):
        if m.get(k) is not None:      # keep [] : "fetched, nothing there" differs from "not fetched"
            raw[k] = m[k]
    return raw


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    first = [c for c in CORE_COLUMNS if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    return df[first + rest]
