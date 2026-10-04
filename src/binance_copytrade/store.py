"""LocalStore: plain files under a data directory (csv tables or json.gz). No cloud, no database.

Every file is written atomically (temp file + rename), so a crash mid-save never leaves a half-written snapshot.
"""
from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
from typing import Callable

import pandas as pd

from . import clock
from .client import TIME_RANGES, _f
from .leadset import LeadSet
from .normalize import DAY_MS, ID_COLUMNS, RAW_SECTIONS, batch_to_raw, flatten_lead

SCHEMA_VERSION = 2   # 2: _fetch/_errors/positionsPrivate in raws, orderHistory/linkedPortfolios/chartPnl, batch timestamps

# csv table name <-> raw list section
CSV_SECTIONS = (("history", "positionHistory"), ("transfers", "transferHistory"), ("copiers", "copyTraders"),
                ("positions", "positions"), ("orders", "orderHistory"))
CSV_FILES = ("leads.csv", "chart.csv", "chart_pnl.csv", "linked.csv", *(f"{n}.csv" for n, _ in CSV_SECTIONS))
JSON_FILES = ("leads.json.gz", "raw.json.gz")


def _atomic(path: Path, write: Callable[[Path], None]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    write(tmp)
    os.replace(tmp, path)


def _write_gz_json(path: Path, obj) -> None:
    def w(p: Path):
        with gzip.open(p, "wt", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    _atomic(path, w)


def _read_gz_json(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _library_version() -> str:
    try:
        from . import __version__
        return __version__
    except ImportError:  # during package init
        return "?"


class LocalStore:
    """Plain files under data/. Two formats, chosen per save:

        csv   data/daily/<date>/leads.csv + chart.csv + chart_pnl.csv (wide, d0..dN) + history.csv + transfers.csv
              + copiers.csv + positions.csv + orders.csv + linked.csv (each only if present) + meta.json
        json  data/daily/<date>/leads.json.gz + raw.json.gz (every field, incl. _fetch/_errors) + meta.json

    Saving a day removes that day's files of the other format first (no stale mixes).
    Stage-1 batches: data/batch/<date>.json.gz  or  data/batch/<date>_leads.csv + <date>_chart.csv
    Enrich checkpoints: data/partial/<date>.enrich.json.gz
    """

    def __init__(self, root: str | Path = "data"):
        self.root = Path(root)
        self.daily_dir = self.root / "daily"
        self.batch_dir = self.root / "batch"
        self.partial_dir = self.root / "partial"

    # ------------------------------------------------------------ daily sets
    def day_dir(self, date: str) -> Path:
        return self.daily_dir / date

    def list_days(self) -> list[str]:
        if not self.daily_dir.exists():
            return []
        return sorted(p.name for p in self.daily_dir.iterdir()
                      if p.is_dir() and ((p / "leads.csv").exists() or (p / "leads.json.gz").exists()))

    def save_leadset(self, ls: LeadSet, fmt: str = "csv", date: str | None = None) -> Path:
        assert fmt in ("csv", "json"), "fmt must be 'csv' or 'json'"
        date = date or ls.date
        d = self.day_dir(date)
        d.mkdir(parents=True, exist_ok=True)
        for name in (*CSV_FILES, *JSON_FILES):          # a day holds exactly one save
            (d / name).unlink(missing_ok=True)
        df = ls.df.copy()
        df["snapshotDate"] = date
        counts: dict[str, int] = {"leads": len(df)}
        if fmt == "csv":
            _atomic(d / "leads.csv", lambda p: df.to_csv(p, index=False, encoding="utf-8-sig"))
            for name, table in self._tables_from_raws(ls.raws).items():
                _atomic(d / f"{name}.csv", lambda p, t=table: t.to_csv(p, index=False, encoding="utf-8-sig"))
                counts[name] = len(table)
        else:
            def w(p: Path):
                with gzip.open(p, "wt", encoding="utf-8") as f:
                    df.to_json(f, orient="records", force_ascii=False, date_format="iso")
            _atomic(d / "leads.json.gz", w)
            _write_gz_json(d / "raw.json.gz", ls.raws)
            counts["raws"] = len(ls.raws)
        meta = {**(ls.meta or {}), "format": fmt, "date": date, "savedAt": clock.now_iso(), "counts": counts,
                "schemaVersion": SCHEMA_VERSION, "library": f"binance-copytrade-data {_library_version()}"}
        _atomic(d / "meta.json", lambda p: p.write_text(json.dumps(meta, ensure_ascii=False, indent=1, default=str),
                                                        encoding="utf-8"))
        return d

    def load_meta(self, date: str) -> dict:
        p = self.day_dir(date) / "meta.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

    def load_leadset(self, date: str | None = None, raws: bool = True) -> LeadSet | None:
        """raws=False reads only the flat rows (fast: history/diff over many days)."""
        days = self.list_days()
        if not days:
            return None
        date = date or days[-1]
        d = self.day_dir(date)
        meta = self.load_meta(date)
        out_raws: dict = {}
        if (d / "leads.json.gz").exists():
            with gzip.open(d / "leads.json.gz", "rt", encoding="utf-8") as f:
                df = pd.read_json(f, orient="records", dtype={c: str for c in ID_COLUMNS}, convert_dates=False)
            if raws and (d / "raw.json.gz").exists():
                out_raws = _read_gz_json(d / "raw.json.gz")
        elif (d / "leads.csv").exists():
            df = pd.read_csv(d / "leads.csv", dtype={c: str for c in ID_COLUMNS}, encoding="utf-8-sig")
            if raws:
                out_raws = self._raws_from_tables(d, df["portfolioId"].astype(str).tolist() if not df.empty else [])
        else:
            return None
        for c in ID_COLUMNS:
            if c in df:
                df[c] = df[c].where(df[c].isna() | (df[c] == ""), df[c].astype(str)).replace({"": None, "nan": None})
        return LeadSet(df, out_raws, date, meta)

    def history(self, portfolio_id: str) -> pd.DataFrame:
        rows = []
        for date in self.list_days():
            ls = self.load_leadset(date, raws=False)
            if ls is None or ls.df.empty:
                continue
            m = ls.df[ls.df["portfolioId"] == str(portfolio_id)]
            if not m.empty:
                rows.append(m.iloc[0].to_dict())
        return pd.DataFrame(rows)

    # ------------------------------------------------------------ data quality
    def check(self, date: str | None = None) -> dict:
        """What a saved day contains and what is missing: section coverage, truncated lists, private books,
        failed / closed leads, stale files, batch timestamps. Pure read, no network."""
        days = self.list_days()
        if not days:
            return {"error": "no saved days"}
        date = date or days[-1]
        ls = self.load_leadset(date)
        meta, raws = ls.meta or {}, ls.raws
        n = len(raws) or len(ls.df)
        cov = {k: sum(1 for r in raws.values() if r.get(k) is not None)
               for k in ("detail", *RAW_SECTIONS, "linkedPortfolios", "chartPnl", "chart")}
        trunc = {k: sum(1 for r in raws.values() if ((r.get("_fetch") or {}).get(k) or {}).get("truncated"))
                 for k in ("positionHistory", "transferHistory", "orderHistory")}
        legacy_transfer_cap = sum(1 for r in raws.values()
                                  if "_fetch" not in r and len(r.get("transferHistory") or []) == 50)
        hist_days = [((r.get("_fetch") or {}).get("positionHistory") or {}).get("oldest") for r in raws.values()]
        ts = clock.iso_ms(meta.get("savedAt")) or clock.now_ms()
        cover = [(ts - o) / DAY_MS for o in hist_days if o]
        files = sorted(p.name for p in self.day_dir(date).iterdir())
        mixed = any(f in files for f in CSV_FILES) and any(f in files for f in JSON_FILES)
        batch = self.load_batch(date) if date in self.list_batches() else None
        return {
            "date": date, "format": meta.get("format"), "schemaVersion": meta.get("schemaVersion", 1),
            "leads": len(ls.df), "raws": len(raws), "sectionsRequested": meta.get("sections"),
            "coverage": {k: f"{v}/{n}" for k, v in cov.items()},
            "privatePositions": sum(1 for r in raws.values() if r.get("positionsPrivate")
                                    or (r.get("detail") or {}).get("positionShow") is False),
            "truncated": trunc, "legacyTransferCappedAt50": legacy_transfer_cap,
            "historyCoverageDays": {"median": round(float(pd.Series(cover).median()), 1) if cover else None,
                                    "min": round(min(cover), 1) if cover else None},
            "sectionErrors": sum(1 for r in raws.values() if r.get("_errors")),
            "failed": len(meta.get("failed") or {}), "closed": len(meta.get("closed") or []),
            "staleMixedFormats": mixed, "files": files,
            "batch": None if batch is None else {
                "leads": len(batch), "withTimestamp": sum(1 for m in batch.values() if m.get("_timestampFromFile") is None)},
        }

    # ------------------------------------------------------------ enrich checkpoints
    def partial_path(self, date: str) -> Path:
        return self.partial_dir / f"{date}.enrich.json.gz"

    def save_partial(self, date: str, payload: dict) -> Path:
        self.partial_dir.mkdir(parents=True, exist_ok=True)
        p = self.partial_path(date)
        _write_gz_json(p, payload)
        return p

    def load_partial(self, date: str) -> dict | None:
        p = self.partial_path(date)
        return _read_gz_json(p) if p.exists() else None

    def clear_partial(self, date: str) -> None:
        self.partial_path(date).unlink(missing_ok=True)

    # ------------------------------------------------------------ csv tables <-> raws
    @staticmethod
    def _chart_wide(raws: dict, key: str = "chart") -> pd.DataFrame:
        rows = []
        for pid, r in raws.items():
            pts = (r.get(key) or {}).get("data") or []
            if not pts:
                continue
            row = {"portfolioId": pid, "chartRange": (r.get(key) or {}).get("timeRange"),
                   "start": pts[0].get("dateTime")}
            for i, c in enumerate(pts):
                v = _f(c.get("value"))
                row[f"d{i}"] = None if v is None else round(v, 2)
            rows.append(row)
        return pd.DataFrame(rows)

    def _tables_from_raws(self, raws: dict) -> dict[str, pd.DataFrame]:
        out = {}
        for key, name in (("chart", "chart"), ("chartPnl", "chart_pnl")):
            t = self._chart_wide(raws, key)
            if not t.empty:
                out[name] = t
        for name, section in CSV_SECTIONS:
            rows = [{"portfolioId": pid, **item} for pid, r in raws.items() for item in (r.get(section) or [])
                    if isinstance(item, dict)]
            if rows:
                out[name] = pd.DataFrame(rows)
        linked = [{"portfolioId": pid, **(r.get("linkedPortfolios") or {})} for pid, r in raws.items()
                  if r.get("linkedPortfolios")]
        if linked:
            out["linked"] = pd.DataFrame(linked)
        return out

    def _raws_from_tables(self, d: Path, ids: list[str]) -> dict:
        raws = {pid: {"portfolioId": pid, "detail": {}, "performance": {}} for pid in ids}
        for fname, key, dtype in (("chart.csv", "chart", "ROI"), ("chart_pnl.csv", "chartPnl", "PNL")):
            p = d / fname
            if not p.exists():
                continue
            chart = pd.read_csv(p, dtype={"portfolioId": str}, encoding="utf-8-sig")
            dcols = sorted([c for c in chart.columns if c.startswith("d") and c[1:].isdigit()], key=lambda c: int(c[1:]))
            for _, row in chart.iterrows():
                pid = str(row["portfolioId"])
                start = int(row["start"]) if pd.notna(row.get("start")) else 0
                pts = [{"value": float(row[c]), "dateTime": start + i * DAY_MS}
                       for i, c in enumerate(dcols) if pd.notna(row[c])]
                raws.setdefault(pid, {"portfolioId": pid, "detail": {}, "performance": {}})
                raws[pid][key] = {"timeRange": row.get("chartRange"), "dataType": dtype, "data": pts}
        for name, section in CSV_SECTIONS:
            p = d / f"{name}.csv"
            if not p.exists():
                continue
            t = pd.read_csv(p, dtype={"portfolioId": str}, encoding="utf-8-sig")
            for pid, g in t.groupby("portfolioId"):
                items = [{k: v for k, v in rec.items() if k != "portfolioId" and pd.notna(v)} for rec in g.to_dict("records")]
                raws.setdefault(str(pid), {"portfolioId": str(pid), "detail": {}, "performance": {}})[section] = items
        p = d / "linked.csv"
        if p.exists():
            for rec in pd.read_csv(p, dtype=str, encoding="utf-8-sig").to_dict("records"):
                pid = str(rec.pop("portfolioId"))
                raws.setdefault(pid, {"portfolioId": pid, "detail": {}, "performance": {}})["linkedPortfolios"] = {
                    k: v for k, v in rec.items() if pd.notna(v)}
        return raws

    # ------------------------------------------------------------ stage-1 batches
    def list_batches(self) -> list[str]:
        if not self.batch_dir.exists():
            return []
        names = set()
        for p in self.batch_dir.iterdir():
            if p.name.endswith(".partial.json.gz"):
                continue
            if p.name.endswith(".json.gz"):
                names.add(p.name[:-len(".json.gz")])
            elif p.name.endswith("_leads.csv"):
                names.add(p.name[:-len("_leads.csv")])
        return sorted(names)

    def save_batch(self, merged: dict, date: str | None = None, fmt: str = "json", partial: bool = False) -> Path:
        """merged: {pid: {leaderboard, performance{tr}, chart365, chart_start, chart_range, timestamp}}."""
        assert fmt in ("csv", "json")
        date = date or clock.today()
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        if partial or fmt == "json":
            p = self.batch_dir / f"{date}{'.partial' if partial else ''}.json.gz"
            _write_gz_json(p, merged)
            return p
        rows = [flatten_lead(batch_to_raw(m)) for m in merged.values()]
        leads = pd.DataFrame(rows)
        _atomic(self.batch_dir / f"{date}_leads.csv", lambda p: leads.to_csv(p, index=False, encoding="utf-8-sig"))
        chart = pd.DataFrame([{"portfolioId": pid, "chartRange": m.get("chart_range"), "start": m.get("chart_start"),
                               **{f"d{i}": v for i, v in enumerate(m.get("chart365") or [])}} for pid, m in merged.items()])
        _atomic(self.batch_dir / f"{date}_chart.csv", lambda p: chart.to_csv(p, index=False, encoding="utf-8-sig"))
        return self.batch_dir / f"{date}_leads.csv"

    def load_batch(self, date: str | None = None, partial: bool = False) -> dict | None:
        """Batches saved before schemaVersion 2 have no per-lead timestamp: they get <date>T12:00Z (and a
        _timestampFromFile marker) so ages computed later stay within half a day of the truth."""
        if partial:
            p = self.batch_dir / f"{date or clock.today()}.partial.json.gz"
            return _read_gz_json(p) if p.exists() else None
        dates = self.list_batches()
        if not dates:
            return None
        date = date or dates[-1]
        p = self.batch_dir / f"{date}.json.gz"
        if p.exists():
            merged = _read_gz_json(p)
        else:
            p = self.batch_dir / f"{date}_leads.csv"
            if not p.exists():
                return None
            leads = pd.read_csv(p, dtype={"portfolioId": str}, encoding="utf-8-sig")
            pc = self.batch_dir / f"{date}_chart.csv"
            chart = pd.read_csv(pc, dtype={"portfolioId": str}, encoding="utf-8-sig") if pc.exists() else pd.DataFrame()
            merged = self._merged_from_tables(leads, chart)
        fallback = f"{date}T12:00:00+00:00"
        for m in merged.values():
            if not m.get("timestamp"):
                m["timestamp"] = fallback
                m["_timestampFromFile"] = True
        return merged

    @staticmethod
    def _merged_from_tables(leads: pd.DataFrame, chart: pd.DataFrame) -> dict:
        g = lambda row, k: (None if k not in row or pd.isna(row[k]) else row[k])
        merged = {}
        for row in leads.to_dict("records"):
            pid = str(row["portfolioId"])
            perf = {}
            for tr in TIME_RANGES:
                k = tr.lower()
                if g(row, f"roi{k}") is None and g(row, f"pnl_{k}") is None:
                    continue
                perf[tr] = {"timeRange": tr, "roi": g(row, f"roi{k}"), "pnl": g(row, f"pnl_{k}"), "mdd": g(row, f"mdd_{k}"),
                            "winRate": g(row, f"winRate_{k}"), "sharpRatio": g(row, "sharpeRatio"),
                            "copierPnl": g(row, f"copierPnl_{k}")}
            lb = {"leadPortfolioId": pid, "nickname": g(row, "nickname"), "currentCopyCount": g(row, "currentCopiers"),
                  "maxCopyCount": g(row, "maxCopiers"), "aum": g(row, "aum"), "startTime": g(row, "startTime"),
                  "portfolioType": g(row, "portfolioType"), "badgeName": g(row, "badge"), "apiKeyTag": g(row, "apiKeyTag"),
                  "tradFiTag": g(row, "tradFiTag"), "aiSummary": g(row, "aiSummary"), "sharpRatio": g(row, "sharpeRatio"),
                  "copierPnl": g(row, "copierPnl30d"), "roi": g(row, "roi30d"), "mdd": g(row, "mdd"), "winRate": g(row, "winRate")}
            merged[pid] = {"portfolioId": pid, "leaderboard": lb, "performance": perf, "timestamp": g(row, "timestamp")}
        if not chart.empty:
            dcols = sorted([c for c in chart.columns if c.startswith("d") and c[1:].isdigit()], key=lambda c: int(c[1:]))
            for row in chart.to_dict("records"):
                pid = str(row["portfolioId"])
                if pid not in merged:
                    continue
                vals = [row[c] for c in dcols if pd.notna(row[c])]
                merged[pid]["chart365"] = vals
                merged[pid]["chart_start"] = int(row["start"]) if pd.notna(row.get("start")) else None
                merged[pid]["chart_range"] = row.get("chartRange")
        return merged
