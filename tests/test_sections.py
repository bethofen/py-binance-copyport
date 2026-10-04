"""Per-lead sections, paging, failure records, resume, checkpoints, batch timestamps (review fixes A/B/C, 2026-10-04)."""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from binance_copytrade import (AuthRequiredError, BinanceCopyClient, BinanceCopyError, CopyTradeService,
                               PortfolioClosedError, SCHEMA_VERSION, Sections, clock, has_sections)
from binance_copytrade.client import EP_LINKED, EP_ORDER_HISTORY
from binance_copytrade.testing import DAY, DETAIL, NOW, PID, BatchFakeClient, FakeClient, freeze_clock
from tests.test_client import _resp


def transfers(n):
    return [{"time": NOW - i * DAY, "coin": "USDT", "amount": 10.0,
             "transType": "LEAD_WITHDRAW" if i % 2 else "LEAD_DEPOSIT"} for i in range(n)]


def history(n, step_days=0.5, lev=5):
    return [{"symbol": "BTCUSDT", "side": "Long", "leverage": lev, "closingPnl": 1.0, "status": "All Closed",
             "opened": NOW - int(i * step_days * DAY), "closed": NOW - int(i * step_days * DAY) + 3_600_000}
            for i in range(n)]


def calls(client, suffix):
    return [c for c in client.calls if c[1].endswith(suffix)]


# ------------------------------------------------------------------ A1 batch timestamps
def test_batch_entries_carry_fetch_time_so_ages_do_not_drift(bsvc, monkeypatch):
    merged = bsvc.batch_collect(pages=5)
    assert all(m["timestamp"] for m in merged.values())
    bsvc.save_batch(merged, fmt="json", date="2026-09-05")
    then = bsvc.batch_frame(bsvc.store.load_batch("2026-09-05")).set_index("portfolioId")["daysTrading"]
    freeze_clock(monkeypatch, NOW + 29 * DAY)                    # re-read a month later
    later = bsvc.batch_frame(bsvc.store.load_batch("2026-09-05")).set_index("portfolioId")["daysTrading"]
    assert (later == then).all()


def test_legacy_batch_without_timestamps_uses_file_date(bsvc, monkeypatch):
    merged = bsvc.batch_collect(pages=5)
    for m in merged.values():
        m.pop("timestamp")
    bsvc.save_batch(merged, fmt="json", date="2026-09-05")
    freeze_clock(monkeypatch, NOW + 29 * DAY)
    back = bsvc.store.load_batch("2026-09-05")
    assert all(m["timestamp"] == "2026-09-05T12:00:00+00:00" and m["_timestampFromFile"] for m in back.values())
    assert bsvc.batch_frame(back).set_index("portfolioId").loc["L3", "daysTrading"] == 35


# ------------------------------------------------------------------ A2 full transfers / paging
def test_transfers_are_fetched_completely(svc, fake_client):
    fake_client.transfers = transfers(250)
    raw = svc.fetch_lead(PID)
    assert len(raw["transferHistory"]) == 250 and len(calls(fake_client, "/transfer-history")) == 3
    assert raw["_fetch"]["transferHistory"]["truncated"] is False
    assert {c[2]["pageSize"] for c in calls(fake_client, "/transfer-history")} == {100}


def test_page_cap_is_reported_as_truncated(svc, fake_client):
    fake_client.transfers = transfers(250)
    raw = svc._fetch_sections(PID, {"detail": DETAIL}, Sections(transfers=True, transfer_pages=1))
    assert len(raw["transferHistory"]) == 100 and raw["_fetch"]["transferHistory"]["truncated"] is True


def test_history_days_stops_once_window_is_covered(svc, fake_client):
    fake_client.history = history(1000, step_days=0.5)           # 500 days of positions, newest first
    raw = svc._fetch_sections(PID, {"detail": DETAIL}, Sections(history_days=90, history_pages=30))
    info = raw["_fetch"]["positionHistory"]
    assert info["windowReached"] and not info["truncated"] and info["pages"] == 2      # 200 positions = 100 days
    assert (NOW - info["oldest"]) / DAY >= 90


def test_history_page_cap_without_window_is_truncated(svc, fake_client):
    fake_client.history = history(1000)
    raw = svc._fetch_sections(PID, {"detail": DETAIL}, Sections(history_pages=2))
    assert len(raw["positionHistory"]) == 200 and raw["_fetch"]["positionHistory"]["truncated"]


# ------------------------------------------------------------------ A3 retries / error classes (client)
@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("binance_copytrade.client.time.sleep", lambda *_: None)
    c = BinanceCopyClient(delay=0, jitter=0, max_retries=3, backoff=1.0)
    c._throttle = lambda: None
    return c


def test_busy_code_is_retried(client):
    busy = _resp({"code": "11012005", "message": "The system is currently busy. Please try again later."})
    ok = _resp({"code": "000000", "data": {"total": 0, "list": []}})
    with patch.object(client.s, "request", side_effect=[busy, busy, ok]) as req:
        assert client.order_history("1") == {"total": 0, "list": []}
    assert req.call_count == 3


def test_busy_forever_gives_up_with_code(client):
    busy = _resp({"code": "11012005", "message": "busy"})
    with patch.object(client.s, "request", return_value=busy):
        with pytest.raises(BinanceCopyError) as e:
            client.transfer_history("1")
    assert e.value.code == "11012005" and not isinstance(e.value, PortfolioClosedError)


def test_closed_and_auth_have_their_own_classes_and_are_not_retried(client):
    with patch.object(client.s, "request", return_value=_resp({"code": "11012028", "message": "closed"})) as req:
        with pytest.raises(PortfolioClosedError):
            client.detail("1")
    assert req.call_count == 1
    with patch.object(client.s, "request", return_value=_resp({"code": "100002001", "message": "login"})):
        with pytest.raises(AuthRequiredError):
            client.detail("1")


def test_new_endpoints_request_shape(client):
    with patch.object(client.s, "request", return_value=_resp({"code": "000000", "data": {"list": []}})) as req:
        client.order_history("9", page=3)
    assert req.call_args.args == ("POST", "https://www.binance.com/bapi/futures/v1" + EP_ORDER_HISTORY)
    assert req.call_args.kwargs["json"] == {"pageNumber": 3, "pageSize": 100, "portfolioId": "9"}
    with patch.object(client.s, "request", return_value=_resp({"code": "000000", "data": None})) as req:
        assert client.linked_portfolios("9") == {}
    assert req.call_args.args[1].endswith(EP_LINKED) and req.call_args.kwargs["params"] == {"portfolioId": "9"}


# ------------------------------------------------------------------ A4 failures recorded
def test_closed_and_failed_leads_are_recorded_in_meta(bsvc):
    merged = bsvc.batch_collect(pages=5)
    bsvc.client.closed_ids = {"L2"}
    orig = bsvc.client.detail
    bsvc.client.detail = lambda pid: (_ for _ in ()).throw(BinanceCopyError("boom")) if pid == "L3" else orig(pid)
    ls = bsvc.enrich(merged, ["L1", "L2", "L3"])
    assert ls.ids == ["L1"] and ls.meta["closed"] == ["L2"] and "L3" in ls.meta["failed"]


def test_section_error_keeps_lead_and_is_recorded(svc, fake_client):
    fake_client.broken_paths = {"/transfer-history": "busy"}
    ls = svc.fetch_leads([PID])
    raw = ls.raw(PID)
    assert len(ls) == 1 and "transferHistory" in raw["_errors"] and "transferHistory" not in raw
    assert ls.df.iloc[0]["sectionErrors"] == "transferHistory"
    assert not has_sections(raw, Sections(transfers=True))      # resume will refetch it


# ------------------------------------------------------------------ A6 private positions
def test_private_positions_marked_not_fetched(svc, fake_client):
    raw = svc.fetch_lead(PID)                                    # DETAIL.positionShow is False
    assert raw["positionsPrivate"] and "positions" not in raw and not calls(fake_client, "/lead-data/positions")
    row = svc.fetch_leads([PID]).df.iloc[0]
    assert row["positionsPrivate"] and row["openPositions"] is None
    assert has_sections(raw, Sections(positions=True))           # private counts as complete


def test_public_empty_book_is_kept_as_empty_list(svc, fake_client):
    orig = fake_client.detail
    fake_client.detail = lambda pid: dict(orig(pid), positionShow=True)
    fake_client.open_positions = [{"symbol": "ETHUSDT", "positionSide": "LONG", "positionAmount": "0"}]
    raw = svc.fetch_lead(PID)
    assert raw["positions"] == fake_client.open_positions and "positionsPrivate" not in raw
    assert svc.fetch_leads([PID]).df.iloc[0]["openPositions"] == 0


# ------------------------------------------------------------------ B orders / linked / PnL chart
def test_full_sections_fetch_and_flatten_new_data(svc, fake_client):
    ls = svc.enrich({}, [PID], sections=Sections.full())
    raw, row = ls.raw(PID), ls.df.iloc[0]
    assert len(raw["orderHistory"]) == 12 and raw["chartPnl"]["dataType"] == "PNL" and raw["linkedPortfolios"]
    assert row["ordersN"] == 12 and row["ordersLimitShare"] == pytest.approx(75.0) and row["ordersPerDay"] > 0
    assert row["linkedPortfolioIds"] == "5043232486099187456" and row["ownerActivePortfolios"] == 2
    assert row["ownerKey"] == min(PID, "5043232486099187456")
    assert row["chartPnlFinal"] == 25000 and row["histCoverDays"] >= 0 and row["transfersN"] == 3
    assert len(calls(fake_client, "/copy-traders")) == 1 and calls(fake_client, "/copy-traders")[0][2]["pageSize"] == 30


def test_orders_respect_order_days(svc, fake_client):
    fake_client.orders = [dict(o, orderTime=NOW - i * DAY) for i, o in enumerate(fake_client.orders * 20)]
    raw = svc._fetch_sections(PID, {"detail": DETAIL}, Sections(orders=True, order_days=50))
    assert raw["_fetch"]["orderHistory"]["windowReached"] and len(raw["orderHistory"]) == 100


# ------------------------------------------------------------------ A7 resume needs every section
def test_resume_refetches_leads_missing_requested_sections(bsvc):
    merged = bsvc.batch_collect(pages=5)
    first = bsvc.enrich(merged, ["L1", "L2"])                    # no transfers
    bsvc.save(first, fmt="json", date="2026-09-05")
    n = len(calls(bsvc.client, "/detail"))
    again = bsvc.enrich(merged, ["L1", "L2"], transfers=True, resume_from=bsvc.load("2026-09-05"))
    assert len(calls(bsvc.client, "/detail")) == n + 2 and all(r.get("transferHistory") is not None for r in again.raws.values())
    n = len(calls(bsvc.client, "/detail"))
    bsvc.save(again, fmt="json", date="2026-09-05")
    bsvc.enrich(merged, ["L1", "L2"], transfers=True, resume_from=bsvc.load("2026-09-05"))
    assert len(calls(bsvc.client, "/detail")) == n                 # now complete -> reused


# ------------------------------------------------------------------ C1 enrich checkpoint
def test_enrich_checkpoint_resumes_after_crash(bsvc):
    merged = bsvc.batch_collect(pages=5)
    bsvc.enrich(merged, ["L1", "L2"], checkpoint=True, checkpoint_every=1, date="2026-09-05")
    assert bsvc.store.load_partial("2026-09-05")["raws"].keys() == {"L1", "L2"}
    n = len(calls(bsvc.client, "/detail"))
    ls = bsvc.enrich(merged, ["L1", "L2", "L3"], checkpoint=True, date="2026-09-05")
    assert len(calls(bsvc.client, "/detail")) == n + 1 and set(ls.ids) == {"L1", "L2", "L3"}
    bsvc.store.clear_partial("2026-09-05")
    assert bsvc.store.load_partial("2026-09-05") is None


# ------------------------------------------------------------------ A5 / C store hygiene
def test_saving_other_format_removes_stale_files_and_writes_atomically(svc):
    ls = svc.fetch_leads([PID])
    d = svc.save(ls, fmt="csv", date="2026-09-05")
    assert (d / "chart_pnl.csv").exists() and (d / "linked.csv").exists()
    svc.save(ls, fmt="json", date="2026-09-05")
    files = {p.name for p in d.iterdir()}
    assert files == {"leads.json.gz", "raw.json.gz", "meta.json"}
    meta = json.loads((d / "meta.json").read_text(encoding="utf-8"))
    assert meta["schemaVersion"] == SCHEMA_VERSION and meta["library"].startswith("binance-copytrade-data")


def test_csv_roundtrip_keeps_new_sections(svc):
    ls = CopyTradeService(svc.cfg, client=svc.client, store=svc.store)
    ls.cfg.include_orders = True
    full = ls.fetch_leads([PID])
    svc.save(full, fmt="csv", date="2026-09-05")
    raw = svc.load("2026-09-05").raw(PID)
    assert len(raw["orderHistory"]) == 12 and raw["chartPnl"]["data"][-1]["value"] == 25000
    assert raw["linkedPortfolios"]["futuresPrivateLPId"] == "5043232486099187456"


def test_load_without_raws_and_fast_history(svc):
    ls = svc.fetch_leads([PID])
    svc.save(ls, fmt="json", date="2026-09-04")
    svc.save(ls, fmt="json", date="2026-09-05")
    assert svc.load("2026-09-05", raws=False).raws == {} and svc.load("2026-09-05").raws
    with patch("binance_copytrade.store._read_gz_json", side_effect=AssertionError("raws must not be read")):
        assert len(svc.history(PID)) == 2 and not svc.latest().empty


def test_check_reports_quality(bsvc):
    ls = bsvc.pipeline(pages=5, sections=Sections(transfers=True, transfer_pages=1, positions=True))
    bsvc.save_batch(ls.batch, fmt="json", date=clock.today())
    bsvc.save(ls, fmt="json")
    r = bsvc.check()
    assert r["schemaVersion"] == SCHEMA_VERSION and r["leads"] == len(ls) and r["failed"] == 0
    assert r["coverage"]["transferHistory"] == f"{len(ls)}/{len(ls)}" and r["privatePositions"] == len(ls)
    assert r["staleMixedFormats"] is False and r["batch"]["leads"] == 45


# ------------------------------------------------------------------ CLI
def test_cli_check_all_sections_and_checkpoint_cleanup(tmp_path, capsys):
    from binance_copytrade.cli import main
    data = str(tmp_path / "data")
    c = BatchFakeClient()
    assert main(["--data-dir", data, "batch", "--pages", "5", "--save", "json", "--date", "2026-09-05"], client=c) == 0
    assert main(["--data-dir", data, "enrich", "--min-days", "30", "--all-sections", "--checkpoint",
                 "--save", "json", "--date", "2026-09-05"], client=c) == 0
    assert not (tmp_path / "data" / "partial" / "2026-09-05.enrich.json.gz").exists()     # cleared after save
    capsys.readouterr()
    assert main(["--data-dir", data, "check", "--date", "2026-09-05"], client=c) == 0
    rep = json.loads(capsys.readouterr().out)
    assert rep["coverage"]["orderHistory"].split("/")[0] == rep["coverage"]["detail"].split("/")[0]
    assert main(["--data-dir", data, "lead", PID, "--no-extras"], client=FakeClient()) == 0
    assert main(["--data-dir", str(tmp_path / "empty"), "check"], client=c) == 1


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_long_ids_survive_save_load_exactly(svc, fmt):
    ls = svc.enrich({}, [PID], sections=Sections(linked=True))
    key = ls.df.iloc[0]["ownerKey"]
    svc.save(ls, fmt=fmt, date="2026-09-05")
    back = svc.load("2026-09-05", raws=False).df.iloc[0]
    assert back["ownerKey"] == key and isinstance(back["ownerKey"], str) and len(key) >= 18
    assert back["linkedPortfolioIds"] == "5043232486099187456" and back["portfolioId"] == PID
