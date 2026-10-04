# Changelog

## 0.2.0 — 2026-10-04 (review fixes)
Fixed
- Batches stamp each lead with the time it was seen; old batches get `<date>T12:00Z`. Re-reading a batch no longer ages every lead by the days since the fetch (was +29 days on the 2026-09-05 batch).
- Transfer history is fetched completely (pages of 100 until `total`); it was cut at 50 for 9% of leads, which inflated withdrawal ratios.
- Temporary API errors (`11012005` busy, "try again", "too many") are retried with backoff instead of dropping the lead.
- Failed and closed leads are recorded in `meta.failed` / `meta.closed`; failed sections in `raw._errors` / row `sectionErrors`.
- Saving a day removes the other format's files (no stale csv next to json); all writes are atomic.
- Leads that hide positions are marked `positionsPrivate` instead of getting an empty (= "flat") position list.
- `enrich --resume` reuses a lead only if it already has every requested section.
Added
- `Sections` (what to fetch per lead) and `Sections.full()`; CLI `--all-sections`, `--history-days`, `--orders`, `--linked`, `--pnl-chart`.
- Endpoints: `order_history` (individual orders), `linked_portfolios` (same owner's portfolios), PnL chart; position history paged by days.
- `PortfolioClosedError`, `AuthRequiredError`, `BinanceCopyError.code`.
- `enrich(checkpoint=True)` / `--checkpoint`: progress in `data/partial/`, resumed after a crash, cleared after a save.
- `check()` / `binance-copytrade check`: coverage, truncation, private books, failures, stale files, batch timestamps.
- `load(raws=False)`; `history()` / `diff()` / `latest()` no longer read raw payloads.
- `meta.schemaVersion` (2) and library version; `_fetch` paging info per section.
- Flat columns: `histTruncated`, `histCoverDays`, `transfersN`, `transfersTruncated`, `ordersN`, `ordersPerDay`, `ordersLimitShare`, `ordersMarketShare`, `ordersSymbols`, `linkedPortfolioIds`, `ownerActivePortfolios`, `ownerKey`, `chartPnlFinal`, `chartPnlMax`, `chartPnlDays`, `positionsPrivate`, `sectionErrors`.
- CI workflow (offline unit tests).
Changed
- Position history pages hold 100 positions (was 50); `--history-pages` counts those pages.
- Copiers: top 30 (was 10).

## 0.1.0 — 2026-10-04
Split out of pybinance-port: client, filters, service, store, normalize, CLI, offline test kit.
