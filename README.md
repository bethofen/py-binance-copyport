# binance-copytrade-data

Library + CLI สำหรับ**ดึง จัดรูป และเก็บ**ข้อมูล lead trader ของ Binance Futures Copy Trading
(ส่วนวิเคราะห์ ให้คะแนน และเลือก lead อยู่ใน repo แยก `copytrade-quant` · repo นี้: `bethofen/py-binance-copyport`, ชื่อ package บน pip: `binance-copytrade-data`, import: `binance_copytrade`)

> **คำเตือน:** endpoint ทั้งหมดเป็น **internal web API ของ binance.com** (`/bapi/...`) ไม่ใช่ official API
> **ไม่มี SLA** ไม่มีเอกสาร อาจเปลี่ยน เพิ่ม auth หรือหยุดได้ทุกเมื่อ (ตรวจล่าสุด 2026-10-03 เรียกได้ทุกตัว
> **โดยไม่ต้อง login, cookie, signature**) ถ้าเจอ error เรื่อง login ให้หยุด client จะ raise
> `BinanceCopyError("requires auth")` ห้าม bypass
> library **ไม่เขียนไฟล์เอง** ทุกอย่างอยู่ในหน่วยความจำจนกว่าจะเรียก `save(...)` และเก็บเฉพาะในเครื่อง (`data/`) ไม่มี cloud

## ติดตั้ง

```bash
pip install -e ".[dev]"
```
จาก repo อื่น (หลังสร้าง repo แล้ว):
```bash
pip install "binance-copytrade-data @ git+https://github.com/bethofen/py-binance-copyport.git"
```
ต้องการ Python ≥ 3.10, `requests`, `pandas`

## โครงสร้าง

| module | หน้าที่ |
|---|---|
| `binance_copytrade.client` | `BinanceCopyClient` ครอบ endpoint ทั้ง 12 ตัว มี retry/backoff (รวม error ชั่วคราว `11012005` busy), delay+jitter, header แบบ browser · error: `BinanceCopyError` (มี `.code`), `PortfolioClosedError` (`11012028`), `AuthRequiredError` |
| `binance_copytrade.filters` | `FetchFilter` เลือกชุด lead ทั้ง server-side และ client-side |
| `binance_copytrade.service` | `CopyTradeService`: `collect`, `fetch_lead(s)`, `batch_collect`, `enrich`, `pipeline`, `save/load`, `history`, `diff`, `rebuild`, `check` · `Sections` กำหนดว่าจะดึงอะไรต่อคน |
| `binance_copytrade.leadset` | `LeadSet` ผลลัพธ์ของทุกการดึง (`df` แถว flat + `raws` payload ดิบ + `meta`) |
| `binance_copytrade.normalize` | `flatten_lead` แปลง raw เป็นแถวเดียว, `batch_to_raw`, `CORE_COLUMNS` |
| `binance_copytrade.store` | `LocalStore` เก็บ/อ่าน csv หรือ json.gz แบบ atomic, checkpoint ของ enrich, `check()` รายงานคุณภาพข้อมูล |
| `binance_copytrade.clock` | นาฬิกาเดียวของทั้ง package (test freeze ได้) |
| `binance_copytrade.cli` | คำสั่ง `binance-copytrade` (หรือ `python -m binance_copytrade`) |
| `binance_copytrade.testing` | ชุดทดสอบ offline: `FakeClient`, `BatchFakeClient`, payload ตัวอย่าง, `freeze_clock` ให้ repo อื่นใช้ต่อได้ |

## ใช้งาน (Python)

```python
from binance_copytrade import CopyTradeService, ServiceConfig, FetchFilter, Sections

svc = CopyTradeService(ServiceConfig(data_dir="data", delay=0.6))

# ชุดเล็ก: leaderboard + ดึงเต็มรายคน
ls = svc.collect(FetchFilter(source="ai", sort="AUM", pages=2, min_days=30, exclude_full=True))
ls.df            # แถว flat 1 แถว/คน (roi7d..roi365d, mdd, aum, marginBalance, minMargin, daysTrading, hist*, chart* ...)
ls.raw("<id>")   # payload ดิบ: detail, performance{7D..365D}, chart, positionHistory, transferHistory, positions ...
svc.save(ls, fmt="json")                      # data/daily/<date>/  (เลือกเอง csv หรือ json)

# ทั้งตลาด สองขั้น
merged = svc.batch_collect(pages=167, sort="AUM")                         # ขั้น 1: 5 request ต่อ 30 คน
svc.save_batch(merged, fmt="json")                                        # data/batch/<date>.json.gz
ls = svc.pipeline(FetchFilter(min_days=30, max_mdd=40, min_aum=10000), batch=merged,
                  sections=Sections.full(), checkpoint=True)               # ขั้น 2 เฉพาะผู้เข้ารอบ
ls.meta["closed"], ls.meta["failed"]                                       # พอร์ตที่ปิดแล้ว / ดึงไม่สำเร็จ
svc.save(ls, fmt="json"); svc.store.clear_partial(ls.date)

# อ่านกลับ
svc.days(); svc.store.list_batches()
ls = svc.load()                    # วันล่าสุด -> LeadSet
merged = svc.store.load_batch()    # batch ล่าสุด
svc.history("<portfolioId>")       # แถวของ lead นี้ทุกวัน
svc.diff()                         # วันล่าสุดเทียบวันก่อน (*_delta)  ส่ง cols=(...) เพิ่มได้
svc.rebuild("2026-10-03")          # flatten ใหม่จาก raw (json)
svc.check("2026-10-03")            # ครอบคลุมแค่ไหน, ถูกตัดกี่คน, ล้มเหลว/ปิดพอร์ตกี่คน, ไฟล์ค้าง
svc.load(raws=False)               # อ่านเฉพาะแถว (เร็ว)
```

### `Sections`: ดึงอะไรต่อคน (นอกจาก detail)

| ช่อง | ค่าเริ่มต้น | ความหมาย |
|---|---|---|
| `history_days` / `history_pages` | None / 2 | ดึงประวัติ position ย้อนจนครบ N วัน ภายในเพดาน N หน้า (100 ไม้/หน้า) |
| `transfers` | False | ฝาก/ถอนของ lead **ครบทุกรายการ** (แบ่งหน้าจนครบ total) |
| `positions` | False | position ที่เปิดอยู่ ถ้า lead ซ่อนไว้จะไม่ดึงแต่ใส่ `positionsPrivate=True` |
| `copiers` | False | copier top 30 |
| `orders` / `order_days` | False / 30 | ออเดอร์รายตัว (แท็บ Latest Records) |
| `linked` | False | พอร์ตอื่นของเจ้าของเดียวกัน → คอลัมน์ `ownerKey`, `linkedPortfolioIds` |
| `pnl_chart` | False | กราฟ PnL รายวันหน่วย USDT |

`Sections.full()` เปิดทุกอย่าง (history 90 วัน) · raw ทุกคนมี `_fetch` (จำนวน, total, ถูกตัดไหม, วันเก่าสุด) และ `_errors` (ส่วนที่ดึงไม่สำเร็จ) ส่วนแถว flat มี `histTruncated`, `transfersTruncated`, `histCoverDays`, `ordersN`, `ordersLimitShare`, `ownerKey`, `chartPnlFinal`, `sectionErrors` ฯลฯ

## ใช้งาน (CLI) ทุกคำสั่งแค่พิมพ์ผล ต้องเติม `--save` ถึงจะเขียนไฟล์

```bash
binance-copytrade options
```
```bash
binance-copytrade --delay 0.6 collect --source ai --sort AUM --pages 3 --min-days 30 --exclude-full --save json
```
```bash
binance-copytrade --delay 0.6 batch --pages 167 --sort AUM --checkpoint --save json
```
```bash
binance-copytrade --delay 0.6 enrich --min-days 30 --max-mdd 40 --min-copier-pnl 0 --min-aum 10000 --all-sections --checkpoint --save json
```
```bash
binance-copytrade --delay 0.6 pipeline --sort AUM --pages 167 --min-days 30 --max-mdd 40 --min-copier-pnl 0 --min-aum 10000 --all-sections --checkpoint --save json --save-batch json
```
```bash
binance-copytrade lead 5075281354358777856 --save json --json raw.json --out flat.csv
```
```bash
binance-copytrade days
```
```bash
binance-copytrade diff --cols roi30d,mdd,aum
```
```bash
binance-copytrade check --date 2026-10-03
```
`--data-dir` ชี้โฟลเดอร์ข้อมูล (ค่าเริ่มต้น `data`) และ `--delay` คือวินาทีระหว่าง request (≥ 0.5)
`--checkpoint` เก็บความคืบหน้าของ enrich ไว้ที่ `data/partial/<date>.enrich.json.gz` รันซ้ำแล้วต่อจากเดิมได้ และไฟล์จะถูกลบเมื่อ `--save` สำเร็จ
flag แยก: `--history-days 90 --transfers --positions --copiers --orders --order-days 30 --linked --pnl-chart`

## โครงข้อมูล (เขียนเฉพาะตอนสั่ง save)

```
data/daily/<date>/   csv : leads.csv chart.csv chart_pnl.csv (1 แถว/คน d0..dN) history.csv transfers.csv copiers.csv
                           positions.csv orders.csv linked.csv meta.json
                     json: leads.json.gz raw.json.gz (payload ครบทุก field รวม _fetch/_errors) meta.json
data/batch/<date>.json.gz   หรือ  <date>_leads.csv + <date>_chart.csv
data/partial/<date>.enrich.json.gz   checkpoint ระหว่าง enrich
```
- `meta.json` บันทึกวิธีดึง (`stage`, `filter`, `sections`), `failed {id: error}`, `closed [id]`, `schemaVersion` (ตอนนี้ 2) และเวอร์ชัน library
- เซฟวันเดิมซ้ำจะลบไฟล์ของอีกรูปแบบทิ้งก่อน และทุกไฟล์เขียนแบบ atomic (เขียนไฟล์ชั่วคราวแล้ว rename)
- batch เก็บเวลาที่เห็นแต่ละคน (`timestamp`) ส่วน batch เก่าที่ไม่มีค่านี้จะใช้ `<date>T12:00Z` แทน
- section ที่ดึงแล้วแต่ว่างจะเก็บเป็น `[]` ถ้าไม่มี key แปลว่าไม่ได้ดึง ฝั่งวิเคราะห์ใช้แยกระหว่าง "ไม่มีการโอนเลย" กับ "ไม่รู้"
- อายุพอร์ตและค่าที่คำนวณจากเวลาทุกตัวอิงเวลาตอนดึง (`timestamp` ใน raw) อ่านไฟล์เก่าซ้ำจึงได้ค่าเดิมเสมอ

| รูปแบบ | ~800 คน | ข้อดี | ข้อเสีย |
|---|---|---|---|
| `json` | ~11 MB (gz, history 4 หน้า) | เก็บทุก field, `rebuild` ได้ | ต้องแตก gz ก่อนอ่าน |
| `csv` | ~6 MB | เปิด Excel/pandas ได้ตรง | เก็บเฉพาะตาราง (detail ที่ซ้อนกันหาย) |

## เลือกชุด lead (`FetchFilter`)

| ระดับ | ตัวเลือก | ค่า |
|---|---|---|
| **แหล่ง** `source` | `all` / `ai` / `recommended` | ทั้งฐาน ~12,000 คน / Smart Filter ของ Binance ~750 คน / 6 กลุ่มหน้าแรก (`groups=`) |
| **เรียง** `sort` | `ROI` `PNL` `MDD` `AUM` `COPY_COUNT` `COPIER_PNL` `SHARP_RATIO` `WIN_RATE` | คั่นด้วย `,` = รวมหลาย leaderboard แบบไม่ซ้ำ |
| **ช่วง** `time_range` | `7D` `30D` `90D` `180D` `365D` | |
| server-side อื่น | `portfolio_type`, `order`, `hide_full`, `nickname`, `pages` (30/หน้า) | |
| **client-side** | `min/max_days`, `min/max_aum`, `min/max_copiers`, `min/max_roi`, `max_mdd`, `min_win_rate`, `min_sharpe`, `min_copier_pnl`, `badges`, `require_api_key`, `require_tradfi`, `exclude_full`, `exclude_ids`, `include_ids`, `limit` | |

Binance ไม่มี parameter กรองตัวเลขฝั่ง server (`minAum`, `maxMdd` ฯลฯ ถูกเพิกเฉย) จึงต้องกรองจากแถว leaderboard ก่อนดึงรายคน

## Pipeline สองขั้น

leaderboard ส่ง `chartItems` (ROI รายวัน) และค่าของช่วงที่ขอมาให้ด้วย เมื่อเรียงด้วย `AUM`/`COPY_COUNT` (ไม่ขึ้นกับช่วง) ทุกช่วงจะได้คนชุดเดียวกัน
5 ช่วง × N หน้าจึงได้ performance 7D–365D และ chart 365 วันครบ โดยไม่ต้องยิงรายคน

| โหมด | request/คน | เวลา |
|---|---|---|
| `batch` (ขั้น 1) | 5 ต่อ 30 คน | 5,000 คน ≈ 13 นาที (`--checkpoint` resume ได้, `--stop-when aum<N`) |
| `enrich` (ขั้น 2) | 1 + history pages + transfer pages + positions + orders + linked + PnL | 884 คน `--all-sections` ≈ 1.5–2 ชม. (`--checkpoint` ต่อจากเดิมได้) |
| `collect` full | ~13 | ใช้กับชุดเล็ก |

## Endpoint (base `https://www.binance.com/bapi/futures/v1`)

| # | Path | Method | ได้อะไร |
|---|---|---|---|
| 1 | `/friendly/future/copy-trade/home-page/query-list` | POST | leaderboard: roi, pnl, aum, mdd, winRate, sharpRatio, copierPnl, copiers, chartItems |
| 2 | `/friendly/future/copy-trade/home-page/recommend-lead-list` | POST | 6 กลุ่มแนะนำ |
| 3 | `/friendly/future/copy-trade/lead-portfolio/detail` | GET | marginBalance, aumAmount, profitSharingRate, min copy (fixedRadio/fixedAmount), tags, closeLeadCount, positionShow |
| 4 | `/public/future/copy-trade/lead-portfolio/performance` | GET | roi, pnl, mdd, winRate, orders, copierPnl ต่อช่วง |
| 5 | `/public/future/copy-trade/lead-portfolio/chart-data` | GET | ROI/PNL รายวัน |
| 6 | `/public/future/copy-trade/lead-portfolio/performance/coin` | GET | สัดส่วน volume ต่อเหรียญ |
| 7 | `/friendly/future/copy-trade/lead-data/positions` | GET | position เปิดอยู่ (`[]` ถ้า lead ตั้ง private) |
| 8 | `/friendly/future/copy-trade/lead-portfolio/position-history` | POST | ไม้ที่ปิดแล้ว |
| 9 | `/friendly/future/copy-trade/lead-portfolio/transfer-history` | POST | ฝาก/ถอนของ lead |
| 10 | `/friendly/future/copy-trade/lead-portfolio/copy-traders` | POST | copier (สูงสุด 30/หน้า) |
| 11 | `/friendly/future/copy-trade/lead-portfolio/order-history` | POST | ออเดอร์รายตัว: symbol, side, type (LIMIT/MARKET), qty, avgPrice, totalPnl, orderTime |
| 12 | `/friendly/future/spot-copy-trade/common/spot-futures-last-lead` | GET | พอร์ตอื่นของเจ้าของเดียวกัน (public/private/signal LP id + status) |

Envelope `{"code":"000000","data":...}` · พอร์ตปิดแล้วตอบ `11012028` · ระบบยุ่งตอบ `11012005` (retry อัตโนมัติ) · leaderboard `pageSize` สูงสุด 30, position/transfer/order history สูงสุด 100 (ทดสอบ 2026-10-04), copiers 30

## ทดสอบ

```bash
python -m pytest
```
```bash
RUN_LIVE=1 python -m pytest tests/test_live.py -v
```
79 unit test (fake client ไม่ใช้เน็ต) + 12 live test · CI: `.github/workflows/tests.yml` รัน unit test บน Python 3.10–3.12

repo อื่นใช้ fake client ได้:
```python
from binance_copytrade import CopyTradeService, ServiceConfig
from binance_copytrade.testing import BatchFakeClient, freeze_clock
svc = CopyTradeService(ServiceConfig(data_dir=str(tmp_path), delay=0), client=BatchFakeClient())
```

## ข้อควรระวัง

- ตั้ง delay ≥ 0.5 วินาที ตัวเลขบนเว็บ refresh ทุก 1–2 ชม. `sharpRatio` เป็น null เมื่อพอร์ตอายุน้อยกว่า ~30 วัน
- ROI คิดจากทุนของ lead เอง (มักเล็ก) ควรดู `impliedStartEquity`, `copierPnl30d`, `marginBalance/aum` ประกอบ
- GitHub Actions runner อยู่ใน US ซึ่ง binance.com บล็อก ถ้าจะรันอัตโนมัติต้องใช้ self-hosted runner หรือ VPS นอก US
- ใช้วิเคราะห์ส่วนตัว ไม่ใช่คำแนะนำการลงทุน
