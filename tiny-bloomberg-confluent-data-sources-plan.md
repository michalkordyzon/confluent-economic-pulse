# Tiny Bloomberg — Confluent Data Ingestion Implementation Plan

**Date:** 2026-09-08  
**Goal:** Build a small, always-on economic/market data platform where Confluent Cloud performs most ingestion and stream processing, while Cloudflare hosts the web application and is used only where a lightweight collector/proxy is useful.

---

## 1. Target architecture

```text
                           ┌─────────────────────────────┐
                           │        DATA SOURCES         │
                           │                             │
                           │ EIA   NBP   Treasury  FRED  │
                           │              CoinGecko      │
                           └──────────────┬──────────────┘
                                          │
                       ┌──────────────────┴──────────────────┐
                       │                                     │
                       ▼                                     ▼
             Confluent HTTP Source V2              Cloudflare Worker
             (preferred path)                      (CoinGecko fallback)
                       │                                     │
                       │                         Confluent Kafka REST API
                       │                                     │
                       └──────────────────┬──────────────────┘
                                          ▼
                               Confluent Kafka topics
                                          │
                                          ▼
                                    Flink SQL
                             normalize / deduplicate /
                              aggregate / calculate
                                          │
                                          ▼
                                derived Kafka topics
                                          │
                             ┌────────────┴────────────┐
                             ▼                         ▼
                    historical storage         serving layer/cache
                    (optional later)           for dashboard
                                                      │
                                                      ▼
                                           Cloudflare Worker API
                                                      │
                                                      ▼
                                           Cloudflare Pages app
```

### Main design principle

Use **Confluent-native ingestion whenever the external API is a conventional HTTPS REST API**.

Cloudflare should not become a mandatory ingestion layer unless the external API needs custom logic, special retry behavior, authentication handling, or protection from unstable rate limits.

---

# 2. The five data sources

| Source | Data | Target cadence | Authentication | Preferred ingestion |
|---|---|---:|---|---|
| U.S. EIA | Electricity operating data | 5–15 min polling; source observations hourly | Free API key | Confluent HTTP Source V2 |
| CoinGecko | BTC / crypto market signal | 5–15 min | Keyless public or free Demo API key | Try Confluent direct; Cloudflare Worker fallback |
| NBP | PLN FX rates / gold | Daily | None | Confluent HTTP Source V2 |
| U.S. Treasury Fiscal Data | Debt to the Penny | Daily | None | Confluent HTTP Source V2 |
| FRED | Initial Jobless Claims (`ICSA`) | Weekly | Free API key | Confluent HTTP Source V2 |

This gives the demo deliberately different update patterns:

- **High-frequency / frequently polled:** EIA + CoinGecko
- **Daily:** NBP + U.S. Treasury
- **Weekly:** FRED Initial Jobless Claims

---

# 3. Kafka topic design

Create raw topics first. Keep source payloads close to the original structure.

```text
raw.eia.electricity
raw.coingecko.market
raw.nbp.fx
raw.treasury.debt
raw.fred.jobless_claims
```

Then produce normalized topics:

```text
curated.electricity
curated.market
curated.fx
curated.debt
curated.labor
```

Finally create one compact dashboard topic:

```text
dashboard.latest_metrics
```

Recommended initial topic settings:

- partitions: `1` for each raw topic
- replication: Confluent-managed default
- retention: 30–90 days for raw demo topics
- compression: broker/default
- key: deterministic natural key for each observation
- value: JSON initially

For a small demo, **JSON is sufficient**. Once the pipeline works, add JSON Schema or Avro through Schema Registry.

---

# 4. Common event envelope

Normalize every source into a common logical structure.

```json
{
  "source": "eia",
  "metric": "electricity_demand",
  "entity": "US48",
  "period": "2026-09-08T12:00:00Z",
  "value": 432100,
  "unit": "MW",
  "observed_at": "2026-09-08T12:00:00Z",
  "ingested_at": "2026-09-08T12:07:31Z"
}
```

Recommended logical fields:

| Field | Meaning |
|---|---|
| `source` | Source system |
| `metric` | Stable internal metric name |
| `entity` | Region, currency, instrument, etc. |
| `period` | Period represented by the observation |
| `value` | Numeric value |
| `unit` | MW, PLN, USD, claims, percent, etc. |
| `observed_at` | Timestamp/date from source |
| `ingested_at` | Kafka ingestion time |

Use a key such as:

```text
source|metric|entity|period
```

Example:

```text
eia|electricity_demand|US48|2026-09-08T12:00:00Z
```

This gives deterministic deduplication.

---

# 5. Source 1 — U.S. EIA electricity

## Purpose

This is the main **streaming-looking operational source** in the demo.

Suggested dataset:

**U.S. Electric System Operating Data — balancing authority / region data**

Official API area:

```text
https://api.eia.gov/v2/electricity/rto/region-data/data/
```

Documentation:

- https://www.eia.gov/opendata/
- https://www.eia.gov/opendata/browser/electricity/rto/region-data

## Cadence

The source itself contains hourly observations.

Recommended connector polling:

```text
every 5 minutes
```

This is intentional: the connector checks frequently, but only new source periods should survive downstream deduplication.

## Authentication

Free EIA API key.

Store the key as a connector secret; do not place it in Git.

## Confluent ingestion

Use **HTTP Source V2 Connector**.

Conceptual configuration:

```json
{
  "connector.class": "HttpSourceV2",
  "http.api.base.url": "https://api.eia.gov",
  "apis.num": "1",

  "api1.http.api.path": "/v2/electricity/rto/region-data/data/",
  "api1.http.request.method": "GET",
  "api1.topics": "raw.eia.electricity",

  "api1.http.response.data.json.pointer": "/response/data",
  "api1.request.interval.ms": "300000",

  "output.data.format": "JSON"
}
```

Add the EIA query parameters required for:

- `frequency=hourly`
- desired fields
- desired regions
- newest observation window
- ascending or descending period ordering

## Important implementation issue: incremental ingestion

EIA is a REST API, not a Kafka/WebSocket source.

Do **not** simply request a large historical range every five minutes.

Preferred sequence:

1. During bootstrap, retrieve a small historical window.
2. Extract the EIA observation `period`.
3. Use it as the record identity.
4. Configure Confluent timestamp/chaining offset mode if the EIA request parameters map cleanly to it.
5. Regardless of connector offset mode, deduplicate by `period + region + metric` in Flink.

### Safe first implementation

For the first working version:

- request only the latest small time window,
- poll every 5 minutes,
- allow duplicate raw events,
- deduplicate in Flink.

This is simpler and more reliable than over-engineering connector pagination before the demo works.

## Flink output

Example logical result:

```text
curated.electricity
```

Fields:

```text
region
period
demand_mw
generation_mw
net_generation_mw
source
ingested_at
```

Potential derived metrics:

- current demand
- change vs previous hour
- 24-hour average
- distance from 24-hour high
- demand trend indicator

---

# 6. Source 2 — CoinGecko

## Purpose

Provides a continuously changing market signal.

Suggested initial instruments:

```text
bitcoin
ethereum
```

Useful fields:

- USD price
- market cap
- 24h volume
- 24h change
- `last_updated_at`

Endpoint family:

```text
GET /api/v3/simple/price
```

Example base:

```text
https://api.coingecko.com/api/v3/simple/price
```

Documentation:

- https://docs.coingecko.com/reference/simple-price
- https://docs.coingecko.com/docs/keyless-public-api
- https://docs.coingecko.com/docs/setting-up-your-api-key

## Cadence

Recommended:

```text
every 5 minutes
```

This is only:

```text
12 calls/hour
288 calls/day
```

per combined request if BTC and ETH are fetched together.

## Option A — direct Confluent HTTP Source

Try this **first**.

Conceptual request:

```text
GET https://api.coingecko.com/api/v3/simple/price
    ?ids=bitcoin,ethereum
    &vs_currencies=usd
    &include_market_cap=true
    &include_24hr_vol=true
    &include_24hr_change=true
    &include_last_updated_at=true
```

Use the Demo API key if available rather than relying permanently on anonymous keyless access.

### Why direct Confluent is attractive

It keeps the architecture consistent:

```text
CoinGecko → HTTP Source V2 → Kafka
```

No extra code is required.

## Option B — Cloudflare Worker fallback

CoinGecko's keyless public API uses dynamic IP-based throttling.

If direct Confluent polling proves unreliable, use:

```text
Cloudflare Cron Trigger
        ↓
Cloudflare Worker
        ↓
CoinGecko
        ↓
Confluent Kafka REST API
        ↓
raw.coingecko.market
```

Confluent Cloud exposes:

```text
POST /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records
```

so a Cloudflare Worker can produce JSON to Kafka using HTTPS without running a Kafka client.

Keep these secrets in Cloudflare Worker secrets:

```text
CONFLUENT_REST_ENDPOINT
CONFLUENT_CLUSTER_ID
CONFLUENT_API_KEY
CONFLUENT_API_SECRET
COINGECKO_API_KEY
```

Never expose them to the browser.

## Deduplication key

```text
coingecko|bitcoin|<last_updated_at>
```

and:

```text
coingecko|ethereum|<last_updated_at>
```

---

# 7. Source 3 — NBP FX

## Purpose

Adds a European/Polish macro-financial source and makes the demo geographically broader.

NBP exposes a public HTTPS API returning JSON/XML.

Documentation:

```text
https://api.nbp.pl/en.html
```

Recommended initial currencies:

```text
USD/PLN
EUR/PLN
CHF/PLN
GBP/PLN
```

## Endpoint

For the entire current Table A:

```text
https://api.nbp.pl/api/exchangerates/tables/A/
```

Or retrieve currencies individually:

```text
https://api.nbp.pl/api/exchangerates/rates/A/USD/
https://api.nbp.pl/api/exchangerates/rates/A/EUR/
```

## Cadence

NBP values are daily.

Recommended polling:

```text
every 1 hour during weekdays
```

or, for a cleaner demo:

```text
once daily after expected NBP publication
```

I prefer hourly polling with downstream deduplication because it makes the pipeline self-healing if publication timing changes.

## Authentication

None.

## Confluent ingestion

Use HTTP Source V2 directly.

```text
NBP → Confluent HTTP Source V2 → raw.nbp.fx
```

## Deduplication key

Use:

```text
currency_code + effectiveDate
```

Example:

```text
USD|2026-09-08
```

## Flink normalization

Turn nested NBP table payloads into records such as:

```json
{
  "source": "nbp",
  "metric": "fx_mid",
  "entity": "USDPLN",
  "period": "2026-09-08",
  "value": 3.87,
  "unit": "PLN"
}
```

---

# 8. Source 4 — U.S. Treasury Debt to the Penny

## Purpose

Provides a slowly moving but very recognizable U.S. fiscal indicator.

Dataset:

**Debt to the Penny**

Official page:

```text
https://fiscaldata.treasury.gov/datasets/debt-to-the-penny/
```

Important fields:

```text
record_date
debt_held_public_amt
intragov_hold_amt
tot_pub_debt_out_amt
```

## Cadence

Dataset is released daily / business-day oriented.

Recommended polling:

```text
once every 6 hours
```

Even though the value normally changes once per business day, this makes the collector resilient to publication timing.

## Authentication

None.

## Confluent ingestion

```text
Treasury Fiscal Data
        ↓
Confluent HTTP Source V2
        ↓
raw.treasury.debt
```

Request only the newest records, sorted descending by date.

Conceptually:

```text
filter = newest date/window
sort   = -record_date
page   = small
```

Avoid pulling the complete dataset every time.

## Deduplication key

```text
record_date
```

## Flink output

```json
{
  "source": "treasury",
  "metric": "total_public_debt",
  "entity": "US",
  "period": "2026-09-08",
  "value": 0,
  "unit": "USD"
}
```

Possible derived metrics:

- daily debt change
- 30-day change
- year-to-date change

---

# 9. Source 5 — FRED Initial Jobless Claims

## Purpose

This is intentionally the **rare/weekly source**.

Series:

```text
ICSA
```

This represents Initial Claims.

API endpoint:

```text
https://api.stlouisfed.org/fred/series/observations
```

Required query parameters:

```text
series_id=ICSA
api_key=<FRED_API_KEY>
file_type=json
```

Documentation:

- https://fred.stlouisfed.org/docs/api/fred/series_observations.html
- https://fred.stlouisfed.org/docs/api/api_key.html

## Cadence

The series is weekly.

Recommended polling:

```text
every 6 hours on release day
```

Simpler alternative:

```text
once daily
```

For this small system, daily polling is perfectly acceptable because duplicates will be removed downstream.

## Authentication

Free FRED API key.

## Confluent ingestion

```text
FRED
 ↓
HTTP Source V2
 ↓
raw.fred.jobless_claims
```

Set:

```text
file_type=json
series_id=ICSA
```

Request only the most recent observation window.

## Response pointer

FRED returns observations inside:

```text
/observations
```

So the connector can map this JSON array directly into Kafka records.

## Deduplication key

```text
ICSA|date
```

Example:

```text
ICSA|2026-09-05
```

---

# 10. Confluent HTTP Source V2 configuration strategy

Current Confluent HTTP Source V2 supports:

- periodic HTTP requests,
- GET query parameters,
- custom headers,
- JSON response pointers,
- retry/backoff,
- offsets,
- chaining,
- cursor pagination,
- timestamp-based pagination,
- configurable polling interval.

Documentation:

```text
https://docs.confluent.io/cloud/current/connectors/cc-http-source-v2.html
```

For each connector configure at minimum:

```text
base URL
API path
HTTP method
request parameters
authentication
response JSON pointer
target Kafka topic
request interval
retry policy
output format
```

Recommended retry policy:

```text
max retries: 5–10
backoff: exponential with jitter
retry: 429 and 5xx
```

---

# 11. Raw vs curated data

Do not normalize everything inside the HTTP connector.

Keep the ingestion layer simple:

```text
REST response
   ↓
RAW Kafka topic
   ↓
Flink SQL
   ↓
CURATED Kafka topic
```

This makes debugging dramatically easier.

If EIA suddenly changes a field or NBP returns unexpected content, you still have the original event available in the raw topic.

---

# 12. Flink SQL processing

Use Flink for three jobs:

## A. Normalization

Convert every source into the common metric model.

## B. Deduplication

For example conceptually:

```sql
ROW_NUMBER() OVER (
    PARTITION BY source, metric, entity, period
    ORDER BY ingested_at DESC
)
```

Keep row `1`.

## C. Derived indicators

Examples:

### Electricity

```text
current demand
1h change
24h change
24h average
```

### Crypto

```text
BTC price
24h %
volume
```

### FX

```text
USD/PLN
EUR/PLN
daily change
```

### Treasury

```text
total debt
daily change
30-day change
```

### Labor

```text
initial claims
weekly change
4-week trend
```

---

# 13. Dashboard topic

Create a compact topic containing the newest state of each metric:

```text
dashboard.latest_metrics
```

Example messages:

```json
{
  "metric": "us_electricity_demand",
  "value": 432100,
  "unit": "MW",
  "period": "2026-09-08T12:00:00Z",
  "status": "up"
}
```

```json
{
  "metric": "btc_usd",
  "value": 112500,
  "unit": "USD",
  "period": "2026-09-08T12:15:00Z",
  "status": "down"
}
```

This prevents the frontend from needing to understand five different schemas.

---

# 14. Serving the web application

Confluent should **not** host the frontend.

Use:

```text
Cloudflare Pages
```

for HTML/CSS/JavaScript and:

```text
Cloudflare Worker
```

for the backend/API layer.

The browser should never hold Kafka credentials.

Recommended flow:

```text
Browser
  ↓
GET /api/metrics
  ↓
Cloudflare Worker
  ↓
serving store / cached latest values
  ↓
JSON
```

---

# 15. Do not make the browser query Kafka directly

Avoid:

```text
Browser → Confluent Kafka REST API
```

because this would expose credentials and couple the UI tightly to Kafka.

Use:

```text
Browser
  ↓
Cloudflare Worker
  ↓
safe serving layer
```

---

# 16. Serving-store choice

For version 1, use a tiny Cloudflare store for the latest dashboard state.

Good candidates:

```text
Cloudflare KV
```

or:

```text
Cloudflare D1
```

Suggested choice:

**D1** if you want history/querying.

**KV** if you only want latest values.

For Tiny Bloomberg I would start with **D1**, because a small time-series history immediately allows charts.

Example table:

```sql
CREATE TABLE metrics (
    source TEXT NOT NULL,
    metric TEXT NOT NULL,
    entity TEXT NOT NULL,
    period TEXT NOT NULL,
    value REAL,
    unit TEXT,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (source, metric, entity, period)
);
```

---

# 17. How curated Confluent data reaches Cloudflare

There are two sensible designs.

## Version 1 — simple pull

Cloudflare Worker periodically retrieves the newest curated records and writes them into D1.

This is easiest to understand for a demo.

## Version 2 — event-driven push

A consumer/service consumes curated Kafka records and updates D1 immediately.

This is more real-time but introduces more components.

### Recommendation

Start with **Version 1**.

The important learning target is Confluent ingestion + Kafka + Flink, not building an elaborate serving stack.

---

# 18. Proposed repository structure

```text
tiny-bloomberg/
│
├── README.md
│
├── docs/
│   ├── architecture.md
│   ├── data-sources.md
│   └── runbook.md
│
├── confluent/
│   ├── connectors/
│   │   ├── eia.json
│   │   ├── coingecko.json
│   │   ├── nbp.json
│   │   ├── treasury.json
│   │   └── fred.json
│   │
│   ├── flink/
│   │   ├── 01_eia_normalize.sql
│   │   ├── 02_coingecko_normalize.sql
│   │   ├── 03_nbp_normalize.sql
│   │   ├── 04_treasury_normalize.sql
│   │   ├── 05_fred_normalize.sql
│   │   └── 10_dashboard_metrics.sql
│   │
│   └── schemas/
│       └── metric.schema.json
│
├── workers/
│   ├── coingecko-collector/
│   └── dashboard-api/
│
├── web/
│   ├── index.html
│   ├── app.js
│   └── style.css
│
└── .github/
    └── workflows/
```

---

# 19. Environment variables / secrets

## Confluent

```text
EIA_API_KEY
FRED_API_KEY
COINGECKO_API_KEY
```

## Cloudflare

If CoinGecko fallback collector is used:

```text
COINGECKO_API_KEY
CONFLUENT_REST_ENDPOINT
CONFLUENT_CLUSTER_ID
CONFLUENT_API_KEY
CONFLUENT_API_SECRET
```

Never commit real secret values.

Include:

```text
.env.example
```

with placeholders only.

---

# 20. Implementation sequence

## Phase 1 — Confluent foundation

Create:

```text
Kafka cluster
topics
service account
Kafka API credentials
Schema Registry
Flink workspace
```

Then manually produce one test JSON event and confirm it appears in the topic.

**Exit condition:** Kafka path works.

---

## Phase 2 — EIA first

Build EIA before the other sources because it proves the most important pattern:

```text
external API
→ Confluent connector
→ Kafka
→ Flink
```

Steps:

1. Obtain EIA API key.
2. Test endpoint manually with `curl`.
3. Create `raw.eia.electricity`.
4. Configure HTTP Source V2.
5. Poll every 5 minutes.
6. Confirm raw events.
7. Create Flink normalization.
8. Deduplicate by `period + region + metric`.
9. Create `curated.electricity`.

**Exit condition:** new EIA observations automatically arrive without external cron.

---

## Phase 3 — NBP

1. Test Table A endpoint.
2. Create connector.
3. Write to `raw.nbp.fx`.
4. Flatten the `rates` array.
5. Keep USD, EUR, CHF, GBP.
6. Deduplicate using `code + effectiveDate`.
7. Produce `curated.fx`.

**Exit condition:** current PLN FX rates appear automatically.

---

## Phase 4 — U.S. Treasury

1. Test Debt to the Penny endpoint.
2. Configure request for recent records only.
3. Create `raw.treasury.debt`.
4. Deduplicate by `record_date`.
5. Convert numeric strings to numeric values.
6. Calculate daily delta.
7. Produce `curated.debt`.

**Exit condition:** newest debt figure is maintained automatically.

---

## Phase 5 — FRED

1. Create FRED account/API key.
2. Test `ICSA`.
3. Create HTTP connector.
4. Extract `/observations`.
5. Keep newest observations.
6. Deduplicate by observation date.
7. Produce `curated.labor`.

**Exit condition:** weekly labor release automatically enters Kafka.

---

## Phase 6 — CoinGecko

Try Confluent direct first.

1. Create Demo key if desired.
2. Poll BTC + ETH in one request.
3. Poll every 5 minutes.
4. Observe rate-limit behavior.
5. If stable, keep Confluent-native ingestion.
6. If repeated `429`/IP throttling occurs, deploy Cloudflare collector.
7. Worker posts records into Confluent Kafka REST API.

**Exit condition:** stable 24-hour collection without manual intervention.

---

## Phase 7 — unified metrics

Create Flink logic producing:

```text
dashboard.latest_metrics
```

Initial five headline cards:

```text
US Electricity Demand
Bitcoin Price
USD/PLN
US Public Debt
US Initial Jobless Claims
```

Each card should expose:

```text
current value
previous value
absolute change
percentage change
timestamp
source
```

---

## Phase 8 — Cloudflare app

Create a minimal dashboard.

Suggested layout:

```text
┌─────────────────────────────────────────────────────────┐
│ TINY BLOOMBERG                            LIVE ●         │
├─────────────────────────────────────────────────────────┤
│ Electricity    BTC/USD      USD/PLN                     │
│ 432 GW         $112,500     3.87                        │
│ +2.1%          -1.3%        +0.2%                       │
├─────────────────────────────────────────────────────────┤
│ US DEBT                         JOBLESS CLAIMS           │
│ $XX.XXT                         2XXk                     │
│ +$X.XB/day                      -Xk w/w                  │
├─────────────────────────────────────────────────────────┤
│                   small history chart                   │
└─────────────────────────────────────────────────────────┘
```

Keep UI intentionally small.

The architecture is the demo — not the CSS.

---

# 21. Observability

For every collector/connector track:

```text
last successful fetch
last source timestamp
records produced
HTTP status
retry count
lag from source
```

Create a simple internal health model:

```text
GREEN  = fresh
YELLOW = delayed
RED    = stale
```

Suggested stale thresholds:

| Source | Yellow | Red |
|---|---:|---:|
| EIA | >2 h | >4 h |
| CoinGecko | >15 min | >30 min |
| NBP | >1 business day | >2 business days |
| Treasury | >2 business days | >3 business days |
| FRED ICSA | >8 days | >10 days |

---

# 22. Failure handling

## HTTP 429

Use exponential backoff.

Do not circumvent rate limits.

## HTTP 5xx

Retry with exponential backoff + jitter.

## 404

Interpret source-by-source.

For example, NBP's `today` endpoint can return 404 when today's table has not yet been published.

That is not necessarily a pipeline failure.

## Invalid JSON/schema change

Keep the raw event and send malformed/unhandled messages to:

```text
dlq.source_name
```

Example:

```text
dlq.eia
```

---

# 23. Backfill strategy

Do not mix large backfills with live ingestion initially.

Create separate scripts/jobs for history.

Example:

```text
backfill/
  eia.py
  nbp.py
  treasury.py
  fred.py
```

Backfill into separate topics:

```text
backfill.eia.electricity
```

Then merge deliberately.

The live connectors should remain small and predictable.

---

# 24. Security

Minimum rules:

1. No source API keys in Git.
2. No Confluent credentials in browser JavaScript.
3. Use dedicated Confluent service accounts.
4. Give source connectors write access only to required topics.
5. Use separate credentials for Cloudflare Worker → Confluent REST.
6. Store Cloudflare secrets with Worker secrets.
7. Rotate keys if exposed.
8. Do not log Authorization headers.

---

# 25. Cost control

Keep the demo intentionally tiny.

Use:

```text
5 sources
1 partition/source initially
small messages
short raw retention
few Flink statements
one Cloudflare Worker/API
one tiny D1 database
```

Do not create dozens of topics or high partition counts.

At this scale, the project demonstrates architecture rather than throughput.

---

# 26. What the demo should prove

The strongest story is:

```text
1. Public external APIs are ingested automatically.
2. Confluent turns polling APIs into continuously updated Kafka streams.
3. Kafka decouples acquisition from processing.
4. Flink normalizes different temporal patterns and schemas.
5. High-frequency, daily and weekly observations coexist in one event model.
6. Data quality and deduplication are explicit.
7. A tiny Cloudflare application consumes the resulting state.
```

This is much stronger than presenting five independent cron jobs.

---

# 27. MVP scope

For the first complete release, stop here:

```text
EIA
CoinGecko
NBP
Treasury
FRED
   ↓
Kafka
   ↓
Flink
   ↓
latest-metrics
   ↓
Cloudflare
   ↓
dashboard
```

Do **not** add yet:

- LLMs
- RAG
- forecasting
- anomaly detection
- watsonx.data
- vector databases
- complex historical warehouse
- dozens of indicators

Those can become later stages.

The first objective is:

> **A boring, reliable pipeline that runs continuously without you touching it.**

---

# 28. Recommended build order

```text
1. Confluent Kafka cluster + topics
2. EIA HTTP Source V2
3. EIA Flink normalization
4. NBP connector
5. Treasury connector
6. FRED connector
7. CoinGecko direct connector
8. CoinGecko Cloudflare fallback only if necessary
9. common metric schema
10. Flink dashboard topic
11. Cloudflare D1/KV serving layer
12. Cloudflare Worker API
13. Cloudflare Pages UI
14. monitoring / stale-source indicators
15. 24–48 hour unattended test
```

---

# 29. Definition of done

The MVP is complete when:

- all five sources operate without manual triggering;
- EIA and CoinGecko are collected repeatedly;
- NBP and Treasury update automatically;
- FRED accepts a weekly release without code changes;
- duplicate observations do not create duplicate curated records;
- connector failures retry automatically;
- every dashboard metric exposes its source timestamp;
- the web page contains no API secrets;
- after a connector restart, ingestion resumes correctly;
- the system survives at least 24–48 hours unattended.

---

# 30. Key technical references

## Confluent

HTTP Source V2 Connector:

https://docs.confluent.io/cloud/current/connectors/cc-http-source-v2.html

Kafka REST API:

https://docs.confluent.io/cloud/current/kafka-rest/kafka-rest-cc.html

Produce Records:

https://docs.confluent.io/cloud/current/ccloud/produce-record/

## EIA

https://www.eia.gov/opendata/

https://www.eia.gov/opendata/browser/electricity/rto/region-data

## CoinGecko

https://docs.coingecko.com/reference/simple-price

https://docs.coingecko.com/docs/keyless-public-api

https://docs.coingecko.com/docs/setting-up-your-api-key

## NBP

https://api.nbp.pl/en.html

## U.S. Treasury Fiscal Data

https://fiscaldata.treasury.gov/datasets/debt-to-the-penny/

## FRED

https://fred.stlouisfed.org/docs/api/fred/series_observations.html

https://fred.stlouisfed.org/docs/api/api_key.html

---

# 31. Final architecture decision

For the MVP:

```text
EIA ----------┐
NBP ----------│
Treasury -----├── Confluent HTTP Source V2 ──┐
FRED ---------│                               │
CoinGecko ----┘ (try direct first)            │
                                               ▼
                                             Kafka
                                               │
                                               ▼
                                             Flink
                                               │
                                               ▼
                                      dashboard.latest_metrics
                                               │
                                               ▼
                                       Cloudflare serving
                                               │
                                               ▼
                                         Tiny Bloomberg
```

Cloudflare ingestion is therefore **not part of the normal path**.

It becomes a targeted adapter only when a source cannot be collected reliably using Confluent HTTP Source V2.

That keeps the project centered on Confluent, which is exactly what this demo should demonstrate.
