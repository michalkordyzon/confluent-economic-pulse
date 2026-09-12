# Implementation Plan — Economic Pulse

**Based on:** `001-tiny-bloomberg-confluent-data-sources-plan.md`  
**Date:** 2026-09-08  
**Goal:** Translate the design plan into a concrete, ordered build checklist that maps directly to files and commands in this repository.

---

## Current state

The repository already contains a working MVP along the **Cloudflare Worker → Confluent → Flink → HTTP Sink → D1** path:

| Component | Status | File |
|---|---|---|
| Cloudflare Worker collectors (all 5 sources) | ✅ Done | `cloudflare/src/index.ts` |
| D1 schema (`latest_metrics` upsert table) | ✅ Done | `cloudflare/schema.sql` |
| Flink normalize job (`economic.raw` → `economy_dashboard`) | ✅ Done | `confluent/flink/01-normalize.sql` |
| HTTP Sink V2 connector template | ✅ Done | `confluent/connectors/http-sink-dashboard.json` |
| HTTP Source V2 connector templates (EIA, NBP, Treasury, FRED) | ✅ Done (templates only) | `confluent/connectors/http-source-*.json` |
| Dashboard HTML (inline in Worker) | ✅ Done | `cloudflare/src/index.ts` |

What remains is **activating and extending** this foundation: deploying the live connectors, hardening the pipeline, and adding the per-source Flink normalization jobs described in the plan.

---

## Architecture gap: plan vs. current build

The design plan (§31) calls for **Confluent HTTP Source V2 as the primary ingest path** for EIA, NBP, Treasury, and FRED, with the Cloudflare Worker as a fallback/CoinGecko-only path.

The current build uses the **Worker for all five sources** with a single merged `economic.raw` topic.

The plan below preserves the current Worker path as the working default and adds the Confluent-native path alongside it so either can be used for the demo.

---

## Phase 0 — Prerequisites (one-time setup)

> Nothing to code. Gate for all subsequent phases.

- [ ] Confluent Cloud account with:
  - Kafka cluster (Basic or Standard tier)
  - Schema Registry enabled on the environment
  - Flink workspace linked to the cluster
  - Service account with **write** access to `economic.raw` and all `raw.*` topics
- [ ] API keys obtained and stored:
  - EIA free key → `wrangler secret put EIA_API_KEY`
  - FRED free key → `wrangler secret put FRED_API_KEY`
  - CoinGecko Demo key (optional, keyless also works)
- [ ] Cloudflare account with Workers and D1 enabled
- [ ] `wrangler.toml` updated: replace `REPLACE_WITH_D1_DATABASE_ID` with real D1 ID

**Exit condition:** `wrangler dev` starts without errors; D1 schema applied.

---

## Phase 1 — Deploy the Cloudflare Worker (current default path)

> Gets the full pipeline working end-to-end before touching Confluent connectors.

### 1.1 Create and migrate D1

```bash
cd cloudflare
npm install
npx wrangler d1 create economic-pulse
# copy returned database_id into wrangler.toml
npx wrangler d1 execute economic-pulse --remote --file=./schema.sql
```

### 1.2 Set Worker secrets

```bash
npx wrangler secret put CONFLUENT_REST_ENDPOINT   # https://pkc-xxxxx.<region>.<cloud>.confluent.cloud:443
npx wrangler secret put CONFLUENT_CLUSTER_ID       # lkc-xxxxx
npx wrangler secret put KAFKA_API_KEY
npx wrangler secret put KAFKA_API_SECRET
npx wrangler secret put DASHBOARD_INGEST_TOKEN     # generate any random token
npx wrangler secret put EIA_API_KEY
npx wrangler secret put FRED_API_KEY
```

### 1.3 Create Kafka topic

In Confluent Cloud UI or CLI:

```text
Topic name:  economic.raw
Partitions:  1
Retention:   7 days (demo)
```

### 1.4 Deploy and smoke-test

```bash
npx wrangler deploy
curl "https://YOUR-WORKER.workers.dev/api/collect?source=coingecko" \
  -H "Authorization: Bearer YOUR_DASHBOARD_INGEST_TOKEN"
```

Expected response: `{"ok":true,"results":[{"source":"coingecko","count":2}]}`

Check `economic.raw` in Confluent Cloud UI to confirm two records arrived.

### 1.5 Verify all five sources manually

```bash
for src in coingecko eia nbp treasury fred; do
  curl -s "https://YOUR-WORKER.workers.dev/api/collect?source=$src" \
    -H "Authorization: Bearer YOUR_DASHBOARD_INGEST_TOKEN"
done
```

**Exit condition:** All five sources produce events in `economic.raw`. Worker cron triggers are registered in `wrangler.toml`.

---

## Phase 2 — Flink normalization job

> Turns the schemaless `economic.raw` topic into a governed `economy_dashboard` table.

### 2.1 Verify the raw table schema in Flink

In Confluent Cloud for Apache Flink, run:

```sql
SHOW CREATE TABLE `economic.raw`;
```

Confirm the value column is named `val`. If it differs, update `01-normalize.sql` before proceeding.

### 2.2 Run the normalization SQL

Open `confluent/flink/01-normalize.sql` in the Flink SQL workspace and execute it.

This creates:
- `economy_dashboard` table (schemaful, JSON Schema Registry, upsert by `metric`)
- Continuous `INSERT INTO` job consuming from `economic.raw`

### 2.3 Verify Flink output

After triggering a collection, confirm rows appear in `economy_dashboard`:

```sql
SELECT * FROM economy_dashboard LIMIT 10;
```

**Exit condition:** Flink job running; `economy_dashboard` topic has records with all expected fields.

---

## Phase 3 — HTTP Sink V2 connector

> Pushes `economy_dashboard` events into the Cloudflare Worker `/api/ingest` endpoint, populating D1.

### 3.1 Edit the connector template

File: `confluent/connectors/http-sink-dashboard.json`

Replace:
- `<SERVICE_ACCOUNT_ID>` → service account ID from Confluent Cloud
- `<YOUR-WORKER>` → your deployed Worker subdomain
- `<DASHBOARD_INGEST_TOKEN>` → same token as the Worker secret

### 3.2 Create the connector

In Confluent Cloud UI: **Connectors → Add connector → HTTP Sink V2 → Upload config JSON**.

Or via Confluent CLI:

```bash
confluent connect cluster create --config-file confluent/connectors/http-sink-dashboard.json
```

### 3.3 Verify ingest

Check `https://YOUR-WORKER.workers.dev/api/dashboard` — metrics should appear.  
Check `https://YOUR-WORKER.workers.dev/` — dashboard page should show live cards.

**Exit condition:** Dashboard page shows all five metric cards with real data without any manual curl trigger.

---

## Phase 4 — Confluent HTTP Source V2 connectors (native path)

> Optional but recommended for the demo story. Activates the "Confluent-native ingestion" path from the plan.

All four connector templates already exist in `confluent/connectors/`. For each:

1. Replace `<SERVICE_ACCOUNT_ID>` with the real service account ID.
2. Replace any `<*_API_KEY>` placeholder with the real key value (or configure as a connector secret).
3. Create the corresponding raw Kafka topic before deploying the connector.
4. Upload to Confluent Cloud.

### 4.1 EIA

```text
Topic to create first: raw.eia.demand
Template: confluent/connectors/http-source-eia.json
Poll interval: 3600000 ms (1 hour) — already set
Response pointer: /response/data — already set
```

### 4.2 NBP

```text
Topic to create first: raw.nbp.fx
Template: confluent/connectors/http-source-nbp.json
Poll interval: 86400000 ms (24 hours) — already set
Response pointer: /0/rates — already set
Note: returns 404 on non-business days — not a pipeline failure; set behavior.on.error=IGNORE for NBP
```

### 4.3 Treasury

```text
Topic to create first: raw.treasury.debt
Template: confluent/connectors/http-source-treasury.json
Poll interval: 86400000 ms (24 hours) — already set
Response pointer: /data — already set
```

### 4.4 FRED

```text
Topic to create first: raw.fred.icsa
Template: confluent/connectors/http-source-fred.json
Poll interval: 604800000 ms (7 days) — already set
Response pointer: /observations — already set
Note: value field can be "." for missing observations — filter in Flink
```

### 4.5 CoinGecko (try Confluent-native first)

The current codebase uses the Worker path. To try Confluent-native:

```text
Topic to create first: raw.coingecko.market
URL: https://api.coingecko.com
Path: /api/v3/simple/price
Params: ids=bitcoin,ethereum&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true
Poll interval: 300000 ms (5 min)
```

Monitor for HTTP 429 over 24 hours. If rate-limiting is persistent, revert to the Worker path and set `COLLECTOR_MODE = "all"` in `wrangler.toml`.

**If keeping Worker for CoinGecko only:** set `COLLECTOR_MODE = "coingecko-only"` in `wrangler.toml` to prevent duplicate collection for the other four sources.

**Exit condition:** Raw topics for all four sources receiving events without Worker involvement.

---

## Phase 5 — Per-source Flink normalization jobs

> Creates curated topics with the common `EconomicEvent` schema. Not yet implemented.

The plan calls for individual normalization jobs per source. These need to be written.

### 5.1 Create curated topics

```text
curated.electricity
curated.fx
curated.debt
curated.labor
curated.market   (if CoinGecko moves to Confluent-native)
```

Topic settings: 1 partition, `changelog.mode=upsert`, JSON Schema Registry.

### 5.2 Write Flink SQL files

Create in `confluent/flink/`:

| File | Input topic | Output topic | Key |
|---|---|---|---|
| `02-eia-normalize.sql` | `raw.eia.demand` | `curated.electricity` | `period + respondent` |
| `03-nbp-normalize.sql` | `raw.nbp.fx` | `curated.fx` | `currency + effectiveDate` |
| `04-treasury-normalize.sql` | `raw.treasury.debt` | `curated.debt` | `record_date` |
| `05-fred-normalize.sql` | `raw.fred.icsa` | `curated.labor` | `date` |

Each job should:
1. Cast the raw JSON/VARBINARY to `EconomicEvent` shape (`source`, `metric`, `entity`, `period`, `value`, `unit`, `observed_at`, `ingested_at`).
2. Deduplicate using `ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY ingested_at DESC)`.
3. Filter `WHERE FRED.value <> '.'` (FRED-specific null marker).
4. Write to the curated topic with `changelog.mode=upsert`.

### 5.3 Write dashboard aggregation job

Create `confluent/flink/10-dashboard-metrics.sql`:

Merge all curated topics into a single `dashboard.latest_metrics` topic with:

```json
{
  "metric": "...",
  "value": 0.0,
  "unit": "...",
  "period": "...",
  "change_abs": 0.0,
  "change_pct": 0.0,
  "status": "up|down|flat",
  "source": "..."
}
```

**Exit condition:** `dashboard.latest_metrics` topic populated; the existing HTTP Sink can be pointed at this topic instead of `economy_dashboard`.

---

## Phase 6 — Dashboard enhancements

> Extends the current minimal dashboard to show the full planned UI.

Current dashboard shows raw metric cards. The plan calls for:

- Previous value + absolute and percentage change per metric
- Color-coded status indicator (up/down/flat)
- Source freshness indicator (GREEN/YELLOW/RED based on stale thresholds from §21)
- Small history chart (requires D1 to store history, not just latest)

### 6.1 Add history table to D1

Edit `cloudflare/schema.sql` — add:

```sql
CREATE TABLE IF NOT EXISTS metric_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  metric TEXT NOT NULL,
  value REAL NOT NULL,
  observed_at TEXT NOT NULL,
  received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_metric_time ON metric_history(metric, observed_at DESC);
```

Apply migration:
```bash
npx wrangler d1 execute economic-pulse --remote --file=./schema.sql
```

### 6.2 Update `/api/ingest` in `cloudflare/src/index.ts`

In addition to the existing upsert into `latest_metrics`, add an insert into `metric_history` (keep last N rows per metric to bound D1 size).

### 6.3 Add `/api/history?metric=<name>&limit=<n>` endpoint

Returns time-series array for chart rendering.

### 6.4 Update dashboard HTML

- Add delta/percentage display to each card
- Add freshness badge using `received_at` field (already stored in D1)
- Add `<canvas>` sparklines using the new `/api/history` endpoint

**Exit condition:** Dashboard shows change indicators and history sparklines; stale sources visually highlighted.

---

## Phase 7 — Hardening and observability

> Makes the pipeline production-demo-grade.

- [ ] Add dead-letter topic `dlq.eia`, `dlq.nbp`, etc. — update `behavior.on.error` in connector JSON files  
- [ ] Add Worker error logging: failed `produce()` calls should log `source` + error to Cloudflare Logpush or `console.error`  
- [ ] Set `COLLECTOR_MODE = "coingecko-only"` in `wrangler.toml` once Phase 4 Confluent connectors are live  
- [ ] 24–48 hour unattended run: verify all five metrics update on schedule without manual intervention  
- [ ] Confirm NBP 404 on weekends/holidays does not stop the connector  
- [ ] Confirm FRED `value="."` observations are filtered before reaching `curated.labor`  
- [ ] Rotate `DASHBOARD_INGEST_TOKEN` to a strong random value before any public demo

**Exit condition:** All items in §29 Definition of Done are met.

---

## Summary: ordered build checklist

```text
Phase 0   Prerequisites (Confluent cluster, API keys, Cloudflare account)
Phase 1   Deploy Worker + D1 + smoke-test all five sources
Phase 2   Run Flink 01-normalize.sql job
Phase 3   Deploy HTTP Sink V2 connector → dashboard live
Phase 4   Deploy HTTP Source V2 connectors (EIA, NBP, Treasury, FRED, try CoinGecko)
Phase 5   Write per-source Flink normalize + dedup jobs (02–05 + 10)
Phase 6   Dashboard: history table, delta display, sparklines
Phase 7   Hardening: DLQ, logging, 48h unattended test
```

Phases 1–3 deliver a working end-to-end demo.  
Phases 4–5 migrate to the Confluent-native ingestion path the plan prescribes.  
Phases 6–7 polish the demo for a live presentation.

---

## Files to create in subsequent phases

| File | Phase | Description |
|---|---|---|
| `confluent/flink/02-eia-normalize.sql` | 5 | EIA raw → curated.electricity |
| `confluent/flink/03-nbp-normalize.sql` | 5 | NBP raw → curated.fx |
| `confluent/flink/04-treasury-normalize.sql` | 5 | Treasury raw → curated.debt |
| `confluent/flink/05-fred-normalize.sql` | 5 | FRED raw → curated.labor |
| `confluent/flink/10-dashboard-metrics.sql` | 5 | All curated → dashboard.latest_metrics |
| `cloudflare/schema.sql` (updated) | 6 | Add metric_history table |
| `cloudflare/src/index.ts` (updated) | 6 | History insert + /api/history endpoint |
