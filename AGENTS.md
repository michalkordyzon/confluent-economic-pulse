# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Stack

- **Cloudflare Worker** (TypeScript, single file: `cloudflare/src/index.ts`) — collectors + HTTP API + dashboard HTML
- **Confluent Cloud** — Kafka topic `economic.raw`, Flink SQL, HTTP Sink V2
- **Cloudflare D1** — read model (`latest_metrics` table, upsert-only by `metric` PK)
- No test framework. No linter config. TypeScript only via `tsc --noEmit`.

## Commands

All commands run from `cloudflare/`:

```bash
npm install
npx wrangler dev          # local dev (Worker)
npx wrangler deploy       # deploy to Cloudflare
npx wrangler types        # regenerate worker-configuration.d.ts
npx tsc --noEmit          # type-check only (no build step — wrangler bundles src/index.ts directly)
```

D1 setup (run once):
```bash
npx wrangler d1 create economic-pulse
npx wrangler d1 execute economic-pulse --remote --file=./schema.sql
```

Manual collection test:
```bash
curl "https://YOUR-WORKER.workers.dev/api/collect?source=coingecko" \
  -H "Authorization: Bearer YOUR_DASHBOARD_INGEST_TOKEN"
```

## Critical patterns

- **Single-file Worker**: all logic lives in `cloudflare/src/index.ts`. No modules, no imports between files.
- **Event contract** (`EconomicEvent` type): every collector produces `{source, metric, label, value, unit, observed_at, collected_at, dimensions}`. The `metric` field is the D1 primary key — upsert semantics throughout.
- **Confluent Produce API wrapping**: the REST payload must be `{value: {type: "JSON", data: event}}`. The API can return HTTP 200 with a per-record `error_code` — checked explicitly after every produce call.
- **`/api/ingest` parsing**: accepts both JSON array and newline-delimited JSON (NDJSON) — HTTP Sink V2 batching can send either. The `extractEvents()` function handles both.
- **Ingest unwrapping**: the HTTP Sink V2 wraps records as `{value: <event>}` — the ingest handler does `raw?.value ?? raw` to normalize.
- **`asIso()` helper**: normalizes timestamps from any upstream format (Unix seconds, Unix ms, ISO string, or null) to ISO-8601. Unix epoch detection threshold: `> 10_000_000_000` = milliseconds.
- **`COLLECTOR_MODE` env var**: set to `"coingecko-only"` in `wrangler.toml` when EIA/NBP/Treasury/FRED are moved to Confluent HTTP Source V2, to prevent duplicate collection.

## Confluent / Flink

- `economic.raw` topic is **schemaless** (no Schema Registry). Flink sees it as a raw table with `VARBINARY` columns (`key`/`val`). Use `SHOW CREATE TABLE \`economic.raw\`;` to confirm column name before editing `01-normalize.sql`.
- `economy_dashboard` uses `changelog.mode = 'upsert'` with JSON Schema Registry — requires Schema Registry to be enabled on the Confluent environment.
- HTTP Sink V2 sends `Authorization` as a sensitive header — set in connector JSON as `api1.http.request.sensitive.headers`.

## Secrets (Cloudflare)

`CONFLUENT_REST_ENDPOINT`, `CONFLUENT_CLUSTER_ID`, `KAFKA_API_KEY`, `KAFKA_API_SECRET`, `DASHBOARD_INGEST_TOKEN`, `EIA_API_KEY`, `FRED_API_KEY`. NBP and U.S. Treasury require no API keys.

## wrangler.toml

`database_id` under `[[d1_databases]]` must be replaced with the actual D1 ID after `wrangler d1 create`. It ships as `"REPLACE_WITH_D1_DATABASE_ID"`.
