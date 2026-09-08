# Confluent Economic Pulse

A compact end-to-end Confluent Cloud demo built around public economic and market data.

## What it demonstrates

Five data sources with very different cadences:

| Source | Metric | Cadence | Default ingestion |
|---|---|---:|---|
| CoinGecko | BTC/ETH price, volume, market cap | every 5 min | Cloudflare Worker |
| U.S. EIA | U.S. electricity demand | hourly | Cloudflare Worker |
| NBP | PLN FX rates | daily | Cloudflare Worker |
| U.S. Treasury Fiscal Data | Debt to the Penny | daily | Cloudflare Worker |
| FRED | Initial Jobless Claims (ICSA) | weekly | Cloudflare Worker |

Default demo path:

```text
Public APIs
    |
    v
Cloudflare Worker Cron collectors
    |
    | HTTPS / Kafka REST Produce API
    v
Confluent Cloud: economic.raw
    |
    v
Confluent Cloud for Apache Flink
    |
    v
economy.dashboard
    |
    v
Confluent HTTP Sink V2
    |
    v
Cloudflare Worker /api/ingest
    |
    v
Cloudflare D1
    |
    v
Simple dashboard page
```

The repository also contains **optional Confluent HTTP Source V2 templates** for NBP, Treasury, FRED and EIA. They are useful when you want Confluent to poll the upstream APIs directly. The Worker path is the default because it normalizes all five sources to one tiny event contract before Kafka and is easier to demo consistently.

## Event contract

Every collector produces a record like:

```json
{
  "source": "nbp",
  "metric": "fx.usd_pln",
  "label": "USD / PLN",
  "value": 3.71,
  "unit": "PLN",
  "observed_at": "2026-09-07T00:00:00Z",
  "collected_at": "2026-09-07T17:05:00Z",
  "dimensions": {"currency": "USD"}
}
```

## Repository layout

```text
cloudflare/
  src/index.ts             Worker: collectors + dashboard + D1 ingestion
  schema.sql               D1 schema
  wrangler.toml            Cron schedules and bindings

confluent/
  flink/01-normalize.sql   Parse raw Kafka JSON into a schemaful Flink table
  connectors/
    http-sink-dashboard.json
    http-source-eia.json
    http-source-nbp.json
    http-source-treasury.json
    http-source-fred.json

docs/
  architecture.md
  setup.md

.env.example
```

## Fastest demo setup

### 1. Create the Kafka topic

Create a topic named:

```text
economic.raw
```

The Cloudflare Worker publishes schemaless JSON to it through Confluent Cloud's Kafka REST Produce API.

### 2. Configure Cloudflare

```bash
cd cloudflare
npm install
npx wrangler d1 create economic-pulse
```

Put the returned D1 database ID into `wrangler.toml`, then:

```bash
npx wrangler d1 execute economic-pulse --remote --file=./schema.sql
```

Set secrets:

```bash
npx wrangler secret put CONFLUENT_REST_ENDPOINT
npx wrangler secret put CONFLUENT_CLUSTER_ID
npx wrangler secret put KAFKA_API_KEY
npx wrangler secret put KAFKA_API_SECRET
npx wrangler secret put DASHBOARD_INGEST_TOKEN
npx wrangler secret put EIA_API_KEY
npx wrangler secret put FRED_API_KEY
```

Deploy:

```bash
npx wrangler deploy
```

### 3. Verify collection manually

```bash
curl "https://YOUR-WORKER.workers.dev/api/collect?source=coingecko" \
  -H "Authorization: Bearer YOUR_DASHBOARD_INGEST_TOKEN"
```

Supported `source` values:

```text
coingecko
eia
nbp
treasury
fred
all
```

### 4. Run the Flink SQL

Open Confluent Cloud for Apache Flink and run:

```text
confluent/flink/01-normalize.sql
```

The raw topic is schemaless, so Confluent exposes it to Flink as a raw table. The SQL parses the JSON payload and writes a schemaful `economy_dashboard` table/topic.

### 5. Create HTTP Sink V2

Use:

```text
confluent/connectors/http-sink-dashboard.json
```

Replace placeholders and create the connector. It POSTs every normalized update to:

```text
https://YOUR-WORKER.workers.dev/api/ingest
```

The Worker upserts the metric into D1.

### 6. Open the dashboard

```text
https://YOUR-WORKER.workers.dev/
```

## Cron design

Cloudflare Cron Triggers are UTC.

```text
*/5 * * * *       CoinGecko
7 * * * *         EIA
15 16 * * *       NBP + Treasury
30 16 * * 4       FRED
```

The exact release time of upstream datasets differs, so these schedules are intentionally simple. The API observation timestamp is shown separately from collection time.

## Direct Confluent collectors

For a more "Confluent-native" version, use the HTTP Source V2 templates under `confluent/connectors/`.

A good split is:

```text
CoinGecko -> Cloudflare -> Kafka
EIA       -> Confluent HTTP Source V2
NBP       -> Confluent HTTP Source V2
Treasury  -> Confluent HTTP Source V2
FRED      -> Confluent HTTP Source V2
```

Why the default repo still uses the Worker for all five: direct HTTP Source responses have five unrelated schemas. A small collector normalizes them before Kafka, which makes the first demo much easier to understand. Once the demo works, move individual sources to HTTP Source V2 to showcase connector-native ingestion.

## Security

Never commit API keys.

The dashboard browser never receives Confluent credentials. Only the Worker can publish to Kafka or accept sink updates.

`/api/ingest` requires `DASHBOARD_INGEST_TOKEN`.

## Demo story

A clean five-minute walkthrough:

1. Show five upstream APIs updating at minute/hour/day/week cadences.
2. Show `economic.raw` receiving a common event contract.
3. Show Flink SQL turning schemaless events into a governed schemaful stream.
4. Show the HTTP Sink updating a serverless read model.
5. Refresh the Cloudflare page and show the latest economic pulse.
