# Setup notes

## Confluent Cloud

You need:

- Kafka cluster
- topic `economic.raw`
- Flink workspace
- Schema Registry for the `economy_dashboard` JSON Schema table
- service account for the HTTP Sink V2 connector

The Cloudflare Worker uses the cluster-scoped Kafka REST endpoint, not the Confluent control-plane API.

Example REST endpoint shape:

```text
https://pkc-xxxxx.<region>.<cloud>.confluent.cloud:443
```

## Cloudflare secrets

```bash
wrangler secret put CONFLUENT_REST_ENDPOINT
wrangler secret put CONFLUENT_CLUSTER_ID
wrangler secret put KAFKA_API_KEY
wrangler secret put KAFKA_API_SECRET
wrangler secret put DASHBOARD_INGEST_TOKEN
wrangler secret put EIA_API_KEY
wrangler secret put FRED_API_KEY
```

## API keys

NBP and U.S. Treasury do not need keys for the endpoints used here.

EIA and FRED use free API keys.

CoinGecko is configured with its public endpoint. If the public endpoint starts requiring credentials for your usage tier, add the appropriate demo API header as a Worker secret rather than embedding it in source code.

## Collector mode

`wrangler.toml` defaults to:

```toml
COLLECTOR_MODE = "all"
```

If you move EIA/NBP/Treasury/FRED to Confluent HTTP Source V2, change it to:

```toml
COLLECTOR_MODE = "coingecko-only"
```

That prevents duplicate collection.
