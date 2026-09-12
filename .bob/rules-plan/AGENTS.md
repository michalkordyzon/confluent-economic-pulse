# Project Architecture Rules (Non-Obvious Only)

- **D1 is a read model, not a source of truth**: the canonical data is in the `economy_dashboard` Kafka/Flink topic. D1 is only the latest-state cache for the dashboard UI. Do not build reporting or history queries on D1.
- **The Worker is stateless and idempotent**: all state flows Confluent → HTTP Sink → D1. The `metric` upsert means re-delivering the same event is safe.
- **Two separate auth surfaces**: `/api/collect` and `/api/ingest` both require `DASHBOARD_INGEST_TOKEN` via `Authorization: Bearer`. Kafka produce uses HTTP Basic auth (`KAFKA_API_KEY:KAFKA_API_SECRET`). These are independent secrets.
- **Flink job is a streaming INSERT, not a batch**: `01-normalize.sql` runs as a continuous Flink job. Stopping it stops the flow from `economic.raw` → `economy_dashboard`. Restarting it replays from the current watermark.
- **NDJSON tolerance is intentional architecture**: HTTP Sink V2's batch mode may send NDJSON. The `extractEvents()` fallback path is load-bearing for high-throughput operation — do not remove it.
- **`COLLECTOR_MODE` controls dual-ingestion prevention**: if sources are migrated to Confluent HTTP Source V2 one-by-one, the Worker and the connectors would double-collect. Set `COLLECTOR_MODE = "coingecko-only"` to limit the Worker to CoinGecko only.
- **No horizontal scaling concern**: `tasks.max = 1` on the HTTP Sink V2 and D1 upsert-by-PK means concurrency is not a concern at demo scale.
