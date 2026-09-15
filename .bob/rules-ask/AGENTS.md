# Project Documentation Rules (Non-Obvious Only)

- **`economic.raw` is schemaless**: the Kafka topic has no Schema Registry subject. Confluent Flink exposes it as a raw table with `VARBINARY` columns — run `SHOW CREATE TABLE \`economic.raw\`;` before editing the Flink SQL to confirm the value column name (`val`).
- **`economy_dashboard` requires Schema Registry**: unlike `economic.raw`, this output table uses `json-registry` format. The Confluent environment must have Schema Registry enabled.
- **HTTP Sink V2 vs HTTP Source V2 are separate**: sink = Confluent → Worker; source = upstream APIs → Confluent. The repo ships templates for both but the Worker path is the default for all five sources.
- **Connector JSON files have placeholders**: `confluent/connectors/*.json` contain `<SERVICE_ACCOUNT_ID>`, `<YOUR-WORKER>`, `<DASHBOARD_INGEST_TOKEN>` — they must be replaced before use; they are not environment-variable references.
- **Dashboard is fully server-rendered static HTML**: the `page` constant in `src/index.ts` is a multi-line template literal with an inline `<script>` that polls `/api/dashboard` every 30 s. There is no frontend build pipeline.
- **Cron schedules are in `wrangler.toml`**, not in code. The `scheduled()` handler dispatches on `controller.cron` string — adding a new cron requires both a `wrangler.toml` entry and a matching `switch` case.
- **`up.sh` is incomplete**: it is missing ACLs for `economy_dashboard`, `dlq-` prefixed topics, and `connect-lcc-` consumer groups needed by the HTTP Sink V2 connector. These are documented in `CONTINUE.md`.
- **FRED data gap**: FRED returns the string `"."` (not null, not 0) for weeks where data hasn't been released yet. The collector silently skips these rows.
