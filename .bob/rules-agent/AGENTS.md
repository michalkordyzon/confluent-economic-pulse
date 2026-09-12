# Project Coding Rules (Non-Obvious Only)

- **No build step**: `wrangler` bundles `src/index.ts` directly. Do not add a compile/build script — `tsc --noEmit` is type-check only.
- **All Worker logic is one file**: `cloudflare/src/index.ts`. Do not split into modules unless also updating `tsconfig.json` includes.
- **Confluent REST produce payload shape is fixed**: `{value: {type: "JSON", data: <EconomicEvent>}}`. Deviating from this silently drops records.
- **Confluent REST 200 ≠ success**: always parse the response body and check `error_code !== 200` after a successful HTTP response.
- **`/api/ingest` accepts both JSON array and NDJSON**: do not change `extractEvents()` to JSON-only — HTTP Sink V2 batching can send newline-delimited records.
- **Ingest unwraps HTTP Sink envelope**: `raw?.value ?? raw` handles the `{value: <event>}` wrapper the HTTP Sink V2 adds. Removing this breaks all sink-delivered events.
- **`metric` is the D1 primary key**: the `latest_metrics` table stores one row per metric string. Upsert is always `ON CONFLICT(metric) DO UPDATE SET`.
- **`asIso()` must handle Unix seconds vs ms**: threshold `> 10_000_000_000` distinguishes them. Do not change this without verifying all five upstream APIs.
- **`COLLECTOR_MODE = "coingecko-only"`**: set this var when switching EIA/NBP/Treasury/FRED to HTTP Source V2 to avoid double-collection. Default is `"all"`.
- **No linter**: there is no ESLint config. Follow the existing style (2-space indent, `const`/`let`, TypeScript strict mode).
