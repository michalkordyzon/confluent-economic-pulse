# FIX.md — Phase 3 diagnosis: D1 empty despite connector RUNNING

**Symptom:** `GET /api/dashboard` returns `{"metrics":[]}`.  
Connector `lcc-j59pxym` status: `RUNNING`. Flink job status: assumed running from Phase 2.  
Pipeline: `economic.raw` → Flink → `economy_dashboard` → HTTP Sink → `/api/ingest` → D1.

---

## 20 possible causes

### HTTP Sink → Worker leg

1. **Connector consuming from wrong offset** — connector was created after records were written to `economy_dashboard`; default offset policy is `latest`, so it skips all existing records and waits for new ones only.

2. **Authorization header rejected** — the `<DASHBOARD_INGEST_TOKEN>` placeholder was not substituted correctly when the connector was created; Worker returns 401 and connector silently retries or marks records as failed.

3. **`behavior.on.error = FAIL` stops on first bad record** — if even one record fails (401, parse error, etc.) the task stops processing; status still shows RUNNING at the connector level but task may be paused.

4. **Wrong `input.data.format`** — connector JSON says `"input.data.format": "JSON"` but `economy_dashboard` uses JSON Schema Registry (`value.format = json-registry`); the connector may be deserializing the Avro/JSON-schema wire bytes as raw JSON and producing a garbled body.

5. **Connector posts to wrong URL path** — `http.api.base.url` + `api1.http.api.path` combine to the target; a subtle slash or hostname typo means posts go to a 404 that the connector treats as success (or causes retry loop).

6. **`economy_dashboard` topic is upsert/compacted** — the connector may not handle compacted topics correctly with default settings; tombstone records (null value) are sent to the Worker which then has no `metric` field and is silently skipped.

7. **Connector task is RUNNING but has zero lag** — the topic genuinely has no new records since connector started; Flink job is not producing new output because no new input is arriving on `economic.raw`.

8. **`https.ssl.enabled = false` causes SSL handshake failure** — the Worker URL is HTTPS; setting this to false may disable TLS negotiation entirely, causing connection refusal.

9. **Worker's `ingest` handler requires `Authorization` but connector sends it as a sensitive header differently formatted** — `"Authorization:Bearer <TOKEN>"` (colon-separated, no space after `Authorization:`) vs the Worker's check `Bearer ${token}` with a space — mismatch if format is wrong.

10. **D1 write silently fails** — Worker returns `{"ok":true}` but the D1 `prepare().bind().run()` throws an exception that is caught somewhere (or not awaited), so records are lost before INSERT.

### Flink → `economy_dashboard` leg

11. **Flink job has stopped or errored** — the `EXECUTE STATEMENT SET` job from Phase 2 may have failed after session ended; no new records flow into `economy_dashboard` even if the connector is polling.

12. **`economy_dashboard` topic has records but all are tombstones** — upsert changelog emits tombstones for deletes; if the Worker got nothing and the connector consumed only tombstones, `event.metric` is null and every record is skipped by the `if (!event?.metric) continue` guard.

13. **Flink `value` column reserved-word backtick issue in output** — the output JSON field may be serialized as `"value"` by some schema registry encoders but received differently; the Worker reads `event.value` — if the field name changed, it stores `NaN`.

14. **`economy_dashboard` Schema Registry deserialization** — the connector `input.data.format: JSON` does not match the topic's actual format (`json-registry`); the connector cannot decode the messages at all and produces empty/malformed POST bodies.

15. **Flink job consuming `economic.raw` from wrong offset** — if the Flink job was (re-)started and began at `latest`, records already in `economic.raw` were never processed; `economy_dashboard` is empty.

### Worker / D1 leg

16. **`event = raw?.value ?? raw` double-unwrap** — if the HTTP Sink V2 wraps the record as `{"value": {"metric": ...}}` and the ingest handler unwraps one level, but the Flink output adds another layer, the resulting object is `{"metric": ...}` only after two unwraps. If the Flink output is already flat (no `value` wrapper), `raw?.value` is the metric's numeric value (a number), not an object — `event` becomes a number and `event?.metric` is `undefined`.

17. **D1 binding not wired** — `wrangler.toml` `[[d1_databases]]` binding name must be `DB`; if it differs, `env.DB` is undefined and the Worker throws on `.prepare()`.

18. **`DASHBOARD_INGEST_TOKEN` Worker secret not set or mismatched** — secret was set in a previous session; if the Worker was redeployed without re-setting secrets, or the `.env` value differs from what's stored in Cloudflare, all `/api/ingest` calls return 401.

19. **NBP 404 on non-business day triggers `FAIL` behavior** — if `economy_dashboard` contains a record sourced from a failed collector run with a null/invalid payload, the HTTP Sink posts it, the Worker's `if (!event?.metric) continue` skips it, returns `{"ok":true, "received":1}` — but records that *do* have metrics never arrived in `economy_dashboard` in the first place.

20. **Worker deployed to wrong route / subdomain** — if a previous deploy used a custom domain and the connector points to the `workers.dev` subdomain (or vice versa), POSTs succeed at the DNS level but hit a different Worker that has no D1 binding.

---

## Fix instructions (top 10, ordered by likelihood)

### Fix 1 — Wrong offset: connector skips pre-existing records (cause #1)
The connector default is `latest`. All existing `economy_dashboard` records were written before the connector started.

**Fix:** Trigger new records to flow through the pipeline end-to-end:
```zsh
set -a && source .env && set +a
curl -s "https://confluent-economic-pulse.michalkordyzon.workers.dev/api/collect?source=nbp" \
  -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN"
# Wait 30s for Flink to normalize, then check:
sleep 30
curl -s https://confluent-economic-pulse.michalkordyzon.workers.dev/api/dashboard | jq '.metrics | length'
```
If count > 0, the pipeline is healthy — the connector was just waiting for new records.

---

### Fix 2 — `input.data.format` mismatch with json-registry topic (causes #4, #14)
`economy_dashboard` uses `value.format = json-registry` (Schema Registry). The connector has `input.data.format: JSON`. This means the connector receives Schema Registry wire-format bytes (magic byte + schema ID prefix) and tries to parse them as plain JSON — it will fail or produce garbage.

**Fix:** Delete the connector and recreate with `"input.data.format": "JSON_SR"` (JSON Schema Registry):
```json
"input.data.format": "JSON_SR",
"schema.registry.url": "<SR_ENDPOINT>",
"basic.auth.credentials.source": "USER_INFO",
"basic.auth.user.info": "<SR_API_KEY>:<SR_API_SECRET>"
```
Add these four fields to `confluent/connectors/http-sink-dashboard.json`, fill placeholders, and recreate the connector.

---

### Fix 3 — Authorization header format wrong (cause #2, #9)
Verify the sensitive header was set correctly. The connector config uses `"Authorization:Bearer <TOKEN>"` (colon, no space after `Authorization:`). The Worker checks `Bearer ${token}` with a space after `Bearer`.

**Fix:** Test the Worker directly with the exact token from `.env`:
```zsh
set -a && source .env && set +a
curl -s -X POST \
  "https://confluent-economic-pulse.michalkordyzon.workers.dev/api/ingest" \
  -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"metric":"test.ping","source":"manual","label":"Test","value":1,"unit":"","observed_at":"2026-01-01T00:00:00Z","collected_at":"2026-01-01T00:00:00Z","dimensions":{}}]'
```
Expected: `{"ok":true,"received":1}`. Then verify D1: `curl -s .../api/dashboard | jq .metrics`.  
If you get 401, the token in `.env` differs from the Worker secret — re-run `npx wrangler secret put DASHBOARD_INGEST_TOKEN` from `cloudflare/`.

---

### Fix 4 — Flink job not running (cause #11)
The Flink `EXECUTE STATEMENT SET` job may have stopped after the session ended or due to an error.

**Fix:** In Confluent Cloud → Flink workspace → Jobs, confirm the job consuming from `economic.raw` is `RUNNING`. If it shows `FAILED` or `STOPPED`:
1. Re-run only the `EXECUTE STATEMENT SET BEGIN ... END` block from `confluent/flink/01-normalize.sql` (do NOT re-run the `CREATE TABLE` — it already exists).
2. Confirm new records appear in `economy_dashboard` after triggering a collection.

---

### Fix 5 — `behavior.on.error = FAIL` halted the task (cause #3)
A single failed delivery (e.g. a 401 on startup before ACLs were correct) may have halted the connector task.

**Fix:** Check task status and restart:
```zsh
set -a && source .env && set +a
confluent connect cluster describe lcc-j59pxym \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID"
```
If any task is `FAILED`, restart it:
```zsh
confluent connect cluster update lcc-j59pxym \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID" \
  --config '{"tasks.max":"1"}'
```
Also consider changing `"behavior.on.error"` to `"LOG"` in the connector config to prevent future halts.

---

### Fix 6 — `https.ssl.enabled = false` breaks HTTPS (cause #8)
The Worker URL is HTTPS. Setting `https.ssl.enabled: false` may disable TLS for outbound connections.

**Fix:** Edit `confluent/connectors/http-sink-dashboard.json`:
```json
"https.ssl.enabled": "true"
```
Delete and recreate the connector with this change.

---

### Fix 7 — Double-unwrap: `raw?.value` resolves to a number (cause #16)
If Flink outputs records as flat objects (no `value` wrapper envelope), then `raw` is already `{"metric":"...", "value": 1.5, ...}`. `raw?.value` evaluates to `1.5` (the numeric metric value), and `event` becomes `1.5`. Then `event?.metric` is `undefined` and every record is silently skipped.

**Fix:** Add a manual ingest test using the exact shape Flink produces:
```zsh
# Test with flat shape (no value wrapper):
curl -s -X POST ".../api/ingest" \
  -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"metric":"nbp.usd","source":"nbp","label":"USD/PLN","value":3.95,"unit":"PLN","observed_at":"2026-01-01T00:00:00Z","collected_at":"2026-01-01T00:00:00Z","dimensions":{}}'
```
If this inserts correctly, the pipeline shape is flat. The `raw?.value ?? raw` guard is **incorrectly extracting** the numeric value as the event object. Fix: change the unwrap logic to check if `raw.value` is an object before using it:
```typescript
const event = (raw?.value && typeof raw.value === "object") ? raw.value : raw;
```

---

### Fix 8 — `economy_dashboard` topic genuinely empty / no lag (cause #7)
The connector is RUNNING and consuming, but there are no records because `economy_dashboard` received no new writes after the connector's start offset.

**Fix:** Check topic message count directly:
```zsh
set -a && source .env && set +a
confluent kafka topic describe economy_dashboard \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID"
```
Look at partition end offsets. If 0 or equal to start offsets, trigger a collection and re-check after 30s. If offsets grow but D1 stays empty, the problem is in the connector → Worker leg (Fixes 2, 3, 5, 7).

---

### Fix 9 — `DASHBOARD_INGEST_TOKEN` secret stale or mismatched (cause #18)
The Worker was deployed; the token stored as a Cloudflare secret may differ from `.env`.

**Fix:** Re-set the secret to guarantee consistency:
```zsh
cd cloudflare
echo "$DASHBOARD_INGEST_TOKEN" | npx wrangler secret put DASHBOARD_INGEST_TOKEN
```
Then verify the token is accepted (Fix 3 manual curl test). If this changes the stored secret, also delete and recreate the connector so it picks up the new token.

---

### Fix 10 — D1 binding name mismatch (cause #17)
`wrangler.toml` must bind D1 as `binding = "DB"`. If the binding is missing or named differently, `env.DB` is `undefined` at runtime and every `prepare()` call throws — but the Worker may still return a 200 if the error is not surfaced.

**Fix:** Check `cloudflare/wrangler.toml`:
```toml
[[d1_databases]]
binding = "DB"
database_name = "economic-pulse"
database_id = "984b097a-f7d3-44ff-a4dc-1845f69ec098"
```
All three fields must be present and match. Query D1 directly to confirm:
```zsh
cd cloudflare
npx wrangler d1 execute economic-pulse --remote --command "SELECT COUNT(*) FROM latest_metrics;"
```
If count stays 0 after a confirmed `/api/ingest` call, the D1 writes are silently failing.

---

### Fix 11 — Connector posts to wrong URL (cause #5)
A slash or hostname typo in `http.api.base.url` combined with `api1.http.api.path` produces the wrong endpoint. The connector may receive a 404 it retries indefinitely.

**Fix:** Verify the effective URL manually:
```zsh
curl -v -X POST \
  "https://confluent-economic-pulse.michalkordyzon.workers.dev/api/ingest" \
  -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[]'
```
Expected: `{"ok":true,"received":0}`. If 404, the path is wrong. Correct `http.api.base.url` (no trailing slash) and `api1.http.api.path` (leading slash), delete and recreate the connector.

---

### Fix 12 — Tombstone records skipping all metrics (cause #6)
The `economy_dashboard` topic uses `changelog.mode=upsert`. Delete events are represented as tombstones (null value). If the connector serializes these as `null` or `{}`, the Worker's `if (!event?.metric) continue` guard silently drops them — but if *all* messages are tombstones, D1 stays empty.

**Fix:** Check whether `economy_dashboard` has any non-tombstone records by consuming a few raw messages:
```zsh
set -a && source .env && set +a
timeout 10 confluent kafka topic consume economy_dashboard \
  --from-beginning \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID" \
  --api-key "$KAFKA_API_KEY" \
  --api-secret "$KAFKA_API_SECRET" \
  --print-key 2>&1 | head -20
```
If all values are empty/null, trigger a fresh collection and wait for Flink to emit new upsert records before checking again.

---

### Fix 13 — Flink `value` field name changed by Schema Registry (cause #13)
The `economy_dashboard` table defines the column as `` `value` `` (backtick-quoted reserved word). Some JSON Schema Registry encoders may serialize it with an underscore suffix or rename it. The Worker reads `event.value` — if the field arrives as something else (e.g. `value_` or `"value"`), D1 stores `NaN` but the row is still inserted.

**Fix:** Post a known payload directly to `/api/ingest` that contains `"value": 99.9` and then query D1:
```zsh
npx wrangler d1 execute economic-pulse --remote \
  --command "SELECT metric, value FROM latest_metrics WHERE metric = 'test.ping';"
```
If `value` is `0` or `null` instead of `99.9`, the field name is being misread. Inspect the raw connector POST body by temporarily adding `console.log(body)` in the Worker's `ingest()` function and checking Cloudflare Worker logs (`npx wrangler tail`).

---

### Fix 14 — Flink job restarted at `latest` offset (cause #15)
If the Flink normalization job was stopped and restarted (e.g. after a SQL error), it may have reset its consumer offset to `latest`, skipping all records already in `economic.raw`. `economy_dashboard` then has no new output.

**Fix:** Trigger a full fresh collection across all sources to generate new `economic.raw` records *after* the Flink job restarted:
```zsh
set -a && source .env && set +a
for src in nbp eia fred; do
  curl -s "https://confluent-economic-pulse.michalkordyzon.workers.dev/api/collect?source=$src" \
    -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN" | jq .
done
```
Wait 30–60s, then check `economy_dashboard` offset growth and `/api/dashboard`.

---

### Fix 15 — Worker routing: wrong subdomain or custom domain (cause #20)
If the Worker was previously deployed with a custom domain and the connector's `http.api.base.url` points to `*.workers.dev` (or vice versa), POSTs may succeed at DNS level but reach a different Worker instance without a D1 binding.

**Fix:** Confirm the exact deployed URL:
```zsh
cd cloudflare && npx wrangler deployments list 2>/dev/null | head -5
```
Then test the `/api/ingest` endpoint on that exact URL (Fix 11 curl test). If the URL in the connector config differs from the deployed URL, delete and recreate the connector with the corrected `http.api.base.url`.

---

### Fix 16 — NBP 404 on non-business day pollutes the pipeline (cause #19)
NBP returns HTTP 404 on weekends and public holidays. If the Worker ran a collection on such a day, it may have produced a record with a null/empty payload to `economic.raw`. Flink normalizes it with a null `metric`, and the Worker's ingest guard drops it. This alone doesn't cause D1 to be empty, but confirms no NBP data flows on non-business days.

**Fix:** Check current day before blaming NBP. If today is a weekend, use a source that always responds:
```zsh
curl -s "https://confluent-economic-pulse.michalkordyzon.workers.dev/api/collect?source=eia" \
  -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN" | jq .
```
Long-term: set `"behavior.on.error": "LOG"` (not `FAIL`) in the connector, and consider adding `"api1.http.response.body.success.status.codes": "200,404"` for the NBP-specific connector when Phase 4 is built.

---

### Fix 17 — D1 write silently fails / exception not surfaced (cause #10)
The `ingest()` handler's `await env.DB.prepare(...).bind(...).run()` may throw (e.g. schema mismatch, column count mismatch) but the error propagates up and is caught by the Worker's outer try-catch (if any), returning a 500 that the connector counts as a failure but D1 still gets nothing.

**Fix:** Stream live Worker logs while sending a test ingest request:
```zsh
cd cloudflare
npx wrangler tail --format pretty &
sleep 2
curl -s -X POST \
  "https://confluent-economic-pulse.michalkordyzon.workers.dev/api/ingest" \
  -H "Authorization: Bearer $DASHBOARD_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"metric":"dbtest","source":"manual","label":"DB Test","value":1,"unit":"","observed_at":"2026-01-01T00:00:00Z","collected_at":"2026-01-01T00:00:00Z","dimensions":{}}]'
```
Check wrangler tail for any exceptions. Follow up with:
```zsh
npx wrangler d1 execute economic-pulse --remote --command "SELECT * FROM latest_metrics WHERE metric='dbtest';"
```

---

### Fix 18 — `economy_dashboard` topic has records but connector consumer group lag is 0 (cause #1 variant)
The connector's consumer group may have committed offsets at the end of the topic on creation (even before processing any records), meaning it has "consumed" everything but never actually POSTed anything.

**Fix:** Check the connector's consumer group offset lag:
```zsh
set -a && source .env && set +a
confluent kafka consumer group list \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID" | grep -i lcc-j59pxym
```
Then describe the group to see lag per partition. If lag is 0 and D1 is empty, reset the offset by deleting and recreating the connector (it will start from `latest` again, but now you trigger fresh records first so it picks them up).

---

### Fix 19 — Schema Registry credentials missing from connector (cause #14 variant)
The connector has `input.data.format: JSON` with no Schema Registry credentials. If `economy_dashboard` requires Schema Registry to decode messages (because it uses `json-registry`), the connector fails to deserialize silently — it may log a deserialization error internally but still report RUNNING.

**Fix:** Check Confluent Cloud → Connector → Logs for any `Deserialization error` or `Schema not found` messages. If present, add SR credentials to the connector config:
```json
"input.data.format": "JSON_SR",
"schema.registry.url": "https://psrc-XXXXX.eu-central-1.aws.confluent.cloud",
"basic.auth.credentials.source": "USER_INFO",
"basic.auth.user.info": "<SR_API_KEY>:<SR_API_SECRET>"
```
SR endpoint and keys are in `.env` as `SR_API_KEY` and `SR_API_SECRET`.

---

### Fix 20 — `economy_dashboard` Flink output has `dimensions_json` but Worker expects `dimensions` (cause #13 variant)
Flink writes the field as `dimensions_json` (a JSON string). The HTTP Sink posts this field name as-is. The Worker's ingest handler reads `event.dimensions` — which will be `undefined` — and falls back to `{}`. The row is still inserted, but this confirms the shape mismatch. More critically, if other field names differ between Flink output and Worker expectations, metrics may be stored as `NaN` or empty strings, causing the dashboard to show garbled data even when rows exist.

**Fix:** Query D1 directly after a confirmed ingest to inspect actual stored values:
```zsh
cd cloudflare
npx wrangler d1 execute economic-pulse --remote \
  --command "SELECT metric, value, source, label FROM latest_metrics LIMIT 5;"
```
If rows exist but values are `0`, `null`, or empty, the field mapping between Flink output and the Worker's ingest handler is misaligned. Cross-reference Flink's `economy_dashboard` column names against what `ingest()` reads (`event.metric`, `event.source`, `event.label`, `event.value`, `event.unit`, `event.observed_at`, `event.collected_at`, `event.dimensions`).
