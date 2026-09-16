# FIX2.md — Phase 3 status after ACL repair

## Current conclusion

The ACL problem for service account `sa-vrx6000` is fixed.

The required ACLs are present:

- `WRITE` on literal topic `economy_dashboard`
- `READ` and `DESCRIBE` on prefixed topic `error-lcc`
- `READ` and `DESCRIBE` on prefixed topic `success-lcc`

The ACL listing was verified with:

```zsh
set -a && source .env && set +a
confluent kafka acl list \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID"
```

## Phase 3 status

Phase 3 in `002-implementation-plan.md` is not yet fully verified end-to-end.

Confirmed:

- The HTTP Sink V2 connector is `RUNNING`.
- The connector task is `RUNNING`.
- The connector uses `JSON_SR`.
- HTTPS is enabled.
- The connector targets the deployed Worker and `/api/ingest`.
- The required Kafka ACLs are present.
- The Worker `/api/dashboard` endpoint responds successfully.

Not yet confirmed:

- A real economic metric has traveled through `economic.raw` → Flink → `economy_dashboard` → HTTP Sink → `/api/ingest` → D1.
- The dashboard contains all expected real metric cards without manual test records.

The dashboard currently contains only manual test records such as `test.direct`, `test.envelope`, `test.envelope2`, and `test.flat`.

The currently running connector is `lcc-9kp1887`. The older connector ID `lcc-j59pxym` referenced in earlier diagnostic notes no longer exists.

CoinGecko collection was attempted but failed with HTTP 429 because its upstream API rate limit was exceeded. NBP collection successfully returned four events, but delivery of those events to D1 still needs confirmation.

## Intended next steps

1. Use the successful NBP collector response as the end-to-end test source.
2. Wait for Flink to normalize the new `economic.raw` events and for the running HTTP Sink connector to process `economy_dashboard`.
3. Query `/api/dashboard` and check whether real NBP metrics appear.
4. If NBP metrics appear, mark the Phase 3 pipeline as operational and treat CoinGecko's HTTP 429 as an independent upstream rate-limit issue.
5. If NBP metrics do not appear, inspect the current connector `lcc-9kp1887` task status and logs, then verify the `economy_dashboard` topic offsets and Flink job status.
6. Do not recreate or modify ACLs unless a new ACL gap is observed; the requested ACL permissions are already present.

## Phase 3 exit condition

Phase 3 can be considered complete when the deployed dashboard shows real economic metrics produced through the pipeline, not only manually inserted test records, and the connector remains healthy without manual ingestion calls.
