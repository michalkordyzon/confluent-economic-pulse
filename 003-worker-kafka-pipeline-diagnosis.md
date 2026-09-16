# Worker → Kafka Pipeline Diagnosis and Fix Plan

## Current conclusion

The pipeline break is between the Cloudflare Worker and Kafka:

```text
Data source → Cloudflare Worker → Confluent Kafka REST endpoint → economic.raw
```

Kafka and the REST endpoint are not generally broken. A direct `curl` request successfully wrote a record and returned offset `34`. The Worker cron also fires every five minutes and the Worker returns `ok: true, count: 4`, but no corresponding records appear in `economic.raw`.

The strongest confirmed defect is therefore **false success reporting inside the Worker**. The response currently proves only that four records were fetched or that four produce attempts were made. It does not prove that Kafka acknowledged those records.

## Evidence

| Observation | What it proves |
| --- | --- |
| The `*/5 * * * *` cron fires | The scheduled handler is running. |
| The Worker returns `ok: true, count: 4` | The handler reaches its success path and counts four items or attempts. |
| `economic.raw` remains empty | No verified record delivery is visible in the topic being inspected. |
| Direct REST `curl` returned offset `34` | The Kafka REST endpoint, topic, and credentials used by that direct test can produce successfully. |
| Worker logs contain no Kafka error | The code may not inspect non-2xx responses, may swallow exceptions, or may log too little to reveal the destination. |

## Most likely causes

In priority order:

1. **The Worker does not validate Kafka's HTTP response.** `fetch()` resolves normally for HTTP errors such as `401`, `403`, `404`, or `422`; it throws mainly for transport failures. If `response.ok` is never checked, the Worker can report success after Kafka rejected the request.
2. **The Worker uses stale or incorrect credentials.** The suspected old API key ends in `J6PN`; the expected current key ends in `T34Y`. This remains a hypothesis until the Kafka response is observed. Secret values cannot be displayed with Wrangler, so verification should be based on replacing them deliberately and observing Kafka's response—not printing secrets.
3. **The Worker targets a different endpoint, cluster, or topic.** A valid request could succeed while writing somewhere other than the `economic.raw` topic currently being inspected.
4. **An exception is caught and converted into a successful result.** A broad `catch` block may increment the count or return `ok: true` despite failed produce calls.
5. **Asynchronous work is not awaited.** Calls made with `forEach(async ...)`, unreturned promises, or an un-awaited `Promise.all` can let the handler finish before writes complete.

Do not delete the old API key yet. Instrument and reproduce first; changing credentials before collecting evidence can obscure the original failure.

## Required code change

Update the Kafka `produce()` function so it captures the HTTP status and response body, rejects every non-2xx response, and returns Kafka's acknowledgement.

```ts
async function produce(env: Env, record: unknown) {
  const topic = "economic.raw";
  const url = `${env.KAFKA_REST_ENDPOINT}/kafka/v3/clusters/${env.KAFKA_CLUSTER_ID}/topics/${encodeURIComponent(topic)}/records`;

  const credentials = btoa(
    `${env.KAFKA_API_KEY}:${env.KAFKA_API_SECRET}`,
  );

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${credentials}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ value: { type: "JSON", data: record } }),
  });

  const responseText = await response.text();

  console.log("Kafka produce response", {
    endpointHost: new URL(env.KAFKA_REST_ENDPOINT).host,
    clusterId: env.KAFKA_CLUSTER_ID,
    topic,
    status: response.status,
    ok: response.ok,
    body: responseText,
  });

  if (!response.ok) {
    throw new Error(
      `Kafka produce failed: HTTP ${response.status}: ${responseText}`,
    );
  }

  let acknowledgement: unknown;
  try {
    acknowledgement = JSON.parse(responseText);
  } catch {
    acknowledgement = responseText;
  }

  return acknowledgement;
}
```

Adapt the URL and payload to the same Confluent REST API form that succeeded in the direct `curl` test. Do not log the API key, API secret, Basic Authorization header, or source secrets.

## Correct collection logic

Count only acknowledged writes, not fetched records or initiated requests.

```ts
const acknowledgements = [];

for (const record of records) {
  acknowledgements.push(await produce(env, record));
}

return Response.json({
  ok: true,
  fetched: records.length,
  produced: acknowledgements.length,
  acknowledgements,
});
```

If parallel writes are preferred, await them explicitly:

```ts
const acknowledgements = await Promise.all(
  records.map((record) => produce(env, record)),
);
```

Avoid this pattern:

```ts
records.forEach(async (record) => {
  await produce(env, record);
});
```

`forEach` does not wait for its asynchronous callbacks.

## Reproduction procedure

### 1. Inspect configuration names—not secret values

```bash
npx wrangler secret list
```

Confirm that the expected bindings exist:

- `KAFKA_API_KEY`
- `KAFKA_API_SECRET`
- any cluster ID, REST endpoint, or topic binding used by the code

Also inspect `wrangler.toml` or `wrangler.jsonc` for environment-specific variables and confirm that the command deploys the same Worker/environment whose URL and cron are being tested.

### 2. Add the instrumentation and deploy

```bash
npx wrangler deploy
```

### 3. Start live logs

```bash
npx wrangler tail
```

If named environments are used, pass the same `--env <name>` to the relevant Wrangler commands.

### 4. Trigger collection manually

In a second terminal:

```bash
curl -i "https://<worker-host>/api/collect"
```

Record:

- the Worker's HTTP status and response body;
- Kafka's HTTP status and response body from the tail;
- endpoint hostname, cluster ID, and topic logged by the Worker;
- any returned Kafka partition and offset acknowledgement.

### 5. Verify the topic independently

Inspect `economic.raw` in the same Confluent environment and cluster logged by the Worker. Use a fresh consumer position or earliest-offset view so an interface filter does not hide records.

## How to interpret the result

| Kafka result | Meaning | Next action |
| --- | --- | --- |
| `401` | Invalid or stale API credentials | Replace both Worker secrets with the intended key pair, then retest. |
| `403` | Credentials are valid but lack authorization | Check the service account and its `WRITE` ACL on literal topic `economic.raw`. |
| `404` | Wrong REST path, cluster ID, topic, or environment | Compare the Worker's full non-secret destination with the successful direct `curl`. |
| `400` / `415` / `422` | Payload or `Content-Type` is incompatible with the endpoint | Make the Worker request match the successful direct `curl` exactly. |
| `429` | Rate limiting | Respect `Retry-After`, add bounded retry/backoff, and reduce concurrency. |
| `5xx` | Confluent-side or transient service failure | Add bounded retries with exponential backoff and preserve the failed batch. |
| `2xx` with partition/offset | Kafka accepted the record | Verify that the UI/consumer is pointed at the logged environment, cluster, and topic and is reading the correct offsets. |
| No Kafka log at all | `produce()` is not reached, logs are attached to a different deployment, or async work is detached | Add logs immediately before the call and verify Worker/environment routing and `await` usage. |

## Credential correction—only if the response supports it

If the response is `401`, deliberately replace both secrets:

```bash
npx wrangler secret put KAFKA_API_KEY
npx wrangler secret put KAFKA_API_SECRET
```

Enter the current matching key and secret pair. Then deploy if required by the project's deployment workflow and repeat the manual test while tailing logs. Never place these values in source code, `.env` files committed to Git, command history, or diagnostic output.

Once the current credentials are proven to work from the Worker, the old key can be revoked to remove ambiguity and reduce security risk.

## Permanent reliability changes

After the immediate failure is fixed:

1. Return separate `fetched`, `attempted`, `produced`, and `failed` counts.
2. Treat Kafka acknowledgement—not completion of `fetch()`—as successful delivery.
3. Attach a collection-run ID to every record and log entry.
4. Log only safe destination metadata: endpoint host, cluster ID, topic, status, partition, offset, and latency.
5. Preserve failed records for retry or send them to a dead-letter path.
6. Add bounded retry with exponential backoff for `429` and transient `5xx` responses; do not blindly retry authentication or validation failures.
7. Add a post-run invariant: `produced + failed = attempted`.
8. Make `/api/collect` return a non-2xx status when any required Kafka write fails.
9. Consider idempotency or a stable event ID so retrying a scheduled run does not silently create duplicates.

## Definition of fixed

The issue is resolved only when one manually triggered collection run provides all of the following evidence:

- the Worker response reports the number of acknowledged Kafka writes;
- Worker logs show a `2xx` Kafka response for every intended record;
- Kafka returns partition/offset acknowledgements;
- those new records are visible or consumable from `economic.raw` in the exact logged Confluent environment and cluster;
- a simulated rejected request makes the Worker return failure instead of `ok: true`.

## Immediate next action

Instrument `produce()`, deploy, run `wrangler tail`, and trigger `/api/collect`. The first Kafka status/body captured there should identify whether this is credentials, destination mismatch, request format, or asynchronous control flow—without further guessing.
