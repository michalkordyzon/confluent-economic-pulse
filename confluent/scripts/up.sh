#!/usr/bin/env bash
# confluent/scripts/up.sh
#
# Provisions a Confluent Cloud cluster for the economic-pulse demo and writes
# all generated secrets into .env at the repo root.
#
# Prerequisites:
#   - confluent CLI installed and logged in  (confluent login)
#   - jq installed
#   - wrangler installed and logged in (npx wrangler whoami)
#
# Usage:
#   bash confluent/scripts/up.sh
#
# What this script does (fully idempotent):
#   1. Creates Kafka cluster (or reuses existing)
#   2. Creates topics: economic.raw, economy_dashboard
#   3. Creates service account (or reuses existing)
#   4. Grants all required Kafka ACLs (cluster, topics, groups)
#   5. Creates Kafka API key for the service account
#   6. Creates Schema Registry API key
#   7. Writes .env
#   8. Pushes all secrets to Cloudflare Worker
#   9. Applies D1 schema migration
#  10. Deploys HTTP Sink V2 connector

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
CLUSTER_NAME="economic-pulse"
CLOUD="aws"
REGION="eu-central-1"       # change to your preferred region
CLUSTER_TYPE="basic"
SA_NAME="economic-pulse-sa"
CONNECTOR_NAME="economic-pulse-dashboard-sink"
WORKER_SUBDOMAIN="confluent-economic-pulse.michalkordyzon"

# Repo root (one level above this script's directory)
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
CONNECTOR_TEMPLATE="$REPO_ROOT/confluent/connectors/http-sink-dashboard.json"

# Topics to create
TOPICS=(
  "economic.raw"
  "economy_dashboard"
)

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo "▶  $*"; }
ok()    { echo "✓  $*"; }
warn()  { echo "⚠  $*"; }
err()   { echo "✗  $*" >&2; exit 1; }

require() {
  command -v "$1" &>/dev/null || err "'$1' is required but not found. Install it first."
}

require confluent
require jq

# ── Check login ───────────────────────────────────────────────────────────────
confluent context current &>/dev/null || err "Not logged in. Run: confluent login"

# Resolve environment ID (use the current/default one)
ENV_ID=$(confluent environment list -o json | jq -r '.[] | select(.is_current == true) | .id')
[[ -z "$ENV_ID" ]] && err "Could not determine current Confluent environment. Run: confluent environment list"
info "Using environment: $ENV_ID"

# ── Create Kafka cluster (idempotent — reuse if already exists) ───────────────
EXISTING_CLUSTER=$(confluent kafka cluster list --environment "$ENV_ID" -o json | \
  jq -r --arg name "$CLUSTER_NAME" '.[] | select(.name == $name) | .id' | head -1)

if [[ -n "$EXISTING_CLUSTER" ]]; then
  CLUSTER_ID="$EXISTING_CLUSTER"
  ok "Cluster '$CLUSTER_NAME' already exists: $CLUSTER_ID — reusing."
else
  info "Creating Kafka cluster '$CLUSTER_NAME' ($CLUSTER_TYPE, $CLOUD $REGION)..."
  CLUSTER_JSON=$(confluent kafka cluster create "$CLUSTER_NAME" \
    --cloud "$CLOUD" \
    --region "$REGION" \
    --type "$CLUSTER_TYPE" \
    --environment "$ENV_ID" \
    -o json)

  CLUSTER_ID=$(echo "$CLUSTER_JSON" | jq -r '.id')
  [[ -z "$CLUSTER_ID" || "$CLUSTER_ID" == "null" ]] && err "Failed to create cluster."
  ok "Cluster created: $CLUSTER_ID"
fi

# Wait until the cluster is RUNNING before creating topics / API keys
info "Waiting for cluster to become RUNNING..."
for i in $(seq 1 30); do
  STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r '.status')
  if [[ "$STATUS" == "UP" ]]; then
    ok "Cluster is UP."
    break
  fi
  echo "   status=$STATUS, waiting 10 s... ($i/30)"
  sleep 10
done

# Confirm it's actually up
STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r '.status')
[[ "$STATUS" != "UP" ]] && err "Cluster did not reach UP status after 5 min. Current status: $STATUS"

# Get REST endpoint
REST_ENDPOINT=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r '.rest_endpoint')
[[ -z "$REST_ENDPOINT" || "$REST_ENDPOINT" == "null" ]] && err "Could not read REST endpoint."
ok "REST endpoint: $REST_ENDPOINT"

# ── Create Kafka topics ───────────────────────────────────────────────────────
for TOPIC in "${TOPICS[@]}"; do
  info "Creating topic '$TOPIC'..."
  confluent kafka topic create "$TOPIC" \
    --partitions 1 \
    --config "retention.ms=604800000" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --if-not-exists
  ok "Topic '$TOPIC' ready."
done

# ── Create service account (idempotent — reuse if already exists) ─────────────
EXISTING_SA=$(confluent iam service-account list -o json | \
  jq -r --arg name "$SA_NAME" '.[] | select(.name == $name) | .id' | head -1)

if [[ -n "$EXISTING_SA" ]]; then
  SA_ID="$EXISTING_SA"
  ok "Service account '$SA_NAME' already exists: $SA_ID -- reusing."
else
  info "Creating service account '$SA_NAME'..."
  SA_JSON=$(confluent iam service-account create "$SA_NAME" \
    --description "economic-pulse demo service account" \
    -o json)
  SA_ID=$(echo "$SA_JSON" | jq -r '.id')
  [[ -z "$SA_ID" || "$SA_ID" == "null" ]] && err "Failed to create service account."
  ok "Service account created: $SA_ID"
fi

# ── Grant Kafka ACLs to the service account ───────────────────────────────────
# All ACLs required for:
#   - Cloudflare Worker producing to economic.raw
#   - HTTP Sink V2 connector consuming economy_dashboard
#   - Connector managing its consumer group and DLQ/success/error topics
info "Applying Kafka ACLs for $SA_ID on cluster $CLUSTER_ID..."

acl() {
  # acl <operations> <resource-flag> <resource-name-or-flag>
  # Wraps confluent kafka acl create --allow with idempotent (|| true)
  confluent kafka acl create \
    --allow \
    --service-account "$SA_ID" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    "$@" \
    2>/dev/null || true
}

# Cluster-level DESCRIBE (required by managed sink connectors)
acl --operations describe --cluster-scope

# economic.raw — Worker produces, Flink reads
acl --operations write,describe   --topic "economic.raw"
acl --operations read,describe    --topic "economic.raw"

# economy_dashboard — connector consumes
acl --operations read,describe    --topic "economy_dashboard"
acl --operations describe-configs --topic "economy_dashboard"

# DLQ topics (connector creates these automatically)
acl --operations create,write,read,describe --topic "dlq-" --prefix

# success-lcc / error-lcc topics (HTTP Sink V2 creates these on startup)
acl --operations create,write --topic "success-lcc" --prefix
acl --operations create,write --topic "error-lcc"   --prefix

# Consumer group for the connector (connect-lcc-<connector-id>)
acl --operations read,describe,delete --consumer-group "connect-lcc-" --prefix

ok "Kafka ACLs applied."

# ── Create Kafka API key for the service account ──────────────────────────────
info "Creating Kafka API key for service account $SA_ID..."
APIKEY_JSON=$(confluent api-key create \
  --resource "$CLUSTER_ID" \
  --service-account "$SA_ID" \
  --environment "$ENV_ID" \
  -o json)
KAFKA_API_KEY=$(echo "$APIKEY_JSON" | jq -r '.api_key')
KAFKA_API_SECRET=$(echo "$APIKEY_JSON" | jq -r '.api_secret')
[[ -z "$KAFKA_API_KEY" || "$KAFKA_API_KEY" == "null" ]] && err "Failed to create Kafka API key."
ok "Kafka API key created: $KAFKA_API_KEY"

# ── Create Schema Registry API key ───────────────────────────────────────────
# Required for the HTTP Sink V2 connector to deserialize json-registry messages
# from economy_dashboard.
info "Looking up Schema Registry cluster ID..."
SR_CLUSTER_ID=$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json | jq -r '.cluster')
[[ -z "$SR_CLUSTER_ID" || "$SR_CLUSTER_ID" == "null" ]] && err "Could not read Schema Registry cluster ID."
ok "Schema Registry cluster: $SR_CLUSTER_ID"

info "Creating Schema Registry API key for service account $SA_ID..."
SR_APIKEY_JSON=$(confluent api-key create \
  --resource "$SR_CLUSTER_ID" \
  --service-account "$SA_ID" \
  --environment "$ENV_ID" \
  -o json)
SR_API_KEY=$(echo "$SR_APIKEY_JSON" | jq -r '.api_key')
SR_API_SECRET=$(echo "$SR_APIKEY_JSON" | jq -r '.api_secret')
[[ -z "$SR_API_KEY" || "$SR_API_KEY" == "null" ]] && err "Failed to create Schema Registry API key."
ok "Schema Registry API key created: $SR_API_KEY"

# Get Schema Registry endpoint
SR_ENDPOINT=$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json | jq -r '.endpoint_url')
[[ -z "$SR_ENDPOINT" || "$SR_ENDPOINT" == "null" ]] && err "Could not read Schema Registry endpoint."
ok "Schema Registry endpoint: $SR_ENDPOINT"

# ── Generate DASHBOARD_INGEST_TOKEN ──────────────────────────────────────────
DASHBOARD_INGEST_TOKEN=$(openssl rand -hex 32)
ok "Generated DASHBOARD_INGEST_TOKEN."

# ── Write .env ────────────────────────────────────────────────────────────────
info "Writing secrets to $ENV_FILE..."

# Preserve EIA_API_KEY and FRED_API_KEY if already present in .env
EXISTING_EIA=""
EXISTING_FRED=""
if [[ -f "$ENV_FILE" ]]; then
  EXISTING_EIA=$(grep "^EIA_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)
  EXISTING_FRED=$(grep "^FRED_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)
fi

cat > "$ENV_FILE" <<EOF
# Generated by confluent/scripts/up.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# DO NOT COMMIT — .env is in .gitignore

# Confluent Cloud
CONFLUENT_ENVIRONMENT_ID=$ENV_ID
CONFLUENT_CLUSTER_ID=$CLUSTER_ID
CONFLUENT_REST_ENDPOINT=$REST_ENDPOINT
CONFLUENT_SERVICE_ACCOUNT_ID=$SA_ID

# Kafka API key (for Cloudflare Worker → Confluent REST produce)
KAFKA_API_KEY=$KAFKA_API_KEY
KAFKA_API_SECRET=$KAFKA_API_SECRET

# Schema Registry (for HTTP Sink V2 connector JSON_SR deserialization)
SR_ENDPOINT=$SR_ENDPOINT
SR_API_KEY=$SR_API_KEY
SR_API_SECRET=$SR_API_SECRET

# Cloudflare Worker
DASHBOARD_INGEST_TOKEN=$DASHBOARD_INGEST_TOKEN

# External API keys — fill in manually if not already set
EIA_API_KEY=${EXISTING_EIA:-<your-eia-api-key>}
FRED_API_KEY=${EXISTING_FRED:-<your-fred-api-key>}
EOF

ok ".env written."

# ── Push secrets to Cloudflare Worker ────────────────────────────────────────
info "Pushing secrets to Cloudflare Worker..."
(
  cd "$REPO_ROOT/cloudflare"
  echo "$REST_ENDPOINT"           | npx wrangler secret put CONFLUENT_REST_ENDPOINT
  echo "$CLUSTER_ID"              | npx wrangler secret put CONFLUENT_CLUSTER_ID
  echo "$KAFKA_API_KEY"           | npx wrangler secret put KAFKA_API_KEY
  echo "$KAFKA_API_SECRET"        | npx wrangler secret put KAFKA_API_SECRET
  echo "$DASHBOARD_INGEST_TOKEN"  | npx wrangler secret put DASHBOARD_INGEST_TOKEN
  if [[ -n "${EXISTING_EIA}" ]]; then
    echo "$EXISTING_EIA"  | npx wrangler secret put EIA_API_KEY
  fi
  if [[ -n "${EXISTING_FRED}" ]]; then
    echo "$EXISTING_FRED" | npx wrangler secret put FRED_API_KEY
  fi
)
ok "Worker secrets pushed."

# ── Apply D1 schema migration ─────────────────────────────────────────────────
info "Applying D1 schema migration..."
(
  cd "$REPO_ROOT/cloudflare"
  npx wrangler d1 execute economic-pulse --remote --file=./schema.sql
)
ok "D1 schema applied."

# ── Deploy HTTP Sink V2 connector ─────────────────────────────────────────────
info "Deploying HTTP Sink V2 connector..."

# Delete existing connector with this name if present (idempotent)
EXISTING_CONNECTOR=$(confluent connect cluster list \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  -o json 2>/dev/null | jq -r --arg name "$CONNECTOR_NAME" '.[] | select(.name == $name) | .id' | head -1)

if [[ -n "$EXISTING_CONNECTOR" ]]; then
  info "Deleting existing connector $EXISTING_CONNECTOR..."
  confluent connect cluster delete "$EXISTING_CONNECTOR" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --force
  ok "Old connector deleted."
  sleep 5
fi

# Fill connector template placeholders
FILLED_CONNECTOR=$(mktemp /tmp/http-sink-dashboard-XXXXXX.json)
cat "$CONNECTOR_TEMPLATE" \
  | sed "s|<KAFKA_API_KEY>|$KAFKA_API_KEY|g" \
  | sed "s|<KAFKA_API_SECRET>|$KAFKA_API_SECRET|g" \
  | sed "s|<SR_ENDPOINT>|$SR_ENDPOINT|g" \
  | sed "s|<SR_API_KEY>|$SR_API_KEY|g" \
  | sed "s|<SR_API_SECRET>|$SR_API_SECRET|g" \
  | sed "s|<YOUR-WORKER>|$WORKER_SUBDOMAIN|g" \
  | sed "s|<DASHBOARD_INGEST_TOKEN>|$DASHBOARD_INGEST_TOKEN|g" \
  > "$FILLED_CONNECTOR"

CONNECTOR_JSON=$(confluent connect cluster create \
  --config-file "$FILLED_CONNECTOR" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  -o json)
CONNECTOR_ID=$(echo "$CONNECTOR_JSON" | jq -r '.id')
rm -f "$FILLED_CONNECTOR"
[[ -z "$CONNECTOR_ID" || "$CONNECTOR_ID" == "null" ]] && err "Failed to create connector."
ok "Connector created: $CONNECTOR_ID"

# Wait for connector to reach RUNNING
info "Waiting for connector to reach RUNNING..."
for i in $(seq 1 24); do
  CONN_STATUS=$(confluent connect cluster describe "$CONNECTOR_ID" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    -o json 2>/dev/null | jq -r '.status.state // .connector.state // "UNKNOWN"')
  if [[ "$CONN_STATUS" == "RUNNING" ]]; then
    ok "Connector is RUNNING."
    break
  fi
  if [[ "$CONN_STATUS" == "FAILED" ]]; then
    err "Connector failed to start. Check Confluent Cloud UI → Connectors for error details."
  fi
  echo "   status=$CONN_STATUS, waiting 10 s... ($i/24)"
  sleep 10
done

# ── Health check ──────────────────────────────────────────────────────────────
info "Running health checks..."
HEALTH_ERRORS=0

# 1. Cluster is UP
LIVE_STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.status' || echo "ERROR")
if [[ "$LIVE_STATUS" == "UP" ]]; then
  ok "CHECK cluster $CLUSTER_ID is UP"
else
  echo "✗  FAIL  cluster status = $LIVE_STATUS" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 2. Topics exist
for TOPIC in "${TOPICS[@]}"; do
  TOPIC_EXISTS=$(confluent kafka topic list --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | \
    jq -r --arg t "$TOPIC" '.[] | select(.name == $t) | .name' || echo "")
  if [[ "$TOPIC_EXISTS" == "$TOPIC" ]]; then
    ok "CHECK topic '$TOPIC' exists"
  else
    echo "✗  FAIL  topic '$TOPIC' not found" >&2
    HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
  fi
done

# 3. Service account exists
SA_EXISTS=$(confluent iam service-account list -o json 2>/dev/null | \
  jq -r --arg id "$SA_ID" '.[] | select(.id == $id) | .id' || echo "")
if [[ "$SA_EXISTS" == "$SA_ID" ]]; then
  ok "CHECK service account $SA_ID exists"
else
  echo "✗  FAIL  service account $SA_ID not found" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 4. Kafka API key exists
APIKEY_EXISTS=$(confluent api-key list --resource "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | \
  jq -r --arg key "$KAFKA_API_KEY" '.[] | select(.key == $key) | .key' || echo "")
if [[ "$APIKEY_EXISTS" == "$KAFKA_API_KEY" ]]; then
  ok "CHECK Kafka API key $KAFKA_API_KEY exists"
else
  echo "✗  FAIL  Kafka API key $KAFKA_API_KEY not found" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 5. Connector is RUNNING
CONN_STATUS=$(confluent connect cluster describe "$CONNECTOR_ID" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  -o json 2>/dev/null | jq -r '.status.state // .connector.state // "UNKNOWN"')
if [[ "$CONN_STATUS" == "RUNNING" ]]; then
  ok "CHECK connector $CONNECTOR_ID is RUNNING"
else
  echo "✗  FAIL  connector status = $CONN_STATUS" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 6. .env has no placeholder values for generated secrets
for VAR in CONFLUENT_CLUSTER_ID CONFLUENT_REST_ENDPOINT KAFKA_API_KEY KAFKA_API_SECRET DASHBOARD_INGEST_TOKEN SR_API_KEY SR_API_SECRET; do
  VAL=$(grep "^${VAR}=" "$ENV_FILE" | cut -d= -f2- || true)
  if [[ -z "$VAL" || "$VAL" == *"<"* ]]; then
    echo "✗  FAIL  $VAR is empty or still a placeholder in .env" >&2
    HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
  else
    ok "CHECK $VAR is set in .env"
  fi
done

# ── Final result ──────────────────────────────────────────────────────────────
echo ""
if [[ $HEALTH_ERRORS -eq 0 ]]; then
  echo "════════════════════════════════════════════════════════════"
  echo "  ALL CHECKS PASSED — cluster is fully operational"
  echo ""
  echo "  Cluster ID    : $CLUSTER_ID"
  echo "  REST endpoint : $REST_ENDPOINT"
  echo "  Service acct  : $SA_ID"
  echo "  Topics        : ${TOPICS[*]}"
  echo "  Connector     : $CONNECTOR_ID"
  echo "  .env          : $ENV_FILE"
  echo ""
  echo "  Next steps:"
  echo "  1. Fill in EIA_API_KEY and FRED_API_KEY in .env if not already set,"
  echo "     then push them: echo \"\$EIA_API_KEY\" | npx wrangler secret put EIA_API_KEY"
  echo "  2. In Confluent Cloud Flink workspace, run confluent/flink/01-normalize.sql"
  echo "     (CREATE TABLE first, then EXECUTE STATEMENT SET)"
  echo "  3. Trigger a test collection:"
  echo "     set -a && source .env && set +a"
  echo "     curl -s \"https://$WORKER_SUBDOMAIN.workers.dev/api/collect?source=nbp\""
  echo "       -H \"Authorization: Bearer \$DASHBOARD_INGEST_TOKEN\""
  echo "════════════════════════════════════════════════════════════"
else
  echo "════════════════════════════════════════════════════════════"
  echo "  SETUP INCOMPLETE — $HEALTH_ERRORS check(s) failed (see above)"
  echo "  Fix the issues and re-run: ep-up"
  echo "════════════════════════════════════════════════════════════"
  exit 1
fi
