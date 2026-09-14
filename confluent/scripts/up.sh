#!/usr/bin/env bash
# confluent/scripts/up.sh
#
# Provisions a Confluent Cloud cluster for the economic-pulse demo and writes
# all generated secrets into .env at the repo root.
#
# Prerequisites:
#   - confluent CLI installed and logged in  (confluent login)
#   - jq installed
#
# Usage:
#   bash confluent/scripts/up.sh
#
# After this script completes, source the .env and push secrets to Cloudflare:
#   set -a && source .env && set +a
#   cd cloudflare
#   echo "$CONFLUENT_REST_ENDPOINT" | npx wrangler secret put CONFLUENT_REST_ENDPOINT
#   echo "$CONFLUENT_CLUSTER_ID"    | npx wrangler secret put CONFLUENT_CLUSTER_ID
#   echo "$KAFKA_API_KEY"           | npx wrangler secret put KAFKA_API_KEY
#   echo "$KAFKA_API_SECRET"        | npx wrangler secret put KAFKA_API_SECRET
#   echo "$DASHBOARD_INGEST_TOKEN"  | npx wrangler secret put DASHBOARD_INGEST_TOKEN
#   echo "$EIA_API_KEY"             | npx wrangler secret put EIA_API_KEY
#   echo "$FRED_API_KEY"            | npx wrangler secret put FRED_API_KEY

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
CLUSTER_NAME="economic-pulse"
CLOUD="aws"
REGION="eu-central-1"       # change to your preferred region
CLUSTER_TYPE="basic"
SA_NAME="economic-pulse-sa"

# Repo root (one level above this script's directory)
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

# Topics to create
TOPICS=(
  "economic.raw"
)

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo "▶  $*"; }
ok()    { echo "✓  $*"; }
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

# ── Grant DeveloperWrite role on the cluster ──────────────────────────────────
info "Granting DeveloperWrite to $SA_ID on cluster $CLUSTER_ID..."
confluent iam rbac role-binding create \
  --principal "ServiceAccount:$SA_ID" \
  --role DeveloperWrite \
  --environment "$ENV_ID" \
  --cloud-cluster "$CLUSTER_ID" \
  --kafka-cluster "$CLUSTER_ID" \
  --resource "Topic:economic.raw" \
  2>/dev/null || true   # idempotent

# Also grant DeveloperRead so the HTTP Sink connector can consume
confluent iam rbac role-binding create \
  --principal "ServiceAccount:$SA_ID" \
  --role DeveloperRead \
  --environment "$ENV_ID" \
  --cloud-cluster "$CLUSTER_ID" \
  --kafka-cluster "$CLUSTER_ID" \
  --resource "Topic:economic.raw" \
  2>/dev/null || true

ok "RBAC role bindings applied."

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

# Cloudflare Worker
DASHBOARD_INGEST_TOKEN=$DASHBOARD_INGEST_TOKEN

# External API keys — fill in manually if not already set
EIA_API_KEY=${EXISTING_EIA:-<your-eia-api-key>}
FRED_API_KEY=${EXISTING_FRED:-<your-fred-api-key>}
EOF

ok ".env written."

# ── Health check — verify everything is actually reachable ────────────────────
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

# 2. Topic exists
TOPIC_EXISTS=$(confluent kafka topic list --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | \
  jq -r '.[] | select(.name == "economic.raw") | .name' || echo "")
if [[ "$TOPIC_EXISTS" == "economic.raw" ]]; then
  ok "CHECK topic 'economic.raw' exists"
else
  echo "✗  FAIL  topic 'economic.raw' not found" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 3. Service account exists
SA_EXISTS=$(confluent iam service-account list -o json 2>/dev/null | \
  jq -r --arg id "$SA_ID" '.[] | select(.id == $id) | .id' || echo "")
if [[ "$SA_EXISTS" == "$SA_ID" ]]; then
  ok "CHECK service account $SA_ID exists"
else
  echo "✗  FAIL  service account $SA_ID not found" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 4. API key exists
APIKEY_EXISTS=$(confluent api-key list --resource "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | \
  jq -r --arg key "$KAFKA_API_KEY" '.[] | select(.key == $key) | .key' || echo "")
if [[ "$APIKEY_EXISTS" == "$KAFKA_API_KEY" ]]; then
  ok "CHECK Kafka API key $KAFKA_API_KEY exists"
else
  echo "✗  FAIL  Kafka API key $KAFKA_API_KEY not found" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 5. .env has no placeholder values for generated secrets
for VAR in CONFLUENT_CLUSTER_ID CONFLUENT_REST_ENDPOINT KAFKA_API_KEY KAFKA_API_SECRET DASHBOARD_INGEST_TOKEN; do
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
  echo "  Cluster ID   : $CLUSTER_ID"
  echo "  REST endpoint: $REST_ENDPOINT"
  echo "  Service acct : $SA_ID"
  echo "  Topics       : ${TOPICS[*]}"
  echo "  .env         : $ENV_FILE"
  echo ""
  echo "  Next steps:"
  echo "  1. Fill in EIA_API_KEY and FRED_API_KEY in .env if not already set"
  echo "  2. Push secrets to Cloudflare Worker:"
  echo "     set -a && source .env && set +a"
  echo "     cd cloudflare"
  echo '     echo "$CONFLUENT_REST_ENDPOINT" | npx wrangler secret put CONFLUENT_REST_ENDPOINT'
  echo '     echo "$CONFLUENT_CLUSTER_ID"    | npx wrangler secret put CONFLUENT_CLUSTER_ID'
  echo '     echo "$KAFKA_API_KEY"           | npx wrangler secret put KAFKA_API_KEY'
  echo '     echo "$KAFKA_API_SECRET"        | npx wrangler secret put KAFKA_API_SECRET'
  echo '     echo "$DASHBOARD_INGEST_TOKEN"  | npx wrangler secret put DASHBOARD_INGEST_TOKEN'
  echo '     echo "$EIA_API_KEY"             | npx wrangler secret put EIA_API_KEY'
  echo '     echo "$FRED_API_KEY"            | npx wrangler secret put FRED_API_KEY'
  echo "  3. Deploy the Worker: npx wrangler deploy"
  echo "════════════════════════════════════════════════════════════"
else
  echo "════════════════════════════════════════════════════════════"
  echo "  SETUP INCOMPLETE — $HEALTH_ERRORS check(s) failed (see above)"
  echo "  Fix the issues and re-run: ep-up"
  echo "════════════════════════════════════════════════════════════"
  exit 1
fi
