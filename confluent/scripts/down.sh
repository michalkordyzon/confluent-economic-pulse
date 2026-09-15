#!/usr/bin/env bash
# confluent/scripts/down.sh
#
# Tears down all Confluent Cloud resources created by up.sh.
# Reads cluster ID and service account ID from .env at the repo root.
#
# What this deletes:
#   - HTTP Sink V2 connector
#   - Kafka API key (from .env)
#   - Schema Registry API key (from .env)
#   - Service account (from .env)
#   - Kafka cluster (from .env)
#
# What this does NOT delete:
#   - The Confluent environment itself (shared, kept)
#   - EIA_API_KEY and FRED_API_KEY entries in .env (external, kept)
#   - Cloudflare D1 database or Worker secrets (separate resource)
#
# Usage:
#   bash confluent/scripts/down.sh

set -euo pipefail

CONNECTOR_NAME="economic-pulse-dashboard-sink"

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo "▶  $*"; }
ok()    { echo "✓  $*"; }
warn()  { echo "⚠  $*"; }
err()   { echo "✗  $*" >&2; exit 1; }

require() {
  command -v "$1" &>/dev/null || err "'$1' is required but not found."
}

require confluent
require jq

# ── Load .env ─────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

[[ -f "$ENV_FILE" ]] || err ".env not found at $ENV_FILE — nothing to tear down."

# Source only the variables we need
CONFLUENT_CLUSTER_ID=$(grep "^CONFLUENT_CLUSTER_ID=" "$ENV_FILE" | cut -d= -f2- || true)
CONFLUENT_ENVIRONMENT_ID=$(grep "^CONFLUENT_ENVIRONMENT_ID=" "$ENV_FILE" | cut -d= -f2- || true)
CONFLUENT_SERVICE_ACCOUNT_ID=$(grep "^CONFLUENT_SERVICE_ACCOUNT_ID=" "$ENV_FILE" | cut -d= -f2- || true)
KAFKA_API_KEY=$(grep "^KAFKA_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)
SR_API_KEY=$(grep "^SR_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)

[[ -z "$CONFLUENT_CLUSTER_ID" ]]        && err "CONFLUENT_CLUSTER_ID not found in .env"
[[ -z "$CONFLUENT_ENVIRONMENT_ID" ]]    && err "CONFLUENT_ENVIRONMENT_ID not found in .env"
[[ -z "$CONFLUENT_SERVICE_ACCOUNT_ID" ]] && warn "CONFLUENT_SERVICE_ACCOUNT_ID not found in .env — skipping service account deletion."

# ── Confirm ───────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  This will PERMANENTLY DELETE:"
echo "  Connector      : $CONNECTOR_NAME (if exists)"
echo "  Cluster        : $CONFLUENT_CLUSTER_ID"
echo "  Service account: ${CONFLUENT_SERVICE_ACCOUNT_ID:-(not set)}"
echo "  Kafka API key  : ${KAFKA_API_KEY:-(not set)}"
echo "  SR API key     : ${SR_API_KEY:-(not set)}"
echo "  (environment $CONFLUENT_ENVIRONMENT_ID is kept)"
echo "════════════════════════════════════════════════════════════"
echo ""
read -r -p "Type 'yes' to confirm deletion: " CONFIRM
[[ "$CONFIRM" == "yes" ]] || { echo "Aborted."; exit 0; }

# ── Delete HTTP Sink V2 connector ─────────────────────────────────────────────
info "Looking for connector '$CONNECTOR_NAME'..."
CONNECTOR_ID=$(confluent connect cluster list \
  --cluster "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID" \
  -o json 2>/dev/null | jq -r --arg name "$CONNECTOR_NAME" '.[] | select(.name == $name) | .id' | head -1)

if [[ -n "$CONNECTOR_ID" ]]; then
  info "Deleting connector $CONNECTOR_ID..."
  confluent connect cluster delete "$CONNECTOR_ID" \
    --cluster "$CONFLUENT_CLUSTER_ID" \
    --environment "$CONFLUENT_ENVIRONMENT_ID" \
    --force 2>/dev/null && ok "Connector deleted." || warn "Connector deletion failed (may already be gone)."
else
  ok "No connector named '$CONNECTOR_NAME' found — skipping."
fi

# ── Delete Kafka API key ──────────────────────────────────────────────────────
if [[ -n "$KAFKA_API_KEY" ]]; then
  info "Deleting Kafka API key $KAFKA_API_KEY..."
  confluent api-key delete "$KAFKA_API_KEY" --force 2>/dev/null && ok "Kafka API key deleted." || warn "Kafka API key deletion failed (may already be gone)."
fi

# ── Delete Schema Registry API key ───────────────────────────────────────────
if [[ -n "$SR_API_KEY" ]]; then
  info "Deleting Schema Registry API key $SR_API_KEY..."
  confluent api-key delete "$SR_API_KEY" --force 2>/dev/null && ok "SR API key deleted." || warn "SR API key deletion failed (may already be gone)."
fi

# ── Delete service account ────────────────────────────────────────────────────
if [[ -n "$CONFLUENT_SERVICE_ACCOUNT_ID" ]]; then
  info "Deleting service account $CONFLUENT_SERVICE_ACCOUNT_ID..."
  confluent iam service-account delete "$CONFLUENT_SERVICE_ACCOUNT_ID" --force 2>/dev/null \
    && ok "Service account deleted." \
    || warn "Service account deletion failed (may already be gone)."
fi

# ── Delete Kafka cluster ──────────────────────────────────────────────────────
info "Deleting Kafka cluster $CONFLUENT_CLUSTER_ID..."
confluent kafka cluster delete "$CONFLUENT_CLUSTER_ID" \
  --environment "$CONFLUENT_ENVIRONMENT_ID" \
  --force 2>/dev/null \
  && ok "Cluster deleted." \
  || warn "Cluster deletion failed (may already be gone)."

# ── Health check — verify resources are actually gone ─────────────────────────
info "Running teardown checks..."
HEALTH_ERRORS=0

# 1. Cluster is gone
CLUSTER_STILL_EXISTS=$(confluent kafka cluster list --environment "$CONFLUENT_ENVIRONMENT_ID" -o json 2>/dev/null | \
  jq -r --arg id "$CONFLUENT_CLUSTER_ID" '.[] | select(.id == $id) | .id' || echo "")
if [[ -z "$CLUSTER_STILL_EXISTS" ]]; then
  ok "CHECK cluster $CONFLUENT_CLUSTER_ID is gone"
else
  echo "✗  FAIL  cluster $CONFLUENT_CLUSTER_ID still exists" >&2
  HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
fi

# 2. Service account is gone
if [[ -n "$CONFLUENT_SERVICE_ACCOUNT_ID" ]]; then
  SA_STILL_EXISTS=$(confluent iam service-account list -o json 2>/dev/null | \
    jq -r --arg id "$CONFLUENT_SERVICE_ACCOUNT_ID" '.[] | select(.id == $id) | .id' || echo "")
  if [[ -z "$SA_STILL_EXISTS" ]]; then
    ok "CHECK service account $CONFLUENT_SERVICE_ACCOUNT_ID is gone"
  else
    echo "✗  FAIL  service account $CONFLUENT_SERVICE_ACCOUNT_ID still exists" >&2
    HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
  fi
fi

# ── Final result ──────────────────────────────────────────────────────────────
echo ""
if [[ $HEALTH_ERRORS -eq 0 ]]; then
  # ── Clear Confluent values from .env (preserve external API keys) ───────────
  info "Clearing Confluent secrets from $ENV_FILE..."

  EXISTING_EIA=$(grep  "^EIA_API_KEY="  "$ENV_FILE" | cut -d= -f2- || true)
  EXISTING_FRED=$(grep "^FRED_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)

  cat > "$ENV_FILE" <<ENVEOF
# Confluent resources deleted on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Run confluent/scripts/up.sh to recreate.

# Confluent Cloud — cleared
CONFLUENT_ENVIRONMENT_ID=
CONFLUENT_CLUSTER_ID=
CONFLUENT_REST_ENDPOINT=
CONFLUENT_SERVICE_ACCOUNT_ID=

# Kafka API key — cleared
KAFKA_API_KEY=
KAFKA_API_SECRET=

# Schema Registry — cleared
SR_ENDPOINT=
SR_API_KEY=
SR_API_SECRET=

# Cloudflare Worker — cleared
DASHBOARD_INGEST_TOKEN=

# External API keys — preserved
EIA_API_KEY=${EXISTING_EIA}
FRED_API_KEY=${EXISTING_FRED}
ENVEOF

  ok ".env cleared (external API keys preserved)."

  echo "════════════════════════════════════════════════════════════"
  echo "  ALL CHECKS PASSED — Confluent resources fully deleted"
  echo "  .env cleared — Confluent values erased, API keys kept."
  echo ""
  echo "  Note: Cloudflare Worker secrets still point at the old"
  echo "  cluster. After running up.sh, secrets are pushed automatically."
  echo "════════════════════════════════════════════════════════════"
else
  echo "════════════════════════════════════════════════════════════"
  echo "  TEARDOWN INCOMPLETE — $HEALTH_ERRORS check(s) failed (see above)"
  echo "  .env was NOT modified — fix the issues above first."
  echo "  Some resources may still be running and incurring costs."
  echo "  Check Confluent Cloud UI and delete manually if needed."
  echo "════════════════════════════════════════════════════════════"
  exit 1
fi
