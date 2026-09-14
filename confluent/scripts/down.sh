#!/usr/bin/env bash
# confluent/scripts/down.sh
#
# Tears down all Confluent Cloud resources created by up.sh.
# Reads cluster ID and service account ID from .env at the repo root.
#
# What this deletes:
#   - Kafka API key (from .env)
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

[[ -z "$CONFLUENT_CLUSTER_ID" ]]        && err "CONFLUENT_CLUSTER_ID not found in .env"
[[ -z "$CONFLUENT_ENVIRONMENT_ID" ]]    && err "CONFLUENT_ENVIRONMENT_ID not found in .env"
[[ -z "$CONFLUENT_SERVICE_ACCOUNT_ID" ]] && warn "CONFLUENT_SERVICE_ACCOUNT_ID not found in .env — skipping service account deletion."

# ── Confirm ───────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  This will PERMANENTLY DELETE:"
echo "  Cluster        : $CONFLUENT_CLUSTER_ID"
echo "  Environment    : $CONFLUENT_ENVIRONMENT_ID"
echo "  Service account: ${CONFLUENT_SERVICE_ACCOUNT_ID:-(not set)}"
echo "  Kafka API key  : ${KAFKA_API_KEY:-(not set)}"
echo "════════════════════════════════════════════════════════════"
echo ""
read -r -p "Type 'yes' to confirm deletion: " CONFIRM
[[ "$CONFIRM" == "yes" ]] || { echo "Aborted."; exit 0; }

# ── Delete Kafka API key ──────────────────────────────────────────────────────
if [[ -n "$KAFKA_API_KEY" ]]; then
  info "Deleting Kafka API key $KAFKA_API_KEY..."
  confluent api-key delete "$KAFKA_API_KEY" --force 2>/dev/null && ok "API key deleted." || warn "API key deletion failed (may already be gone)."
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

# ── Scrub .env — remove Confluent-generated values, keep external API keys ────
info "Scrubbing generated secrets from $ENV_FILE..."

# Preserve only external API keys that user obtained manually
EIA_API_KEY=$(grep "^EIA_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)
FRED_API_KEY=$(grep "^FRED_API_KEY=" "$ENV_FILE" | cut -d= -f2- || true)

cat > "$ENV_FILE" <<EOF
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

# Cloudflare Worker — cleared
DASHBOARD_INGEST_TOKEN=

# External API keys — preserved
EIA_API_KEY=${EIA_API_KEY}
FRED_API_KEY=${FRED_API_KEY}
EOF

ok ".env scrubbed (external API keys preserved)."

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

# 3. .env generated values are cleared
for VAR in CONFLUENT_CLUSTER_ID CONFLUENT_REST_ENDPOINT KAFKA_API_KEY KAFKA_API_SECRET DASHBOARD_INGEST_TOKEN; do
  VAL=$(grep "^${VAR}=" "$ENV_FILE" | cut -d= -f2- || true)
  if [[ -z "$VAL" ]]; then
    ok "CHECK $VAR is cleared in .env"
  else
    echo "✗  FAIL  $VAR still has a value in .env: $VAL" >&2
    HEALTH_ERRORS=$((HEALTH_ERRORS + 1))
  fi
done

# ── Final result ──────────────────────────────────────────────────────────────
echo ""
if [[ $HEALTH_ERRORS -eq 0 ]]; then
  echo "════════════════════════════════════════════════════════════"
  echo "  ALL CHECKS PASSED — Confluent resources fully deleted"
  echo "  .env cleared (EIA_API_KEY / FRED_API_KEY preserved)"
  echo ""
  echo "  Note: Cloudflare Worker secrets still point at the old"
  echo "  cluster. After running ep-up tomorrow, re-push secrets:"
  echo "    set -a && source .env && set +a"
  echo "    cd cloudflare"
  echo '    echo "$CONFLUENT_REST_ENDPOINT" | npx wrangler secret put CONFLUENT_REST_ENDPOINT'
  echo '    echo "$CONFLUENT_CLUSTER_ID"    | npx wrangler secret put CONFLUENT_CLUSTER_ID'
  echo '    echo "$KAFKA_API_KEY"           | npx wrangler secret put KAFKA_API_KEY'
  echo '    echo "$KAFKA_API_SECRET"        | npx wrangler secret put KAFKA_API_SECRET'
  echo '    echo "$DASHBOARD_INGEST_TOKEN"  | npx wrangler secret put DASHBOARD_INGEST_TOKEN'
  echo "════════════════════════════════════════════════════════════"
else
  echo "════════════════════════════════════════════════════════════"
  echo "  TEARDOWN INCOMPLETE — $HEALTH_ERRORS check(s) failed (see above)"
  echo "  Some resources may still be running and incurring costs."
  echo "  Check Confluent Cloud UI and delete manually if needed."
  echo "════════════════════════════════════════════════════════════"
  exit 1
fi
