#!/usr/bin/env bash
# Generates a Hatchet API token against the docker-compose hatchet-lite
# instance and writes it to .hatchet-token (used by the e2e test and the
# example commands).
set -euo pipefail
cd "$(dirname "$0")/.."

TENANT_ID="707d0855-80ab-4e1f-a156-f1c4546cbf52" # hatchet-lite default tenant

TOKEN=$(docker compose exec hatchet-lite /hatchet-admin token create \
  --config /config --tenant-id "$TENANT_ID" 2>/dev/null | grep -o 'eyJ[A-Za-z0-9._-]*' | head -1)

if [ -z "$TOKEN" ]; then
  echo "Failed to generate token — is 'docker compose up -d' running?" >&2
  exit 1
fi

printf '%s' "$TOKEN" > .hatchet-token
echo "Token written to .hatchet-token"
echo "Export it with: export HATCHET_CLIENT_TOKEN=\$(cat .hatchet-token) HATCHET_CLIENT_TLS_STRATEGY=none"
