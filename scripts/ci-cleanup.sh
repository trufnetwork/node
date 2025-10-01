#!/usr/bin/env bash
set -euo pipefail

echo "[ci-cleanup] Starting cleanup of Kwil DB resources..."

# Stop and remove docker compose stack(s)
if [ -f "compose.yaml" ]; then
  echo "[ci-cleanup] docker compose down for single stack"
  docker compose -f compose.yaml down -v --remove-orphans || true
fi

# Common container names/images
names=("tn-db" "kwil-postgres" "kwild" "postgres")
images=("ghcr.io/trufnetwork/kwil-postgres" "ghcr.io/trufnetwork/node:local" "ghcr.io/trufnetwork/kwil-postgres:latest" "ghcr.io/trufnetwork/kwil-postgres:16.8-1")

echo "[ci-cleanup] Stopping/removing lingering containers by name..."
for n in "${names[@]}"; do
  ids=$(docker ps -aq --filter "name=${n}") || true
  if [ -n "${ids}" ]; then
    echo "[ci-cleanup] Removing containers matching name ${n}: ${ids}"
    docker rm -f ${ids} || true
  fi
done

echo "[ci-cleanup] Stopping/removing lingering containers by image..."
for img in "${images[@]}"; do
  ids=$(docker ps -aq --filter "ancestor=${img}") || true
  if [ -n "${ids}" ]; then
    echo "[ci-cleanup] Removing containers for image ${img}: ${ids}"
    docker rm -f ${ids} || true
  fi
done

# Kill leftover processes that might bind ports (8484, 5432)
ports=(8484 5432)
for p in "${ports[@]}"; do
  if command -v lsof >/dev/null 2>&1; then
    pids=$(lsof -t -i :${p}) || true
    if [ -n "${pids}" ]; then
      echo "[ci-cleanup] Killing processes on port ${p}: ${pids}"
      kill -9 ${pids} || true
    fi
  fi
done

echo "[ci-cleanup] Cleanup complete."

# Best-effort: kill kwild process if it was started directly (not via docker)
if command -v pkill >/dev/null 2>&1; then
  pkill -9 -f "\bkwild\b" 2>/dev/null || true
fi
