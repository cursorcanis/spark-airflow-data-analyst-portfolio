#!/usr/bin/env bash
# Bring the whole stack back to a clean, healthy state. Runs at boot (atl-transit-recover.service) and by hand.
#
# Why this exists: after a crash/reboot, Docker restores containers before Tailscale is up. Every container
# that publishes a port on BIND_ADDR (the Tailscale IP) then fails with "cannot assign requested address" and
# Docker never retries it, leaving a half-started stack (and containers that lost their DNS). The fix is to
# wait until BIND_ADDR exists, then do a clean `down` + staged `up` (storage -> Spark -> Airflow).
#
# Usage: scripts/ops/recover_stack.sh            (from anywhere; finds the repo from its own path)
#        DRY_RUN=1 scripts/ops/recover_stack.sh  (print what would run)
# Env:   ATL_UNLOAD_OLLAMA=1   also unload Ollama models first (frees RAM on a shared machine)
#        ADDR_TIMEOUT_S=600    how long to wait for BIND_ADDR / Docker
set -euo pipefail

REPO_DIR="${REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$REPO_DIR"
ADDR_TIMEOUT_S="${ADDR_TIMEOUT_S:-600}"

log() { echo "[recover $(date -u +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN:-0}" == 1 ]]; then echo "DRY_RUN: $*"; else "$@"; fi; }

wait_for() {  # wait_for <description> <command...>
  local what="$1"; shift
  local waited=0
  until "$@" >/dev/null 2>&1; do
    if (( waited >= ADDR_TIMEOUT_S )); then log "gave up waiting for $what after ${waited}s"; return 1; fi
    sleep 5; waited=$((waited + 5))
  done
  log "$what ready after ${waited}s"
}

# BIND_ADDR from .env (same default as docker-compose.yml)
BIND_ADDR="$(sed -n 's/^BIND_ADDR=//p' .env 2>/dev/null | tail -1)"
BIND_ADDR="${BIND_ADDR:-127.0.0.1}"

wait_for "Docker daemon" docker info
if [[ "$BIND_ADDR" != 127.0.0.1 && "$BIND_ADDR" != 0.0.0.0 ]]; then
  wait_for "address $BIND_ADDR" bash -c "ip -o addr show | grep -qw -- '$BIND_ADDR'"
fi

if [[ "${ATL_UNLOAD_OLLAMA:-0}" == 1 ]] && command -v ollama >/dev/null; then
  for model in $(ollama ps 2>/dev/null | awk 'NR>1 {print $1}'); do
    log "unloading Ollama model $model"; run ollama stop "$model" || true
  done
fi

log "clean restart: down (volumes are kept)"
run docker compose down --remove-orphans

log "stage 1/3: storage"
run docker compose up -d --wait --wait-timeout 180 lake postgres
log "stage 2/3: Spark"
run docker compose up -d --wait --wait-timeout 180 spark-master spark-worker spark-connect
log "stage 3/3: Airflow + poller"
run docker compose up -d

[[ "${DRY_RUN:-0}" == 1 ]] && exit 0

# Every long-running service must be up, and the healthchecked ones healthy. airflow-init is one-shot.
deadline=$((SECONDS + 300))
while :; do
  bad="$(docker compose ps -a --format '{{.Service}} {{.State}} {{.Health}}' \
         | awk '$1 != "airflow-init" && ($2 != "running" || ($3 != "" && $3 != "healthy"))')"
  [[ -z "$bad" ]] && break
  if (( SECONDS > deadline )); then log "not healthy after 5 min:"; echo "$bad"; exit 1; fi
  sleep 10
done
# Containers that start without DNS are the other failure we've seen: check Spark resolves its own name.
if ! docker compose exec -T spark-master getent hosts spark-master >/dev/null; then
  log "spark-master cannot resolve its own name (broken network)"; exit 1
fi
log "stack healthy:"
docker compose ps --format '{{.Service}}: {{.Status}}'
