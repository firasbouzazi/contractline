#!/usr/bin/env bash
# Checks that the running local stack works end to end.
# Usage: infra/smoke.sh [cdc] [orchestration]   (default: cdc)
set -euo pipefail

cd "$(dirname "$0")"
compose=(docker compose -f docker-compose.yml)
profiles=("${@:-cdc}")

check_cdc() {
  local marker="smoke-$(date +%s)@example.com"
  echo "cdc: inserting a row into crm.public.customers"
  "${compose[@]}" exec -T postgres psql -U crm -d crm -qc \
    "INSERT INTO public.customers (email, country_code, segment) VALUES ('${marker}', 'FR', 'consumer');"

  echo "cdc: waiting for the change on topic cdc.crm.public.customers"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic cdc.crm.public.customers \
        --from-beginning --timeout-ms 5000 2>/dev/null | grep -q "${marker}"; then
      echo "cdc: OK"
      return 0
    fi
  done
  echo "cdc: FAILED, the inserted row never reached Kafka" >&2
  return 1
}

check_orchestration() {
  echo "orchestration: checking Airflow health"
  if curl -fsS http://localhost:8080/api/v2/monitor/health | grep -q '"healthy"'; then
    echo "orchestration: OK"
  else
    echo "orchestration: FAILED, Airflow is not healthy" >&2
    return 1
  fi
}

for profile in "${profiles[@]}"; do
  case "$profile" in
    cdc) check_cdc ;;
    orchestration) check_orchestration ;;
    *) echo "unknown profile: $profile" >&2; exit 2 ;;
  esac
done
