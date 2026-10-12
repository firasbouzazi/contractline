#!/usr/bin/env bash
# Sends the end-to-end scenario's source changes. Run after `make up`.
set -euo pipefail
cd "$(dirname "$0")"
compose=(docker compose -f docker-compose.yml)

echo "postgres: customer 2 becomes a partner, customer 3 is deleted"
"${compose[@]}" exec -T postgres psql -U crm -d crm -q <<'SQL'
UPDATE public.customers SET segment = 'partner', updated_at = now() WHERE customer_id = 2;
DELETE FROM public.customers WHERE customer_id = 3;
SQL

echo "kafka: three order events on sales.orders.v1 (one with an unknown status)"
# Orders are stamped "now": the contract's freshness rule blocks stale data.
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic sales.orders.v1 \
  --property parse.key=true --property 'key.separator=|' <<EVENTS
e2e-1|{"order_id": "e2e-1", "customer_id": 1, "status": "paid", "total_amount": "42.50", "currency_code": "EUR", "ordered_at": "${now}"}
e2e-2|{"order_id": "e2e-2", "customer_id": 2, "status": "placed", "total_amount": "10.00", "currency_code": "TND", "ordered_at": "${now}"}
e2e-3|{"order_id": "e2e-3", "customer_id": 1, "status": "lost", "total_amount": "1.00", "currency_code": "EUR", "ordered_at": "${now}"}
EVENTS
sleep 5  # let Debezium publish the Postgres changes
