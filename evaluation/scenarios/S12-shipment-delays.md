# S12 · Gold: on-time delivery rate

**Type:** gold model · **Inputs:** `silver.logistics.shipments`, `silver.sales.orders` · **Difficulty:** medium

Logistics wants, per week, the share of orders delivered within 3 calendar days
of the order. "Delivered" means the shipment status reached `delivered`. Orders
without a shipment after 3 days are late. Returned shipments count as delivered
for this metric.

**Done when:** model merged; the YAML description states the definition in full.
