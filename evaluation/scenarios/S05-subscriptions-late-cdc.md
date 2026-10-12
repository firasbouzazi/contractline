# S05 · Subscriptions with late CDC events (tricky)

**Type:** new dataset · **Source:** Postgres `billing.public.subscriptions` via Debezium · **Difficulty:** tricky

Columns: `subscription_id`, `customer_id`, `plan_code`, `monthly_price`,
`status` (`trial`, `active`, `paused`, `cancelled`), `valid_from`, `updated_at`.

The billing team replays failed batches: CDC events for a subscription can
arrive hours after newer ones. Finance computes MRR from the history, so the
history must be right even when events arrive out of order. Cancelled
subscriptions are deleted from the source table after 30 days.

**Done when:** contract merged, generated tests green, and the PR explains how out-of-order events are handled.
