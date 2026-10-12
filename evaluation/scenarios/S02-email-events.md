# S02 · Marketing email events (Kafka)

**Type:** new dataset · **Source:** Kafka topic `marketing.email_events.v1` (JSON) · **Difficulty:** baseline

The email platform publishes one event per send, open and click:
`event_id` (uuid), `campaign_id`, `recipient_email`, `event_type` (`sent`, `opened`, `clicked`, `bounced`),
`link_url` (only for clicks), `occurred_at`.

Marketing needs counts per campaign, never the addresses themselves, but they
must be able to count distinct recipients.

**Done when:** contract merged, generated tests green.
