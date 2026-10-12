# S07 · Customer feedback with free text (PII in free text, tricky)

**Type:** new dataset · **Source:** Kafka `marketing.feedback.v1` · **Difficulty:** tricky

The survey tool sends `feedback_id`, `customer_id`, `score` (0–10), `comment`
(free text), `submitted_at`. Customers write things like "call me at …" or
"my name is …" in comments. Product wants to analyse comments.

**Done when:** contract merged, and the PR states clearly what happens to `comment` and why.
