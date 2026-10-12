# S11 · Gold: monthly recurring revenue

**Type:** gold model · **Inputs:** `silver.finance.subscriptions` (S05) · **Difficulty:** medium

`agg_monthly_recurring_revenue`: for each calendar month and plan, the sum of
`monthly_price` of subscriptions that were `active` on the last day of the
month. Trials and paused subscriptions do not count. A subscription cancelled
on the 31st does not count for that month.

**Done when:** model merged; tests include a subscription that changes plan mid-month.
