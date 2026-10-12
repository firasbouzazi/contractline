# S10 · Gold: customer dimension

**Type:** gold model · **Inputs:** `silver.sales.customers` · **Difficulty:** baseline

A `dim_customer` with one row per customer as they are today: id, segment,
country, customer since (date of `created_at`), and `is_active` (true unless the
customer was deleted at the source).

**Done when:** model merged with tests that cover a deleted customer.
