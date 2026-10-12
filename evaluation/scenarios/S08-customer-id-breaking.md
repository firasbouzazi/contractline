# S08 · Customer ids become strings (breaking change, tricky)

**Type:** change · **Source:** existing `sales.customers` · **Difficulty:** tricky

The CRM migrates customer ids from integers to strings like `C-000123` next
month. Debezium will send the new type from the switch-over. `sales.orders`
and `fct_orders` join on `customer_id`.

**Done when:** a plan for the change is in the PR, the contract version reflects it, and every consumer is accounted for.
