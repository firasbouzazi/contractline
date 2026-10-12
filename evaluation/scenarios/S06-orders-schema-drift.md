# S06 · Orders topic gains a field (schema drift, tricky)

**Type:** change · **Source:** existing `sales.orders` · **Difficulty:** tricky

The shop backend started sending `coupon_code` (string, optional) in order
events last Tuesday, without telling anyone. Marketing wants it in the
lakehouse. Some events also now contain `customer_email` for guest checkouts.

Decide what the contract should say, what version it becomes, and what happens
to the events that already arrived.

**Done when:** contract change merged with the right version bump.
