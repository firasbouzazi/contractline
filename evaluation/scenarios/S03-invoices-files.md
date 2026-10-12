# S03 · Invoices from the ERP (daily CSV)

**Type:** new dataset · **Source:** files, `/landing/finance/invoices/*.csv`, full export every night · **Difficulty:** baseline

Columns: `invoice_number`, `customer_id`, `issued_date`, `due_date`, `amount_excl_vat`,
`vat_amount`, `currency_code`, `status` (`draft`, `sent`, `paid`, `void`).

Finance asks: "if an invoice disappears from the export, it was deleted in the ERP;
we need to see that it existed". Amounts have two decimals; some are up to 12 digits.

**Done when:** contract merged, generated tests green.
