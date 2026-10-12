# S01 · B2B accounts from the CRM (Postgres CDC)

**Type:** new dataset · **Source:** Postgres `crm.public.accounts` via Debezium · **Difficulty:** baseline

Sales wants the list of business accounts in the lakehouse, with history: we
need to know which account manager owned an account at any point in time.

Columns in the source: `account_id` (bigint, PK), `company_name`, `vat_number`,
`account_manager_email`, `country_code`, `tier` (`bronze`, `silver`, `gold`),
`created_at`, `updated_at`.

- Track history of `account_manager_email` and `tier`.
- The account manager's email is an employee's personal data.
- Accounts are never deleted at the source; they get `tier = closed` instead. (Is `closed` a tier? Ask.)

**Done when:** contract merged, generated tests green.
