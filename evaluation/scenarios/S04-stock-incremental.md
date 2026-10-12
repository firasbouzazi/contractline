# S04 · Warehouse stock levels (incremental JSON files)

**Type:** new dataset · **Source:** files, hourly JSON-lines export of changed rows · **Difficulty:** medium

Each file contains the stock rows that changed since the previous export:
`warehouse_code`, `sku`, `quantity_on_hand`, `quantity_reserved`, `updated_at`.
The key is (`warehouse_code`, `sku`). Exports sometimes arrive late (a file from
09:00 can land after the 10:00 one). We only need the latest stock per key.

`quantity_reserved` can never exceed `quantity_on_hand`; such rows are errors from the WMS.

**Done when:** contract merged, generated tests green. Note any rule you could not express.
