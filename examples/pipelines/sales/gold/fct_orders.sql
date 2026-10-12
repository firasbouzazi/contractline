-- One row per order that was not cancelled, with the customer's segment and
-- country as they are today.
SELECT
    o.order_id,
    o.customer_id,
    c.segment AS customer_segment,
    c.country_code,
    o.status,
    o.total_amount,
    o.currency_code,
    CAST(o.ordered_at AS DATE) AS order_date
FROM {{ ref('silver.sales.orders') }} AS o
LEFT JOIN {{ ref('silver.sales.customers') }} AS c
    ON c.customer_id = o.customer_id
    AND c._is_current
WHERE o.status <> 'cancelled'
