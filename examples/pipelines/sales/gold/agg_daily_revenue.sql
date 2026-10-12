-- Revenue per day and currency, from orders that count as sales.
SELECT
    order_date,
    currency_code,
    COUNT(*) AS order_count,
    CAST(SUM(total_amount) AS DECIMAL(18,2)) AS revenue
FROM {{ ref('gold.sales.fct_orders') }}
GROUP BY order_date, currency_code
