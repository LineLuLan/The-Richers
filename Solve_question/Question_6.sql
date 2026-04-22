SELECT 
    c.age_group,
    COUNT(o.order_id) * 1.0 / COUNT(DISTINCT c.customer_id) AS avg_orders_per_customer
FROM raw.customers c
LEFT JOIN raw.orders o
    ON c.customer_id = o.customer_id
WHERE c.age_group IS NOT NULL
GROUP BY c.age_group
ORDER BY avg_orders_per_customer DESC;