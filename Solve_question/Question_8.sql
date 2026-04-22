SELECT 
    payment_method,
    COUNT(*) AS total_orders
FROM raw.orders
WHERE LOWER(order_status) = 'cancelled'
GROUP BY payment_method
ORDER BY total_orders DESC;