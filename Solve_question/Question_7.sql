SELECT 
    g.region,
    SUM(p.payment_value) AS total_revenue
FROM raw.orders o
JOIN raw.payments p 
    ON o.order_id = p.order_id
JOIN raw.geography g
    ON o.zip = g.zip
GROUP BY g.region
ORDER BY total_revenue DESC;