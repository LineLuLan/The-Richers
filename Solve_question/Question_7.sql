SELECT 
    g.region,
    SUM(s.revenue) AS total_revenue
FROM raw.sales s
JOIN raw.geography g
    ON s.customer_id = g.customer_id   -- sửa nếu key khác
GROUP BY g.region
ORDER BY total_revenue DESC;