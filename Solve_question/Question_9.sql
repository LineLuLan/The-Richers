WITH total_items AS (
    SELECT 
        p.size,
        COUNT(*) AS total_items
    FROM raw.order_items oi
    JOIN raw.products p
        ON oi.product_id = p.product_id
    GROUP BY p.size
),

returned_items AS (
    SELECT 
        p.size,
        COUNT(*) AS total_returns
    FROM raw.returns r
    JOIN raw.products p
        ON r.product_id = p.product_id
    GROUP BY p.size
)

SELECT 
    t.size,
    t.total_items,
    COALESCE(r.total_returns, 0) AS total_returns,
    COALESCE(r.total_returns, 0) * 1.0 / t.total_items AS return_rate
FROM total_items t
LEFT JOIN returned_items r
    ON t.size = r.size
WHERE t.size IN ('S','M','L','XL')
ORDER BY return_rate DESC;