SELECT 
    r.return_reason,
    COUNT(*) AS total_returns
FROM raw.returns r
JOIN raw.products p
    ON r.product_id = p.product_id
WHERE p.category = 'Streetwear'
GROUP BY r.return_reason
ORDER BY total_returns DESC;