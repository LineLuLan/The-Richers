SELECT 
    COUNT(*) AS total_rows,
    COUNT(promo_id) AS promo_rows,
    100.0 * COUNT(promo_id) / COUNT(*) AS promo_percentage
FROM raw.order_items;