SELECT 
    segment,
    Round(AVG((price - cogs) / price),2) AS avg_margin
FROM raw.products
WHERE price > 0   -- tránh chia cho 0
GROUP BY segment
ORDER BY avg_margin DESC;