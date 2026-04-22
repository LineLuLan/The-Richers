SELECT 
    traffic_source,
    AVG(bounce_rate) AS avg_bounce_rate
FROM raw.web_traffic
WHERE traffic_source IS NOT NULL
GROUP BY traffic_source
ORDER BY avg_bounce_rate ASC;