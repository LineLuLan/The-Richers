WITH ordered AS (
    SELECT 
        customer_id,
        CAST(order_date AS DATE) AS order_date,
        LAG(CAST(order_date AS DATE)) OVER (
            PARTITION BY customer_id 
            ORDER BY CAST(order_date AS DATE)
        ) AS prev_order_date
    FROM raw.orders
),

gaps AS (
    SELECT 
        customer_id,
        DATE_DIFF('day', prev_order_date, order_date) AS gap_days
    FROM ordered
    WHERE prev_order_date IS NOT NULL
)

SELECT 
    MEDIAN(gap_days) AS median_gap_days
FROM gaps;