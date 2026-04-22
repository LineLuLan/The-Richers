-- int_sales_daily.sql
-- Mục đích: Tổng hợp doanh thu và lợi nhuận gộp theo ngày
-- Làm spine (trục thời gian) cho mart_temporal_operations
--
-- Schema thực tế stg_sales:
--   Date, Revenue, COGS
-- Lưu ý: stg_sales là bảng aggregated sẵn theo ngày,
-- không có order_id / customer_id / channel

WITH sales_daily AS (
    SELECT
        "Date"                              AS sale_date,
        SUM("Revenue")                      AS total_revenue,
        SUM("COGS")                         AS total_cogs,
        SUM("Revenue") - SUM("COGS")        AS gross_profit,
        CASE
            WHEN SUM("Revenue") > 0
            THEN ROUND(
                (SUM("Revenue") - SUM("COGS")) / SUM("Revenue") * 100, 2)
            ELSE NULL
        END                                 AS gross_margin_pct
    FROM {{ ref('stg_sales') }}
    GROUP BY "Date"
)

SELECT * FROM sales_daily