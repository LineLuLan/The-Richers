-- int_inventory_latest.sql
-- Mục đích: Lấy snapshot tồn kho mới nhất cho mỗi product_id
-- Bảng inventory chụp theo tháng (year + month) → cần ROW_NUMBER
-- để đảm bảo 1 dòng / product_id khi join vào mart_product_returns
--
-- Schema thực tế stg_inventory:
--   snapshot_date, product_id, stock_on_hand, units_received, units_sold,
--   stockout_days, days_of_supply, fill_rate, sell_through_rate,
--   stockout_flag, overstock_flag, reorder_flag,
--   product_name, category, segment, year, month

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ ref('stg_inventory') }}
),

latest AS (
    SELECT
        product_id,
        snapshot_date               AS latest_snapshot_date,
        stock_on_hand,
        units_received,
        units_sold                  AS units_sold_last_period,
        stockout_days,
        days_of_supply,
        fill_rate,
        sell_through_rate,
        stockout_flag,
        overstock_flag,
        reorder_flag,
        category,
        segment
    FROM ranked
    WHERE rn = 1
)

SELECT * FROM latest