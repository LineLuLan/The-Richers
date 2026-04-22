-- int_returns_by_product.sql
-- Mục đích: Tổng hợp hành vi trả hàng theo từng product_id
-- Bước aggregate bắt buộc trước khi join vào stg_products
--
-- Schema thực tế stg_returns:
--   return_id, order_id, product_id, return_date,
--   return_reason, return_quantity, refund_amount

WITH base AS (
    SELECT * FROM {{ ref('stg_returns') }}
),

-- Đếm số lần xuất hiện của mỗi return_reason theo product_id
-- để tìm lý do phổ biến nhất (thay thế MODE() WITHIN GROUP không có trong DuckDB)
reason_counts AS (
    SELECT
        product_id,
        return_reason,
        COUNT(*) AS reason_count
    FROM base
    GROUP BY product_id, return_reason
),

top_reason AS (
    SELECT
        product_id,
        -- DuckDB: max_by(value, score) → lấy value tương ứng với score cao nhất
        max_by(return_reason, reason_count) AS top_return_reason
    FROM reason_counts
    GROUP BY product_id
),

returns_agg AS (
    SELECT
        product_id,
        COUNT(*)                                                        AS total_return_events,
        COUNT(DISTINCT order_id)                                        AS distinct_orders_returned,
        SUM(return_quantity)                                            AS total_units_returned,
        SUM(refund_amount)                                              AS total_refund_amount,
        AVG(refund_amount)                                              AS avg_refund_amount,
        -- Phân phối lý do
        COUNT(*) FILTER (WHERE return_reason = 'wrong_size')            AS returns_wrong_size,
        COUNT(*) FILTER (WHERE return_reason = 'defective')             AS returns_defective,
        COUNT(*) FILTER (WHERE return_reason = 'not_as_described')      AS returns_not_as_described,
        COUNT(*) FILTER (WHERE return_reason = 'changed_mind')          AS returns_changed_mind,
        COUNT(*) FILTER (WHERE return_reason = 'late_delivery')         AS returns_late_delivery
    FROM base
    GROUP BY product_id
)

SELECT
    r.*,
    t.top_return_reason
FROM returns_agg r
LEFT JOIN top_reason t USING (product_id)