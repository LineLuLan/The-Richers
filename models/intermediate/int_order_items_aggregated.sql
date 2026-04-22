-- int_order_items_aggregated.sql
-- Mục đích: Tính tổng chi tiết đơn hàng → 1 dòng / order_id
-- Bắt buộc aggregate trước khi join vào orders để tránh fan-out
--
-- Schema thực tế stg_order_items:
--   order_id, product_id, promo_id, promo_id_2,
--   quantity, unit_price, discount_amount, gross_item_revenue

WITH order_items_agg AS (
    SELECT
        order_id,
        COUNT(*)                                                    AS total_line_items,
        COUNT(DISTINCT product_id)                                  AS distinct_products,
        SUM(quantity)                                               AS total_quantity,
        SUM(gross_item_revenue)                                     AS gross_revenue,
        SUM(gross_item_revenue - discount_amount)                   AS net_revenue,
        SUM(discount_amount)                                        AS total_discount_amount,
        AVG(unit_price)                                             AS avg_unit_price,
        MAX(CASE WHEN promo_id IS NOT NULL
                  OR promo_id_2 IS NOT NULL
                 THEN TRUE ELSE FALSE END)                          AS has_promotion
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT * FROM order_items_agg