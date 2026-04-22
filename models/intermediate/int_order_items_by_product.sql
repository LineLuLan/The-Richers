-- int_order_items_by_product.sql
-- Mục đích: Tổng hợp hiệu suất bán hàng theo từng product_id
-- Bước aggregate bắt buộc trước khi join vào stg_products
--
-- Schema thực tế stg_order_items:
--   order_id, product_id, promo_id, promo_id_2,
--   quantity, unit_price, discount_amount, gross_item_revenue

WITH product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id)                        AS orders_count,
        SUM(quantity)                                   AS total_units_sold,
        SUM(gross_item_revenue)                         AS gross_revenue,
        SUM(gross_item_revenue - discount_amount)       AS net_revenue,
        SUM(discount_amount)                            AS total_discount_amount,
        AVG(unit_price)                                 AS avg_selling_price,
        -- Tỷ lệ % đơn hàng có áp dụng khuyến mãi
        ROUND(
            COUNT(*) FILTER (WHERE promo_id IS NOT NULL)::DECIMAL
            / COUNT(*) * 100, 2
        )                                               AS promo_attach_rate_pct
    FROM {{ ref('stg_order_items') }}
    GROUP BY product_id
)

SELECT * FROM product_sales