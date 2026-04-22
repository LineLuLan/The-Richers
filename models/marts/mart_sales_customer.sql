-- mart_sales_customer.sql
-- ============================================================
-- MART 1: Doanh thu & Chân dung Khách hàng
-- ============================================================
-- Câu hỏi kinh doanh:
--   - Ai đang mua hàng? (age_group, gender, acquisition_channel)
--   - Họ ở đâu? (city, region, district)
--   - Đơn hàng đến từ nguồn nào? (order_source, device_type)
--   - Phương thức thanh toán nào phổ biến? Trả góp hay trả thẳng?
--
-- Kiến trúc Star Schema:
--   FACT:       stg_orders          (trung tâm)
--   DIMENSIONS: stg_customers       (chân dung khách hàng — join qua customer_id)
--               stg_geography       (địa lý — join qua zip)
--   AGGREGATED: int_order_items_aggregated  (doanh thu / đơn — join qua order_id)
--               int_payments_aggregated     (thanh toán / đơn — join qua order_id, 1-1)
--
-- Lưu ý thực tế:
--   - stg_customers dùng zip làm địa chỉ, stg_geography dùng zip làm PK
--   - stg_orders không có promotion_id; promotion nằm trong stg_order_items
--   - stg_payments là 1-1 với orders (không cần aggregate)
-- ============================================================

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

geography AS (
    SELECT * FROM {{ ref('stg_geography') }}
),

order_items_agg AS (
    SELECT * FROM {{ ref('int_order_items_aggregated') }}
),

payments AS (
    SELECT * FROM {{ ref('int_payments_aggregated') }}
),

final AS (
    SELECT
        -- === ĐỊNH DANH ĐƠN HÀNG ===
        o.order_id,
        o.order_date,
        o.order_status,
        o.order_source,
        o.device_type,
        o.payment_method                                AS order_payment_method,

        -- === CHÂN DUNG KHÁCH HÀNG ===
        o.customer_id,
        c.gender,
        c.age_group,
        c.acquisition_channel,
        c.signup_date,
        DATE_DIFF('day', c.signup_date, o.order_date)   AS days_since_signup,

        -- === ĐỊA LÝ (join orders.zip → geography.zip) ===
        g.city,
        g.region,
        g.district,

        -- === DOANH THU (từ int aggregate) ===
        oi.total_line_items,
        oi.distinct_products,
        oi.total_quantity,
        oi.gross_revenue,
        oi.net_revenue,
        oi.total_discount_amount,
        oi.avg_unit_price,
        oi.has_promotion,

        -- Tỷ lệ chiết khấu trên đơn hàng
        CASE
            WHEN oi.gross_revenue > 0
            THEN ROUND(oi.total_discount_amount / oi.gross_revenue * 100, 2)
            ELSE 0
        END                                             AS order_discount_rate_pct,

        -- === THANH TOÁN (1-1 với orders) ===
        pay.payment_value,
        pay.installments,
        pay.payment_type,                               -- 'installment' vs 'one_time'

        -- === PHÂN LOẠI ĐƠN HÀNG ===
        CASE
            WHEN oi.gross_revenue >= 2000000 THEN 'high_value'
            WHEN oi.gross_revenue >= 500000  THEN 'mid_value'
            ELSE 'low_value'
        END                                             AS order_value_tier

    FROM orders o
    LEFT JOIN customers         c   ON o.customer_id = c.customer_id
    LEFT JOIN geography         g   ON o.zip         = g.zip
    LEFT JOIN order_items_agg   oi  ON o.order_id    = oi.order_id
    LEFT JOIN payments          pay ON o.order_id    = pay.order_id
)

SELECT * FROM final