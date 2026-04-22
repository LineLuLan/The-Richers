-- int_payments_aggregated.sql
-- Mục đích: Enrich thông tin thanh toán cho mỗi đơn hàng
-- Theo schema.yml: stg_payments có quan hệ 1-1 với orders (order_id là unique)
-- Không cần GROUP BY — nhưng giữ CTE để nhất quán với pattern intermediate
--
-- Schema thực tế stg_payments:
--   order_id, payment_method, payment_value, installments

WITH payments AS (
    SELECT
        order_id,
        payment_method,
        payment_value,
        installments,
        -- Phân loại: trả góp hay trả thẳng
        CASE
            WHEN installments > 1 THEN 'installment'
            ELSE 'one_time'
        END AS payment_type
    FROM {{ ref('stg_payments') }}
)

SELECT * FROM payments