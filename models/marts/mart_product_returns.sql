-- mart_product_returns.sql
-- ============================================================
-- MART 2: Hiệu suất Sản phẩm & Rủi ro Hoàn trả
-- ============================================================
-- Câu hỏi kinh doanh:
--   - Sản phẩm / category / segment nào mang lại doanh thu cao nhất?
--   - Biên lợi nhuận gộp (gross margin) của từng sản phẩm là bao nhiêu?
--   - Tỷ lệ trả hàng cao nhất ở sản phẩm nào? Lý do chính là gì?
--   - Đánh giá trung bình có tương quan với tỷ lệ trả hàng không?
--   - Sản phẩm nào đang hết hàng hoặc cần reorder?
--
-- Kiến trúc Star Schema:
--   CENTER:     stg_products
--   AGGREGATED: int_order_items_by_product  (doanh thu / sản phẩm)
--               int_returns_by_product      (trả hàng / sản phẩm)
--               int_inventory_latest        (tồn kho mới nhất)
--               reviews_agg (inline CTE)    (đánh giá / sản phẩm)
--
-- Schema thực tế stg_products:
--   product_id, product_name, category, segment, size, color, price, cogs
-- ============================================================

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_sales AS (
    SELECT * FROM {{ ref('int_order_items_by_product') }}
),

product_returns AS (
    SELECT * FROM {{ ref('int_returns_by_product') }}
),

-- Aggregate reviews theo product_id (inline — 1 sản phẩm nhiều đánh giá)
reviews_agg AS (
    SELECT
        product_id,
        COUNT(*)                                        AS total_reviews,
        ROUND(AVG(rating), 2)                           AS avg_rating,
        COUNT(*) FILTER (WHERE rating <= 2)             AS negative_reviews,
        COUNT(*) FILTER (WHERE rating >= 4)             AS positive_reviews
    FROM {{ ref('stg_reviews') }}
    GROUP BY product_id
),

inventory AS (
    SELECT * FROM {{ ref('int_inventory_latest') }}
),

final AS (
    SELECT
        -- === ĐỊNH DANH SẢN PHẨM ===
        p.product_id,
        p.product_name,
        p.category,
        p.segment,
        p.size,
        p.color,
        p.price                                         AS list_price,
        p.cogs,

        -- === HIỆU SUẤT BÁN HÀNG ===
        COALESCE(ps.orders_count, 0)                    AS orders_count,
        COALESCE(ps.total_units_sold, 0)                AS total_units_sold,
        COALESCE(ps.gross_revenue, 0)                   AS gross_revenue,
        COALESCE(ps.net_revenue, 0)                     AS net_revenue,
        COALESCE(ps.total_discount_amount, 0)           AS total_discount_amount,
        ps.avg_selling_price,
        ps.promo_attach_rate_pct,

        -- === BIÊN LỢI NHUẬN ===
        -- Dùng cogs từ stg_products (cost cố định / đơn vị)
        CASE
            WHEN ps.net_revenue > 0
            THEN ROUND(
                (ps.net_revenue - (p.cogs * ps.total_units_sold))
                / ps.net_revenue * 100, 2)
            ELSE NULL
        END                                             AS gross_margin_pct,

        -- === RỦI RO HOÀN TRẢ ===
        COALESCE(pr.total_return_events, 0)             AS total_return_events,
        COALESCE(pr.total_units_returned, 0)            AS total_units_returned,
        COALESCE(pr.total_refund_amount, 0)             AS total_refund_amount,
        CASE
            WHEN ps.total_units_sold > 0
            THEN ROUND(
                COALESCE(pr.total_units_returned, 0)::DECIMAL
                / ps.total_units_sold * 100, 2)
            ELSE 0
        END                                             AS return_rate_pct,
        pr.top_return_reason,
        pr.returns_wrong_size,
        pr.returns_defective,
        pr.returns_not_as_described,
        pr.returns_changed_mind,
        pr.returns_late_delivery,

        -- === ĐÁNH GIÁ KHÁCH HÀNG ===
        COALESCE(r.total_reviews, 0)                    AS total_reviews,
        r.avg_rating,
        r.positive_reviews,
        r.negative_reviews,

        -- === TỒN KHO HIỆN TẠI ===
        inv.stock_on_hand,
        inv.days_of_supply,
        inv.fill_rate,
        inv.sell_through_rate,
        inv.stockout_flag,
        inv.overstock_flag,
        inv.reorder_flag,
        inv.latest_snapshot_date,

        -- === PHÂN LOẠI RỦI RO TỔNG HỢP ===
        CASE
            WHEN (
                CASE WHEN ps.total_units_sold > 0
                     THEN COALESCE(pr.total_units_returned, 0)::DECIMAL / ps.total_units_sold
                     ELSE 0 END
            ) > 0.15
                OR COALESCE(r.avg_rating, 5) < 3
            THEN 'high_risk'
            WHEN (
                CASE WHEN ps.total_units_sold > 0
                     THEN COALESCE(pr.total_units_returned, 0)::DECIMAL / ps.total_units_sold
                     ELSE 0 END
            ) BETWEEN 0.05 AND 0.15
            THEN 'medium_risk'
            ELSE 'low_risk'
        END                                             AS product_risk_tier

    FROM products p
    LEFT JOIN product_sales    ps   ON p.product_id = ps.product_id
    LEFT JOIN product_returns  pr   ON p.product_id = pr.product_id
    LEFT JOIN reviews_agg      r    ON p.product_id = r.product_id
    LEFT JOIN inventory        inv  ON p.product_id = inv.product_id
)

SELECT * FROM final