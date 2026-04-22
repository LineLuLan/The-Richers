-- mart_temporal_operations.sql
-- ============================================================
-- MART 3: Dòng thời gian, Vận hành & Tồn kho
-- ============================================================
-- Câu hỏi kinh doanh:
--   - Xu hướng doanh thu và lợi nhuận theo ngày / tháng / mùa vụ?
--   - Lưu lượng web có chuyển đổi thành doanh thu không?
--   - Kênh traffic nào hiệu quả nhất (revenue per session)?
--   - Thời gian giao hàng trung bình là bao nhiêu ngày?
--   - Khi lưu lượng cao mà stockout_flag bật → đốt tiền marketing?
--
-- Kiến trúc Time-Series (spine theo ngày):
--   SPINE:   int_sales_daily          (trục ngày từ stg_sales)
--   JOINED:  int_web_traffic_daily    (lưu lượng cùng ngày)
--   INLINE:  shipments_daily          (giao hàng theo ship_date)
--   INLINE:  inventory_daily          (tỷ lệ stockout theo snapshot_date)
--
-- Schema thực tế:
--   stg_sales:     Date, Revenue, COGS
--   stg_web_traffic: date, sessions, unique_visitors, page_views,
--                    bounce_rate, avg_session_duration_sec, traffic_source
--   stg_shipments: order_id, ship_date, delivery_date, shipping_fee
--   stg_inventory: snapshot_date, product_id, stock_on_hand, stockout_flag, ...
-- ============================================================

WITH sales_daily AS (
    SELECT * FROM {{ ref('int_sales_daily') }}
),

web_traffic_daily AS (
    SELECT * FROM {{ ref('int_web_traffic_daily') }}
),

-- Aggregate shipments theo ship_date
shipments_daily AS (
    SELECT
        ship_date,
        COUNT(*)                                                        AS total_shipments,
        COUNT(*) FILTER (WHERE delivery_date IS NOT NULL)               AS delivered_count,
        -- Giao hàng trễ: delivery_date - ship_date > 7 ngày
        -- DuckDB: DATE - DATE trả về BIGINT (số ngày), dùng trực tiếp
        COUNT(*) FILTER (
            WHERE delivery_date IS NOT NULL
              AND (delivery_date - ship_date) > 7
        )                                                               AS delayed_count,
        AVG((delivery_date - ship_date))
            FILTER (WHERE delivery_date IS NOT NULL)                    AS avg_delivery_days,
        SUM(shipping_fee)                                               AS total_shipping_fee
    FROM {{ ref('stg_shipments') }}
    GROUP BY ship_date
),

-- Aggregate inventory stockout theo snapshot_date (monthly)
inventory_daily AS (
    SELECT
        snapshot_date,
        COUNT(*)                                                        AS total_products_tracked,
        SUM(CASE WHEN stockout_flag THEN 1 ELSE 0 END)                 AS products_out_of_stock,
        SUM(stock_on_hand)                                              AS total_stock_on_hand,
        ROUND(
            SUM(CASE WHEN stockout_flag THEN 1 ELSE 0 END)::DECIMAL
            / NULLIF(COUNT(*), 0) * 100, 2
        )                                                               AS stockout_rate_pct,
        SUM(CASE WHEN reorder_flag THEN 1 ELSE 0 END)                  AS products_need_reorder
    FROM {{ ref('stg_inventory') }}
    GROUP BY snapshot_date
),

final AS (
    SELECT
        -- === TRỤC THỜI GIAN ===
        -- DuckDB: DATE_PART trả về DOUBLE, cast sang INTEGER cho gọn
        s.sale_date,
        YEAR(s.sale_date)                           AS year,
        MONTH(s.sale_date)                          AS month,
        WEEK(s.sale_date)                           AS week_of_year,
        DAYOFWEEK(s.sale_date)                      AS day_of_week,   -- 0=Sun
        -- DuckDB: dùng strftime thay TO_CHAR
        strftime(s.sale_date, '%b %Y')              AS month_label,

        -- Nhãn sự kiện thương mại
        -- DuckDB: dùng strftime thay TO_CHAR
        CASE
            WHEN strftime(s.sale_date, '%m-%d') = '11-11'              THEN '11.11 Siêu Sale'
            WHEN strftime(s.sale_date, '%m-%d') = '12-12'              THEN '12.12 Siêu Sale'
            WHEN MONTH(s.sale_date) IN (1, 2)                          THEN 'Tết / Lunar New Year'
            WHEN strftime(s.sale_date, '%m-%d') BETWEEN '06-01'
                                                AND '06-30'            THEN 'Mid-Year Sale'
            ELSE 'Normal'
        END                                         AS commerce_event,

        -- === DOANH THU & LỢI NHUẬN ===
        s.total_revenue,
        s.total_cogs,
        s.gross_profit,
        s.gross_margin_pct,

        -- === LƯU LƯỢNG WEB ===
        wt.total_sessions,
        wt.total_unique_visitors,
        wt.total_page_views,
        wt.avg_bounce_rate,
        wt.avg_session_duration_sec,
        wt.sessions_organic,
        wt.sessions_paid_search,
        wt.sessions_social,
        wt.sessions_direct,
        wt.sessions_email,
        wt.sessions_referral,

        -- === TỶ LỆ CHUYỂN ĐỔI ===
        CASE
            WHEN wt.total_sessions > 0
            THEN ROUND(s.total_revenue / wt.total_sessions, 2)
            ELSE NULL
        END                                         AS revenue_per_session,

        CASE
            WHEN wt.total_sessions > 0
            THEN ROUND(
                wt.sessions_paid_search::DECIMAL
                / NULLIF(wt.total_sessions, 0) * 100, 2)
            ELSE NULL
        END                                         AS paid_search_share_pct,

        -- === VẬN HÀNH GIAO HÀNG ===
        sh.total_shipments,
        sh.delivered_count,
        sh.delayed_count,
        sh.avg_delivery_days,
        sh.total_shipping_fee,
        CASE
            WHEN sh.total_shipments > 0
            THEN ROUND(sh.delayed_count::DECIMAL / sh.total_shipments * 100, 2)
            ELSE NULL
        END                                         AS delay_rate_pct,

        -- === TỒN KHO (snapshot gần nhất theo tháng) ===
        inv.total_products_tracked,
        inv.products_out_of_stock,
        inv.stockout_rate_pct,
        inv.total_stock_on_hand,
        inv.products_need_reorder,

        -- === CỜ CẢNH BÁO KẾT HỢP ===
        -- Marketing đang chạy nhưng hàng đã hết
        CASE
            WHEN COALESCE(inv.stockout_rate_pct, 0) > 20
             AND COALESCE(wt.total_sessions, 0) > 0
            THEN TRUE ELSE FALSE
        END                                         AS wasted_traffic_stockout,

        -- Ngày giao hàng trễ nghiêm trọng
        -- DuckDB: không thể reference alias delay_rate_pct trong cùng SELECT → tính lại inline
        CASE
            WHEN sh.total_shipments > 0
             AND ROUND(sh.delayed_count::DECIMAL / sh.total_shipments * 100, 2) > 30
            THEN TRUE ELSE FALSE
        END                                         AS high_delay_day

    FROM sales_daily        s
    LEFT JOIN web_traffic_daily  wt   ON s.sale_date     = wt.traffic_date
    LEFT JOIN shipments_daily    sh   ON s.sale_date     = sh.ship_date
    -- Inventory là monthly snapshot → join theo tháng đầu tháng
    -- DuckDB: DATE_TRUNC cú pháp giống PostgreSQL, hoạt động bình thường
    LEFT JOIN inventory_daily    inv  ON DATE_TRUNC('month', s.sale_date)
                                      = DATE_TRUNC('month', inv.snapshot_date)
)

SELECT * FROM final
ORDER BY sale_date