-- int_web_traffic_daily.sql
-- Mục đích: Tổng hợp lưu lượng web theo ngày
-- Pivot traffic_source thành các cột riêng để phân tích kênh
--
-- Schema thực tế stg_web_traffic:
--   date, sessions, unique_visitors, page_views,
--   bounce_rate, avg_session_duration_sec, traffic_source

WITH daily_traffic AS (
    SELECT
        date                                                            AS traffic_date,
        SUM(sessions)                                                   AS total_sessions,
        SUM(unique_visitors)                                            AS total_unique_visitors,
        SUM(page_views)                                                 AS total_page_views,
        AVG(bounce_rate)                                                AS avg_bounce_rate,
        AVG(avg_session_duration_sec)                                   AS avg_session_duration_sec,
        -- Breakdown theo traffic_source (pivot)
        SUM(sessions) FILTER (WHERE traffic_source = 'organic_search')  AS sessions_organic,
        SUM(sessions) FILTER (WHERE traffic_source = 'paid_search')     AS sessions_paid_search,
        SUM(sessions) FILTER (WHERE traffic_source = 'social_media')    AS sessions_social,
        SUM(sessions) FILTER (WHERE traffic_source = 'direct')          AS sessions_direct,
        SUM(sessions) FILTER (WHERE traffic_source = 'email_campaign')           AS sessions_email,
        SUM(sessions) FILTER (WHERE traffic_source = 'referral')        AS sessions_referral
    FROM {{ ref('stg_web_traffic') }}
    GROUP BY date
)

SELECT * FROM daily_traffic