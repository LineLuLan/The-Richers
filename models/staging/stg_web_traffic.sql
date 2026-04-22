-- stg_web_traffic.sql

with source as (
    select * from {{ source('raw', 'web_traffic') }}
),

renamed as (
    select
        cast(date as date) as date,
        cast(sessions as integer) as sessions,
        cast(unique_visitors as integer) as unique_visitors,
        cast(page_views as integer) as page_views,
        cast(bounce_rate as double) as bounce_rate,
        cast(avg_session_duration_sec as double) as avg_session_duration_sec,
        cast(traffic_source as varchar) as traffic_source
    from source
)

select * from renamed