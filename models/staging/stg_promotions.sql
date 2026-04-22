-- stg_promotions.sql

with source as (
    select * from {{ source('raw', 'promotions') }}
),

renamed as (
    select
        cast(promo_id as varchar) as promo_id,
        cast(promo_name as varchar) as promo_name,
        cast(promo_type as varchar) as promo_type,
        cast(discount_value as double) as discount_value,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        cast(applicable_category as varchar) as applicable_category,
        cast(promo_channel as varchar) as promo_channel,
        cast(stackable_flag as boolean) as stackable_flag,
        cast(min_order_value as integer) as min_order_value
    from source
)

select * from renamed