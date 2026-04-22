-- stg_shipments.sql

with source as (
    select * from {{ source('raw', 'shipments') }}
),

renamed as (
    select
        cast(order_id as integer) as order_id,
        cast(ship_date as date) as ship_date,
        cast(delivery_date as date) as delivery_date,
        cast(shipping_fee as double) as shipping_fee
    from source
)

select * from renamed