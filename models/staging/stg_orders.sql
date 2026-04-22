-- stg_orders.sql

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        cast(order_id as bigint)                        as order_id,
        cast(order_date as date)                        as order_date,
        cast(customer_id as bigint)                     as customer_id,
        cast(zip as varchar)                            as zip,
        cast(order_status as varchar)                   as order_status,
        cast(payment_method as varchar)                 as payment_method,
        cast(device_type as varchar)                    as device_type,
        cast(order_source as varchar)                   as order_source
    from source
)

select * from renamed