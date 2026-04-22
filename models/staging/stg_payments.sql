-- stg_payments.sql

with source as (
    select * from {{ source('raw', 'payments') }}
),

renamed as (
    select
        cast(order_id as bigint)                        as order_id,
        cast(payment_method as varchar)                 as payment_method,
        cast(payment_value as decimal(16,2))            as payment_value,
        cast(installments as bigint)                    as installments
    from source
)

select * from renamed