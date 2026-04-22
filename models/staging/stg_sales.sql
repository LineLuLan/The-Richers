-- stg_sales.sql

with source as (
    select * from {{ source('raw', 'sales') }}
),

renamed as (
    select
        cast(Date as date) as Date,
        cast(Revenue as double) as Revenue,
        cast(COGS as double) as COGS
    from source
)

select * from renamed