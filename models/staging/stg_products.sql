-- stg_products.sql

with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        cast(product_id as bigint) as product_id,
        cast(product_name as varchar) as product_name,
        cast(category as varchar) as category,
        cast(segment as varchar) as segment,
        cast(size as varchar) as size,
        cast(color as varchar) as color,
        cast(price as double) as price,
        cast(cogs as double) as cogs
    from source
)

select * from renamed