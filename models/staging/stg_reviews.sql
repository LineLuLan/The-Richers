-- stg_reviews.sql

with source as (
    select * from {{ source('raw', 'reviews') }}
),

renamed as (
    select
        cast(review_id as varchar) as review_id,
        cast(order_id as integer) as order_id,
        cast(product_id as integer) as product_id,
        cast(customer_id as integer) as customer_id,
        cast(review_date as date) as review_date,
        cast(rating as integer) as rating,
        cast(review_title as varchar) as review_title
    from source
)

select * from renamed