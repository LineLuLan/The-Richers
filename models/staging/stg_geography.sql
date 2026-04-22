-- stg_geography.sql

with source as (
    select * from {{ source('raw', 'geography') }}
),

renamed as (
    select
        cast(zip as varchar) as zip,
        cast(city as varchar) as city,
        cast(region as varchar) as region,
        cast(district as varchar) as district
    from source
)

select * from renamed