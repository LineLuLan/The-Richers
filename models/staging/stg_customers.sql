with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        cast(customer_id as integer) as customer_id,

        trim(cast(zip as varchar)) as zip,
        lower(trim(cast(city as varchar))) as city,

        cast(signup_date as date) as signup_date,

        lower(trim(cast(gender as varchar))) as gender,
        lower(trim(cast(age_group as varchar))) as age_group,
        lower(trim(cast(acquisition_channel as varchar))) as acquisition_channel

    from source
),

cleaned as (
    select *
    from renamed
    where customer_id is not null
)

select
    customer_id,
    zip,
    city,
    signup_date,
    gender,
    age_group,
    acquisition_channel
from cleaned