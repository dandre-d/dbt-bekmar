
with raw as (
    select *
    from {{ ref('locations') }}
),

renamed as (
    select
        cast(location_key as integer) as location_key,
        location_name,
        city,
        province,
        country,
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude
    from raw
)

select * from renamed