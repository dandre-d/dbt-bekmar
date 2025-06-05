-- models/dwh/dim_location.sql

{{ config(
    materialized = 'table'
) }}

select 
    location_key,
    location_name,
    city,
    province,
    country,
    latitude,
    longitude,
    current_timestamp as insert_date,
    current_timestamp as update_date
from {{ ref('stg_seed_locations') }}
