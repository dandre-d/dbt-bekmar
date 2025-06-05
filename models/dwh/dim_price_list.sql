{{ config(
    materialized='incremental',
    unique_key=['pl_key']
) }}

with source as (
    select * from {{ ref('stg_opln_price_list') }}
)

select
    s.pl_key,
    s.pl_code,
    s.price_list_name,
    s.base_num,
    s.factor,
    s.is_active,
    s.company_key,
    s.insert_date,
    sysdatetime() as update_date
from source s

{% if is_incremental() %}
where (
    pl_key
) not in (
    select pl_key from {{ this }}
)
or exists (
    select 1
    from {{ this }} t
    where t.pl_key = s.pl_key
      and (
          t.price_list_name <> s.price_list_name or
          t.base_num <> s.base_num or
          t.factor <> s.factor or
          t.is_active <> s.is_active
      )
)
{% endif %}
