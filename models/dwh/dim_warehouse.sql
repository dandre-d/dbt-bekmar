{{ config(
    materialized='incremental',
    unique_key=['warehouse_key']
) }}

with staged as (
    select * 
    from {{ ref('stg_owhs_warehouse') }}
),

final as (
    select 
        warehouse_key,
        warehouse_code COLLATE SQL_Latin1_General_CP1_CI_AS as warehouse_code,
        warehouse_name COLLATE SQL_Latin1_General_CP1_CI_AS as warehouse_name,
        company_key,
        is_active,
        insert_date,
        current_timestamp as update_date
    from staged
)

select * from final

{% if is_incremental() %}
-- Wrap the incremental filter with a `where` clause applied to the final select
where (
    warehouse_key
) not in (
    select warehouse_key
    from {{ this }}
)
or exists (
    select 1
    from {{ this }} as target
    where target.warehouse_key = final.warehouse_key
      and (
        target.warehouse_name <> final.warehouse_name
        or target.is_active <> final.is_active
      )
)
{% endif %}
