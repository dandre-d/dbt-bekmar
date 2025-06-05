{{ config(
    materialized='incremental',
    unique_key=['sp_key']
) }}

with staged as (
    select * 
    from {{ ref('stg_oslp_sales_person') }}
),

final as (

    {% if is_incremental() %}

    select
        s.sp_key,
        s.sp_code,
        s.sales_person,
        s.commission,
        s.is_active,
        s.company_key,
        s.insert_date,
        current_timestamp as update_date
    from staged s

    left join {{ this }} t
        on s.sp_key = t.sp_key

    where 
        t.sp_code is null -- new record
        or s.sales_person <> t.sales_person
        or s.commission <> t.commission
        or s.is_active <> t.is_active

    {% else %}

    select 
        sp_key,
        sp_code,
        sales_person,
        commission,
        is_active,
        company_key,
        insert_date,
        current_timestamp as update_date
    from staged

    {% endif %}
)

select * from final
