with source as (
    select *
    from {{ ref('owhs_warehouses') }}
),

transformed as (
    select 
        {{ sqlserver_proper_case_with_exceptions ('source_db') }} as company_key,
        s."WHsCode" as warehouse_code,
        s."WhsName" as warehouse_name,
        case 
            when s."InActive" = 'Y' then 0 
            else 1 
        end as is_active,
        current_timestamp as insert_date
    from source s
)

select
    {{ sqlserver_surrogate_key(['company_key' , 'warehouse_code']) }} as warehouse_key
    , company_key
    , warehouse_code
    , warehouse_name
    , is_active
    , insert_date
 from transformed