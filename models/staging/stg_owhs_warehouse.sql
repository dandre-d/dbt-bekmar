with source as (
    select *
    from {{ ref('owhs_warehouses') }}
),

company as (
    select *
    from {{ ref('dim_company') }}
),

transformed as (
    select 
        c.company_key,
        s."WHsCode" as warehouse_code,
        s."WhsName" as warehouse_name,
        case 
            when s."InActive" = 'Y' then 0 
            else 1 
        end as is_active,
        current_timestamp as insert_date
    from source s
    left join company c 
        on c.company_db = s.source_db
)

select
    {{ sqlserver_surrogate_key(['company_key' , 'warehouse_code']) }} as warehouse_key
    , company_key
    , warehouse_code
    , warehouse_name
    , is_active
    , insert_date
 from transformed