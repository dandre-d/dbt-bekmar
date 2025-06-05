-- models/staging/source/stg_source__oslp_sales_person.sql

with source as (
    select *
    from {{ ref('oslp_salesperson') }}
),

company as (
    select *
    from {{ ref('dim_company') }}
),

transformed as (
    select 

        s."SlpCode" as sp_code,
        {{ sqlserver_proper_case_with_exceptions ('SlpName') }} as sales_person,
        s."Commission" as commission,
        case 
            when s."Active" = 'Y' then 1 
            else 0 
        end as is_active,
        c.company_key,
        current_timestamp as insert_date
    from source s
    inner join company c
        on c.company_db = s.source_db
)

select {{ sqlserver_surrogate_key(['sp_code', 'company_key']) }}  AS sp_key
        , sp_code
        , sales_person
        , commission
        , is_active
        , company_key
        , insert_date
    from transformed
