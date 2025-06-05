with price_list as (
    select * from {{ ref('opln_pricelist') }}
),

company as (
    select * from {{ ref('dim_company') }}
)
, final as (
select

    pl.ListNum as pl_code,
    pl.ListName as price_list_name,
    pl.BASE_NUM as base_num,
    pl.Factor as factor,
    1 as is_active,
    c.company_key,
    sysdatetime() as insert_date
from price_list pl
inner join company c
    on c.company_db = pl.source_db
) 
select {{ sqlserver_surrogate_key([ 'company_key' , 'pl_code']) }}  AS pl_key
    , pl_code
    , price_list_name
    , base_num
    , factor
    , is_active
    , company_key
    , insert_date

    from final