-- Script Name: dim_branch
-- Author     : Dadie | 2025-05-27
-- Description:
{{ config(materialized='table') }}

select
  {{ sqlserver_surrogate_key(['company_key', 'branch_code']) }}  AS branch_key,
    dc.company_key,
    cc.branch_code,
    cc.branch_name,
    cc.branch_group,
    cc.is_active,
    cc.insert_date
from {{ ref('stg_oprc_branch') }} cc
inner join {{ ref('dim_company') }} as dc
   on  dc.company_db = cc.source_db
