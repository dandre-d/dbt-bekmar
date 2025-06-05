-- Script Name: dim_branch
-- Author     : Dadie | 2025-05-27
-- Description:
{{ config(materialized='table') }}

with branch as ()
select
    {{ sqlserver_surrogate_key(['source_db']) }}  AS company_key,
    cc.branch_code COLLATE SQL_Latin1_General_CP1_CI_AS as branch_code,
    cc.branch_name,
    cc.branch_group,
    cc.is_active,
    cc.insert_date
from {{ ref('stg_oprc_branch') }} cc
)
select
    {{ sqlserver_surrogate_key(['company_key', 'branch_code']) }} AS branch_key,
    company_key,
    branch_code,
    branch_name,
    branch_group,
    is_active,
    insert_date
from branch