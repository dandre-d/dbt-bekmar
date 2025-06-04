-- Script Name: dim_customer
-- Author     : Dadie | 2025-05-27
-- Description:

{{ config(materialized='table') }}

select
  {{ sqlserver_surrogate_key(['company_key', 'customer_code']) }}  AS customer_key,
    dc.company_key,
    cc.customer_code,
    cc.customer_name,
    cc.customer_type,
    cc.customer_contact,
    cc.is_active,
    cc.insert_date
from {{ ref('stg_ocrd_customer') }} cc
inner join {{ ref('dim_company') }} as dc
   on  dc.company_db = cc.source_db
