-- Script Name: dim_customer
-- Author     : Dadie | 2025-05-27
-- Description:

{{ config(materialized='table') }}

with customer as (

select
    {{ sqlserver_surrogate_key(['source_db']) }}  AS company_key,
    cc.customer_code,
    cc.customer_name,
    cc.customer_type,
    cc.customer_contact,
    cc.is_active,
    cc.insert_date
from {{ ref('stg_ocrd_customer') }} cc
)
select
    {{ sqlserver_surrogate_key(['company_key', 'customer_code']) }} AS customer_key,
    company_key,
    customer_code COLLATE SQL_Latin1_General_CP1_CI_AS as customer_code,
    customer_name,
    customer_type,
    customer_contact,
    is_active,
    insert_date
from customer