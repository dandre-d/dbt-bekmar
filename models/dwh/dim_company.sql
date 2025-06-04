-- Script Name: dim_company
-- Author     : Dadie | 2025-05-27
-- Description:

{{ config(materialized='table') }}

SELECT 
     {{ sqlserver_surrogate_key(['company_code', 'company_db']) }}  AS company_key,
    company_code,
    company_db,
    company_name,
    is_active,
    SYSDATETIME() AS insert_date
FROM {{ ref('stg_company') }} AS sc
