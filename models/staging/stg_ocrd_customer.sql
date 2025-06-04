-- Script Name: models\staging\stg_ocrd_customer.sql
-- Author     : Dadie | 2025-05-27
-- Description:

SELECT 
        CardCode AS customer_code,
        CardName AS customer_name,
        CardType AS customer_type,
        CntctPrsn AS customer_contact,
        source_db AS source_db,
        1 AS is_active,
        SYSDATETIME() AS insert_date
    FROM {{ ref('ocrd_businesspartner') }} AS OB