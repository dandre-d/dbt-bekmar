-- script_name: stg_company.sql
-- description: "Staging table for company information"
{{ config(materialized='view') }}

SELECT   DB_ID AS company_code
		, db_name AS company_db
		, {{ sqlserver_proper_case_with_exceptions ('db_friendly_name') }}  AS company_name
		, is_active AS is_active
FROM [control].[source_db_control]
WHERE db_friendly_name IS NOT NULL