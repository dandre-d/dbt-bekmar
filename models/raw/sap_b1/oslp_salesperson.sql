{{ config(materialized='view') }}

select *
from ({{ union_sap_tables('OSLP') }}) OSLP
