{{ config(materialized='view') }}

select *
from ({{ union_sap_tables('ORDR') }}) ORDR
