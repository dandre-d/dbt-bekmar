 {{ config(materialized='view') }}

select *
from ({{ union_sap_tables('RIN1') }}) RIN1
