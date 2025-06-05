 {{ config(materialized='view') }}

select *
from ({{ union_sap_tables('INV1') }}) INV1
