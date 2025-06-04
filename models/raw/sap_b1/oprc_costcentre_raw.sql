-- script name: oprc_costcentre_raw.sql
{{ config(materialized='view') }}

select *
from ({{ union_sap_tables('OPRC') }}) OPRC