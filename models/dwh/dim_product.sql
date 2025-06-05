-- Script Name: dim_product
-- Author     : Dadie | 2025-05-27
-- Description:

{{ config(materialized='table') }}

with product as (
select
      , {{ sqlserver_surrogate_key(['source_db']) }}  AS company_key
			, cc.product_code COLLATE SQL_Latin1_General_CP1_CI_AS as product_code
			, cc.product_name
			, cc.product_group
			, cc.product_category
			, cc.product_type
			, cc.unit
			, cc.unit_price
			, cc.is_active
			, cc.insert_date
from {{ ref('stg_oitm_product') }} cc
)
select
		{{ sqlserver_surrogate_key(['company_key', 'product_code']) }} AS product_key,
		company_key,
		product_code,
		product_name,
		product_group,
		product_category,
		product_type,
		unit,
		unit_price,
		is_active,
		insert_date
		from product