{# -- script_name: stg_oitm_product.sql
-- description: "model requires cleaning and separation of case statement logic to either a function or mapping table" #}
{{ config(materialized='view') }}

	SELECT
			  ItemCode as product_code      
			, ItemName as product_name
			, ItmsGrpNam as product_group
			, product_category as product_category
			, source_db as source_db
			, product_type as product_type
			, unit as unit
			, unit_price as unit_price
			, is_active as is_active
			, insert_date as insert_date
	FROM {{ ref('stg_oitm_product_int_0') }} 
