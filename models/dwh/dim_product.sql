-- Script Name: dim_product
-- Author     : Dadie | 2025-05-27
-- Description:

{{ config(materialized='table') }}

select
  {{ sqlserver_surrogate_key(['company_key', 'product_code']) }}  AS product_key
      , dc.company_key
			, cc.product_code
			, cc.product_name
			, cc.product_group
			, cc.product_category
			, cc.product_type
			, cc.unit
			, cc.unit_price
			, cc.is_active
			, cc.insert_date
from {{ ref('stg_oitm_product') }} cc
inner join {{ ref('dim_company') }} as dc
   on  dc.company_db = cc.source_db
