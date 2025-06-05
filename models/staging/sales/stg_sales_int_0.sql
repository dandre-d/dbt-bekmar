 -- This intermediate model consolidates invoice, credit note, and discount data
-- into a unified sales staging set for fact generation.
-- models\staging\sales\stg_sales_int_0.sql
{{ config(materialized='view') }}

with unioned_sales as (
   select 
    order_num,
    order_line_num,
    ocr_code,
    customer_code,
    whs_code,
    slp_code,
    source_db,
    quantity,
    unit_price,
    discount,
    total_amount,
    item_code,
    profit,
    tax_amount,
    amount_inc_tax,
    doc_date,
    doc_due_date,
    invoice_type,
    is_active
 from {{ ref('stg_oinv_invoice') }} inv
 
 union all

  select 
    order_num,
    order_line_num,
    ocr_code,
    customer_code,
    whs_code,
    slp_code,
    source_db,
    quantity,
    unit_price,
    discount,
    total_amount,
    item_code,
    profit,
    tax_amount,
    amount_inc_tax,
    doc_date,
    doc_due_date,
    invoice_type,
    is_active
 from {{ ref('stg_orin_creditnotes') }}

 union all

  select 
    order_num,
    order_line_num,
    ocr_code,
    customer_code,
    whs_code,
    slp_code,
    source_db,
    quantity,
    unit_price,
    discount,
    total_amount,
    item_code,
    profit,
    tax_amount,
    amount_inc_tax,
    doc_date,
    doc_due_date,
    invoice_type,
    is_active
 from {{ ref('stg_oinv_discounts') }}
)
select 
    order_num  as order_num,
    order_line_num  as order_line_num,
    cast(case
        when len(trim(isnull(ocr_code, ''))) < 1
        then 'No Branch'
        else ocr_code
    end as char(25))    as ocr_code ,
    cast(isnull(customer_code,'-1')  as char(25))  as customer_code ,
    cast(isnull(whs_code, '-1') as char(25))  as whs_code  ,
    cast(isnull(item_code,'-1') as nvarchar(50) )  as item_code,
    sales.slp_code,
    d_company.company_key,
    sales.quantity,
    sales.unit_price,
    sales.discount,
    sales.total_amount,
    sales.profit,
    sales.tax_amount,
    sales.amount_inc_tax,
    sales.doc_date,
    sales.doc_due_date,
    sales.invoice_type,
    sales.is_active
from unioned_sales as sales
inner join {{ ref("dim_company") }} d_company on sales.source_db COLLATE SQL_Latin1_General_CP850_CI_AS = d_company.company_db