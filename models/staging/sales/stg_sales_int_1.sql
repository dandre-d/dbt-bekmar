
with dates as (
    select full_date,date_key
    from {{ ref("dim_date") }} 
) 
, final as (
  select
    order_num
	, order_line_num -- Invoice line number
	, {{ sqlserver_surrogate_key(['company_key' , 'ocr_code']) }} branch_key
	, {{ sqlserver_surrogate_key(['company_key' , 'customer_code']) }} customer_key
	, {{ sqlserver_surrogate_key(['company_key' , 'whs_code']) }} as warehouse_key
	, {{ sqlserver_surrogate_key(['company_key' , 'slp_code']) }} sp_key
    , {{ sqlserver_surrogate_key(['company_key' , 'item_code']) }} product_key
	, inv.company_key 
    , due_date.date_key AS due_date_key -- Due date
    , inv_date.date_key AS invoice_date_key -- Invoice date
	, inv.quantity -- Quantity sold
	, inv.unit_price -- Unit price from invoice
	, inv.discount -- Discount applied
	, inv.total_amount -- Line total (Amount)
	, inv.profit
	, inv.tax_amount
	, inv.amount_inc_tax
	, invoice_type
	, inv.is_active
	, SYSDATETIME() AS insert_date -- Insert date
from {{ ref("stg_sales_int_0") }} inv
left join dates inv_date on inv.doc_date = inv_date.full_date
left join dates due_date on inv.doc_due_date = due_date.full_date
where inv.doc_date >= dateadd(year, -2, getdate() ) -- Filter for the last year
)
select 
    {{ sqlserver_surrogate_key(['order_num', 'order_line_num', 'customer_key']) }} AS sales_key,
    order_num,
    order_line_num,
    branch_key,
    customer_key,  
    product_key,
    warehouse_key,
    sp_key,
    company_key,
    invoice_date_key,
    due_date_key,
    cast( quantity as decimal(18,4) ) as quantity ,
    cast( unit_price as decimal(18,4) ) as unit_price ,
    cast( discount as decimal(18,4) ) as discount ,
    cast( total_amount as decimal(18,4) ) as total_amount ,
    cast( tax_amount as decimal(18,4) ) as tax_amount ,
    cast( amount_inc_tax as decimal(18,4) ) as amount_inc_tax ,
    cast( profit as decimal(18,4) ) as profit ,
    invoice_type,
    is_active,
    insert_date
from final f
