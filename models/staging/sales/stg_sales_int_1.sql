
with final as (
  select
    order_num
	, order_line_num -- Invoice line number
	, d_branch.branch_key
	, d_customer.customer_key -- Customer key from dim_customer
	, d_warehouse.warehouse_key -- Warehouse key from dim_warehouse
	, d_salesperson.sp_key -- Sales person key from dim_sales_person
	, inv.company_key -- Company key from dim_company
    , d_product.product_key
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
left join {{ ref("dim_branch") }} d_branch
    on inv.ocr_code = d_branch.branch_code
    and d_branch.company_key = inv.company_key
left join {{ ref("dim_customer") }} d_customer
    on inv.customer_code = d_customer.customer_code COLLATE SQL_Latin1_General_CP1_CI_AS
    and d_customer.company_key = inv.company_key
left join {{ ref("dim_warehouse") }} d_warehouse
    on inv.whs_code = d_warehouse.warehouse_code COLLATE SQL_Latin1_General_CP1_CI_AS
    and isnull(d_warehouse.company_key, inv.company_key) = inv.company_key 
left join {{ ref("dim_sales_person") }}
    d_salesperson
    on inv.slp_code = d_salesperson.sp_code
    and d_salesperson.company_key = inv.company_key
left join {{ ref("dim_product") }}
    d_product  -- product dimension
    on inv.item_code = d_product.product_code  -- MAP item code TO dim_product
    and d_product.company_key = inv.company_key
left join {{ ref("dim_date") }} inv_date on inv.doc_date = inv_date.full_date
left join {{ ref("dim_date") }} due_date on inv.doc_due_date = due_date.full_date

)
select 
    {{ sqlserver_surrogate_key(['order_num', 'order_line_num', 'company_key']) }} AS sales_key,
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
