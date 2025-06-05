with inv as (
    select * from {{ ref('oinv_ar_invoice') }}
),
inv1 as (
    select * from {{ ref('inv1_ar_invoiceline') }}
)

select
    inv.DocNum as order_num,
    inv1.LineNum as order_line_num,
    inv1.OcrCode COLLATE SQL_Latin1_General_CP1_CI_AS as ocr_code,
    inv.CardCode COLLATE SQL_Latin1_General_CP1_CI_AS  as customer_code,
    inv1.WhsCode COLLATE SQL_Latin1_General_CP1_CI_AS  as whs_code,
    inv1.ItemCode COLLATE SQL_Latin1_General_CP1_CI_AS  as item_code,
    inv.SlpCode as slp_code,
    inv.source_db,
    inv1.Quantity as quantity,
    inv1.Price as unit_price,
    inv1.DiscPrcnt as discount,
    inv1.LineTotal as total_amount,
    inv1.GrssProfit as profit,
    inv1.LineVat as tax_amount,
    inv1.PriceAfVAT as amount_inc_tax,
    inv.DocDate as doc_date,
    inv.DocDueDate as doc_due_date,
    1 as invoice_type,
    case when inv.CANCELED <> 'N' then 0 else 1 end as is_active
from inv
join inv1 on inv.DocEntry = inv1.DocEntry
     and inv.source_db = inv1.source_db
