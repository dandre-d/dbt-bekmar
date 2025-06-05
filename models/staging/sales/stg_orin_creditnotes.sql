with rin as (
    select * from {{ ref('orin_cn_invoice') }}
),
rin1 as (
    select * from {{ ref('rin1_cn_invoiceline') }}
)

select
    rin.DocNum as order_num,
    rin1.LineNum as order_line_num,
    rin1.OcrCode COLLATE SQL_Latin1_General_CP1_CI_AS  as ocr_code,
    rin.CardCode COLLATE SQL_Latin1_General_CP1_CI_AS  as customer_code,
    rin1.WhsCode COLLATE SQL_Latin1_General_CP1_CI_AS  as whs_code,
    rin1.ItemCode COLLATE SQL_Latin1_General_CP1_CI_AS as item_code,
    rin.SlpCode as slp_code,
    rin.source_db,
    rin1.Quantity as quantity,
    rin1.Price as unit_price,
    rin1.DiscPrcnt as discount,
    rin1.LineTotal as total_amount,
    rin1.GrssProfit as profit,
    rin1.LineVat as tax_amount,
    rin1.PriceAfVAT as amount_inc_tax,
    rin.DocDate as doc_date,
    rin.DocDueDate as doc_due_date,
    2 as invoice_type,
    case when rin.CANCELED <> 'N' then 0 else 1 end as is_active
from rin
join rin1 on rin.DocEntry = rin1.DocEntry and rin.source_db = rin1.source_db
