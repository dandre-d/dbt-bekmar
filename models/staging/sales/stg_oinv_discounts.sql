with inv as (
    select * from {{ ref('oinv_ar_invoice') }}
),
inv1 as (
    select * from {{ ref('inv1_ar_invoiceline') }}
)

	SELECT inv.DocNum AS order_num
		, inv1.[LineNum] AS order_line_num -- Invoice line number
		, inv1.[OcrCode] COLLATE SQL_Latin1_General_CP1_CI_AS as ocr_code
		, inv.[CardCode]  COLLATE SQL_Latin1_General_CP1_CI_AS as customer_code -- Customer key from dim_customer
		, '-1' COLLATE SQL_Latin1_General_CP1_CI_AS as whs_code-- Warehouse key from dim_warehouse
		, inv.[SlpCode]  as slp_code -- Sales person key from dim_sales_person
		, inv.source_db
		, 1 as quantity-- Quantity sold
		, 0 AS unit_price -- Unit price from invoice
		, inv.DiscSumSy AS discount -- Discount applied
		, - 1 * inv.DiscSumSy AS total_amount -- Line total (Amount)
		, '-1' COLLATE SQL_Latin1_General_CP1_CI_AS as item_code
		, 0 AS profit
		, 0 AS [tax_amount]
		, 0 AS [amount_inc_tax]
		, inv.[DocDate] AS doc_date -- Invoice date
		, inv.[DocDueDate] AS doc_due_date -- Due date
		, 3 invoice_type
		, IIF((inv.CANCELED <> 'N' /* OR inv1.LineStatus <> 'O'*/), 0, 1) [is_active]
	FROM inv  -- Invoice Header
	OUTER APPLY (
		SELECT TOP 1 inv1.[DocEntry]
			, inv1.source_db
			, inv1.[OcrCode]
			, inv1.[LineNum] + 1 [LineNum]
		FROM  inv1
		WHERE inv.[DocEntry] = inv1.[DocEntry]
			AND inv.source_db = inv1.source_db
		ORDER BY inv1.[LineNum] DESC
		) inv1
	WHERE inv.DiscSumSy > 0
