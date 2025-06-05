{{ config(
    materialized='incremental',
    unique_key='sales_key'
) }}

with source as (
    select * from {{ ref('stg_sales_int_1') }}
),

filtered as (
    select
        s.sales_key,
        s.order_num,
        s.order_line_num,
        s.branch_key,
        s.customer_key,
        s.warehouse_key,
        s.product_key,
        s.sp_key,
        s.invoice_date_key,
        s.due_date_key,
        s.quantity,
        s.unit_price,
        s.discount,
        s.total_amount,
        s.tax_amount,
        s.amount_inc_tax,
        s.profit,
        s.invoice_type,
        s.is_active
    from source s

    {% if is_incremental() %}
    where
        s.sales_key not in (select sales_key from {{ this }})
        or exists (
            select 1
            from {{ this }} t
            where t.sales_key = s.sales_key
              and (
                  t.order_num       <> s.order_num or
                  t.order_line_num  <> s.order_line_num or
                  t.branch_key      <> s.branch_key or
                  t.customer_key    <> s.customer_key or
                  t.warehouse_key   <> s.warehouse_key or
                  t.product_key     <> s.product_key or
                  t.sp_key          <> s.sp_key or
                  t.invoice_date_key<> s.invoice_date_key or
                  t.due_date_key    <> s.due_date_key or
                  t.quantity        <> s.quantity or
                  t.unit_price      <> s.unit_price or
                  t.discount        <> s.discount or
                  t.total_amount    <> s.total_amount or
                  t.tax_amount      <> s.tax_amount or
                  t.amount_inc_tax  <> s.amount_inc_tax or
                  t.profit          <> s.profit or
                  t.invoice_type    <> s.invoice_type or
                  t.is_active       <> s.is_active
              )
        )
    {% endif %}
)

select * from filtered
