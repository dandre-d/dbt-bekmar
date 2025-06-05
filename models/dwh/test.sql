select *
from {{ ref('stg_sales_int_1') }} fact_sales
