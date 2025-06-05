Welcome to Bekmar dbt project!

for raw models, use the name of the source table seperted by the function name
for example:
```sql
select * from {{ source('raw', 'opc_tablefunction') }}
```
for stagin, seperate name with stg_ source table name, and bekamr table name 
for example:
```sql
select * from {{ ref('stg_opc_tablefunctionbekmar') }}
```

With dimentions and fact in the dwh, please indicate with a prefix of dim_ or fact_ and the table name
for example:
```sql
select * from {{ ref('dim_tablefunctionbekmar') }}
```

TO update to the below:
stg_<source>__<source_table>[_<business_area>]
dim_<business_object>[_<domain>]
fact_<business_event>[_<domain>]
