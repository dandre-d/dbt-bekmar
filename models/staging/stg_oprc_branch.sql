-- Script Name: models\staging\stg_oprc_branch.sql
-- Author     : Dadie | 2025-05-27
-- Description:
with branch as (
    SELECT DISTINCT 
        company_key,
        PrcCode AS branch_code,
        PrcName AS branch_name,
        CASE 
            WHEN Locked = 'Y' THEN 0 
            ELSE 1 
        END AS is_active,
        CASE 	WHEN PrcCode in ('HO', 'HS', 'LE', 'LT', 'LV', 'NS', 'TZ')  THEN 'Retail'
				WHEN PrcCode in ('DC', 'FL', 'PJ', 'WS') THEN 'Projects'
				WHEN PrcCode like 'Centr_z%' THEN 'Default'
				ELSE 'Other'
        END AS branch_group,
        sysdatetime() AS insert_date
    FROM {{ ref('oprc_costcentre_raw') }} cc
    inner join {{ ref('dim_company') }} as dc
    on  dc.company_db = cc.source_db


)
select 
    {{ sqlserver_surrogate_key(['company_key', 'branch_code']) }} AS branch_key,
    company_key,
    branch_code,
    branch_name,
    branch_group,
    is_active,
    insert_date
from branch
