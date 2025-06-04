-- Script Name: models\staging\stg_oprc_branch.sql
-- Author     : Dadie | 2025-05-27
-- Description:
SELECT *
FROM (
    SELECT DISTINCT 
        source_db AS source_db,
        PrcCode AS branch_code,
        PrcName AS branch_name,
        CASE 
            WHEN Locked = 'Y' THEN 0 
            ELSE 1 
        END AS is_active,
        SYSDATETIME() AS insert_date,
        CASE 	WHEN PrcCode in ('HO', 'HS', 'LE', 'LT', 'LV', 'NS', 'TZ')  THEN 'Retail'
				WHEN PrcCode in ('DC', 'FL', 'PJ', 'WS') THEN 'Projects'
				WHEN PrcCode like 'Centr_z%' THEN 'Default'
				ELSE 'Other'
        END AS branch_group
    FROM {{ ref('oprc_costcentre_raw') }} cc


) AS cost_centre
