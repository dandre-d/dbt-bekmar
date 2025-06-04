{# -- script_name: stg_oitm_product.sql
-- description: "model requires cleaning and separation of case statement logic to either a function or mapping table" #}
{{ config(
     materialized = 'view'
) }}

SELECT
     oitm.itemcode,
     oitm.itemname,
     oitb.itmsgrpnam,
     CASE
          WHEN oitb.itmsgrpnam LIKE '%Pumps%' THEN 'Pumps'
          WHEN oitb.itmsgrpnam LIKE '%Wines%' THEN 'Wines'
          WHEN oitb.itmsgrpnam LIKE '%Fitting%'
          OR oitb.itmsgrpnam LIKE '%Plumbing%' THEN 'Fittings and Plumbing Supplies'
          WHEN oitb.itmsgrpnam LIKE '%Electrical%'
          OR oitb.itmsgrpnam IN (
               'Relay',
               'Controllers & Access',
               'Combiner Box',
               'Change Over/SPD'
          ) THEN 'Electrical Products'
          WHEN oitb.itmsgrpnam LIKE '%Tools%'
          OR oitb.itmsgrpnam LIKE '%Hardware%'
          OR oitb.itmsgrpnam LIKE '%Webco%' THEN 'Tools and Hardware'
          WHEN oitb.itmsgrpnam LIKE '%Clothing%' THEN 'Clothing and Safety Gear'
          WHEN oitb.itmsgrpnam LIKE '%Agricultural%' THEN 'Agricultural Products'
          WHEN oitb.itmsgrpnam LIKE '%Chemical%'
          OR oitb.itmsgrpnam LIKE '%Fertilizer%' THEN 'Chemicals and Fertilizers'
          WHEN oitb.itmsgrpnam LIKE '%Spirits%' THEN 'Miscellaneous Beverages'
          WHEN oitb.itmsgrpnam LIKE '%Special%' THEN 'Specialty Products'
          WHEN oitb.itmsgrpnam LIKE '%Furniture%'
          OR oitb.itmsgrpnam LIKE '%Outdoor%' THEN 'Furniture and Outdoor Supplies'
          WHEN oitb.itmsgrpnam LIKE '%Irrigation%'
          OR oitb.itmsgrpnam LIKE '%Dripline%'
          OR oitb.itmsgrpnam LIKE '%Sprinkler%'
          OR oitb.itmsgrpnam LIKE '%Hunter%'
          OR oitb.itmsgrpnam LIKE '%Netafim%' THEN 'Irrigation and Agricultural Products'
          WHEN oitb.itmsgrpnam LIKE '%Pipe%'
          OR oitb.itmsgrpnam LIKE '%Fitting%'
          OR oitb.itmsgrpnam LIKE '%Valve%'
          OR oitb.itmsgrpnam LIKE '%HDPE%'
          OR oitb.itmsgrpnam LIKE '%Hoses%'
          OR oitb.itmsgrpnam LIKE '%Plasson%'
          OR oitb.itmsgrpnam LIKE '%Repair%' THEN 'Pipes, Fittings, and Valves'
          WHEN oitb.itmsgrpnam LIKE '%Cleaning%'
          OR oitb.itmsgrpnam LIKE '%Supplies%'
          OR oitb.itmsgrpnam LIKE '%Health%'
          OR oitb.itmsgrpnam LIKE '%Accessories%' THEN 'Cleaning and Health Products'
          WHEN oitb.itmsgrpnam LIKE '%Energy%'
          OR oitb.itmsgrpnam LIKE '%Fruit%'
          OR oitb.itmsgrpnam LIKE '%Drinks%'
          OR oitb.itmsgrpnam LIKE '%Beer%'
          OR oitb.itmsgrpnam LIKE '%Ciders%' THEN 'Beverages'
          WHEN oitb.itmsgrpnam LIKE '%Accessories%'
          OR oitb.itmsgrpnam LIKE '%Gifts%'
          OR oitb.itmsgrpnam LIKE '%Braai%' THEN 'Accessories and Gifts'
          WHEN oitb.itmsgrpnam LIKE '%Irrigator%'
          OR oitb.itmsgrpnam LIKE '%Rain%'
          OR oitb.itmsgrpnam LIKE '%Drip%'
          OR oitb.itmsgrpnam LIKE '%Fertigation%' THEN 'Irrigation and Agricultural Products'
          WHEN oitb.itmsgrpnam LIKE '%Steel%'
          OR oitb.itmsgrpnam LIKE '%Galv%'
          OR oitb.itmsgrpnam LIKE '%Forged%'
          OR oitb.itmsgrpnam LIKE '%Malleable%'
          OR oitb.itmsgrpnam LIKE '%Wght%' THEN 'Steel Products'
          WHEN oitb.itmsgrpnam LIKE '%Water%'
          OR oitb.itmsgrpnam LIKE '%Purifier%'
          OR oitb.itmsgrpnam LIKE '%Meter%'
          OR oitb.itmsgrpnam LIKE '%Hydrant%'
          OR oitb.itmsgrpnam LIKE '%Sparkling%' THEN 'Water and Filtration Products'
          WHEN oitb.itmsgrpnam LIKE '%Batteries%'
          OR oitb.itmsgrpnam LIKE '%Inverter%'
          OR oitb.itmsgrpnam LIKE '%Solar%'
          OR oitb.itmsgrpnam LIKE '%MPPT%' THEN 'Energy Products'
          WHEN oitb.itmsgrpnam LIKE '%Cigar%'
          OR oitb.itmsgrpnam LIKE '%Liqueurs%'
          OR oitb.itmsgrpnam LIKE '%Bar%'
          OR oitb.itmsgrpnam LIKE '%Mixology%' THEN 'Beverages and Consumables'
          WHEN oitb.itmsgrpnam LIKE '%ZZ%'
          OR oitb.itmsgrpnam LIKE '%Inactive%' THEN 'Inactive Products'
          WHEN oitb.itmsgrpnam LIKE '%QC%' THEN 'Quality Control'
          WHEN oitb.itmsgrpnam IN (
               'Agriplas',
               'Webco Products',
               'Consumables'
          ) THEN 'Products'
          WHEN oitb.itmsgrpnam IN (
               'Assemblies Clusters',
               'Services',
               'Manufacturing'
          ) THEN 'Services'
          WHEN oitb.itmsgrpnam IN (
               'Vermont',
               'VSD',
               'Lights',
               'Filtration',
               'Pool Products',
               'Tanks'
          ) THEN 'Lighting & Filtration'
          WHEN oitb.itmsgrpnam IN (
               'Gyro SA',
               'Bolts, Nuts & Screws',
               'Cables',
               'Breakers',
               'Bolts Nuts & Gasket',
               'Pivots',
               'Viking Micro',
               'Lugs / Ferrules',
               'Glands',
               'Clamps & Other',
               'Brackets',
               'Coolers'
          ) THEN 'Hardware & Accessories'
          WHEN oitb.itmsgrpnam IN (
               'Ice',
               'INSECTS',
               'RENTAL INCOME'
          ) THEN 'Miscellaneous'
          ELSE 'Other'
     END AS product_category,
     CASE
          WHEN oitm.sellitem = 'Y' THEN 'Sales'
          WHEN oitm.invntitem = 'Y' THEN 'Inventory'
          ELSE 'Other'
     END AS product_type,
     CASE
          WHEN len(
               oitm.invntryuom
          ) > 0 THEN oitm.invntryuom
     END AS unit,
     oitm.lastpurprc unit_price,
     oitm.source_db,
     CASE
          WHEN oitm.validFor = 'Y' THEN 1
          ELSE 0
     END AS is_active
     , sysdatetime() AS insert_date
FROM
     {{ ref('oitm_items') }}
     oitm
     LEFT JOIN {{ ref('oitb_itemgroup') }}
     oitb
     ON oitm.itmsgrpcod = oitb.itmsgrpcod
     AND oitm.source_db = oitb.source_db
