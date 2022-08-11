{{ config(
    materialized='view'
    ) }}
select
    DISTINCT
    REPLACE( B.PRODUCT_ID, '''', '')  AS PRODUCT_ID,
    B.PRODUCT_NAME,
    B.LIST_PRICE,
    REPLACE(BRAND_NAME, '''', '') AS BRAND_NAME
FROM 
{{ source('production', 'categories') }} A
LEFT JOIN
{{ source ('dbt_development','stg_well_liked_products') }} B
ON A.CATEGORY_ID = B.CATEGORY_ID
LEFT JOIN
{{ source('production', 'brands') }}