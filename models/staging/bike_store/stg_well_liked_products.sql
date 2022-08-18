{{ config(
    materialized='view'
    ) }}
select
    PRODUCT_ID,
    REPLACE(REPLACE(LEFT(B.PRODUCT_NAME, 
    CHARINDEX('-', B.PRODUCT_NAME) - 1), '''', ' '), '(', '') 
    AS PRODUCT_NAME,
    LIST_PRICE,
    CATEGORY_ID
from
{{ source('production', 'products') }} B
