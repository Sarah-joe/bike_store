{{ config(
    materialized='view'
    ) }}
select
    A.ORDER_ID,
    A.ITEM_ID,
    A.QUANTITY,
    A.DISCOUNT,
    A.CUSTOMER_ID,
    A.CUSTOMER_NAME,
    A.PHONE AS CUSTOMER_PHONE ,
    A.EMAIL,
    A.STREET,
    A.CITY,
    A.STATE,
    A.ZIP_CODE,
    A.STORE_NAME,
    A.STORE_PHONE,
    A.SHIPPED_DATE,
    B.PRODUCT_ID,
    REPLACE(B.PRODUCT_NAME, '''',' ') AS PRODUCT_NAME,
    B.LIST_PRICE AS PRODUCT_PRICE
from (select 
    D.*,
    A.CUSTOMER_ID,
    REPLACE(A.FIRST_NAME, '''',' ') || REPLACE(A.LAST_NAME, '''',' ') AS Customer_name,
    REPLACE(A.PHONE, '''',' ') AS PHONE ,
    REPLACE(A.EMAIL, '''',' ') AS EMAIL,
    REPLACE(A.STREET, '''',' ') AS STREET,
    REPLACE(A.CITY, '''',' ') AS CITY,
    SUBSTRING(A.STATE, 2,2) AS STATE,
    A.ZIP_CODE,
    REPLACE(C.STORE_NAME, '''',' ') AS STORE_NAME,
    REPLACE(C.PHONE, '''',' ')  AS STORE_PHONE,
    B.SHIPPED_DATE
from {{ source('sales', 'customers') }} A 
LEFT JOIN {{ ref('stg_orders')}} B
ON A.CUSTOMER_ID = B.CUSTOMER_ID
LEFT JOIN {{ source('sales', 'stores') }} C
ON B.STORE_ID = C.STORE_ID
LEFT JOIN {{ source('sales', 'order_items') }} D 
ON B.ORDER_ID = D.ORDER_ID) AS A
LEFT JOIN {{ ref('intm_brand_price') }} B  
ON A.PRODUCT_ID = B.PRODUCT_ID
WHERE DISCOUNT BETWEEN 0.1 AND 0.2
