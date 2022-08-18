select
    ORDER_ID,
    ITEM_ID,
    PRODUCT_ID,
    QUANTITY, 

    ROUND(LIST_PRICE) AMOUNT,
    DISCOUNT*100 AS DISCOUNT

from {{ source('sales', 'order_items') }}