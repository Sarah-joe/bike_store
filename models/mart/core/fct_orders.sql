WITH ORDERS AS (
    SELECT 
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE
    FROM 
        {{ source('sales', 'orders') }}
),

PAYMENTS AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

Final as (
    select
        ORDERS.ORDER_ID,
        ORDERS.CUSTOMER_ID,
        ORDERS.ORDER_DATE,
        coalesce(PAYMENTS.AMOUNT, 0) as AMOUNT
    FROM
        orders
        left join PAYMENTS using (ORDER_ID)
)

SELECT * FROM final