with orders as (
    
    select 
        SHIPPED_DATE,
        CUSTOMER_ID,
        ORDER_ID,
        ORDER_DATE,
        REQUIRED_DATE,
        STORE_ID
    from 
        {{ source('sales', 'orders') }}
    )

select * from orders