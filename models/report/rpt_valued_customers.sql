with customers as (
    select * from {{ ref('dim_customers') }}
)
,
products as (
select
    a.ORDER_ID,
    b.product_id,
    a.customer_id
from 
    {{ ref('stg_orders') }} a
left join 
    {{ source('sales', 'order_items') }} b
on a.order_id = b.order_id
)

select
DISTINCT
    customers.*
from products
left join customers using (customer_id)

