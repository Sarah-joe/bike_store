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
    customers.*,
    case when number_of_orders > 2 then 'True' else false end result,
    'DBT_'|| TO_CHAR(curreNT_DATE,'YYYY_MM_DD_HH_MI_SS') || '_content' as etl_batch_id
	,'bi_dbt_user_prd' as etl_insert_user_id
	, current_timestamp as etl_insert_rec_dttm
	, null as etl_update_user_id
	, cast(null as timestamp) as etl_update_rec_dttm
from products
left join customers using (customer_id)
where CUSTOMER_ID is not null and CUSTOMER_name is not null and order_ID is not null and number_of_orders is not null
order by  CUSTOMER_ID asc