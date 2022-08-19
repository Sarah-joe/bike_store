{{
    config(
        tags = ["rpt_valued_customers"]
    )
}}

select 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    FIRST_ORDER_DATE,
    MOST_RECENT_ORDER_DATE,
    NUMBER_OF_ORDERS,
    LIFETIME_VALUE
from
    {{ ref('rpt_valued_customers')}}
group by 1,2,3,4,5,6
having count(*)>1