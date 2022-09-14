WITH 
    customers AS 
    (   SELECT
            a.*,
            b.order_id
        FROM
            {{ source('sales', 'customers')}} a
        LEFT JOIN
            {{ source('sales', 'orders')}} b
        USING
            (customer_id)
    ) 
    ,
    orders AS 
    (   SELECT 
            * 
        FROM 
            {{ ref('fct_orders') }}
    ) 
    ,
    customer_orders AS 
    (   SELECT
            customer_id,
            ORDER_ID,
            MIN(order_date) AS first_order_date,
            MAX(order_date) AS most_recent_order_date,
            COUNT(order_id) AS number_of_orders,
            SUM(amount)     AS lifetime_value
        FROM 
            orders
        GROUP BY 
            1, 
            2
    ) 
    ,
    FINAL AS 
    (   SELECT
            customers.customer_id,
            customer_orders.order_id,
            REPLACE(customers.FIRST_NAME, '''',' ') || REPLACE(customers.LAST_NAME, '''',' ') AS 
            Customer_name,
            customer_orders.first_order_date,
            customer_orders.most_recent_order_date,
            COALESCE(customer_orders.number_of_orders, 0) AS number_of_orders,
            customer_orders.lifetime_value
        FROM 
            customers
        LEFT JOIN 
            customer_orders 
        USING 
            (customer_id)
    )
SELECT 
    * 
FROM 
    FINAL
where lifetime_value > 1000