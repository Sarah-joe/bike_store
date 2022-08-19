import snowflake.snowpark.functions as F 

def model(dbt, session):

    dbt.config(
        materialized = "table"
    )

    raw_payments_df = dbt.source("sales", "order_items")

    renamed_df = raw_payments_df.select
    (
        F.col("order_id"),
        F.col("ITEM_ID"),
        F.col("PRODUCT_ID"),
        F.col("QUANTITY,"),

        F.col(ROUND("LIST_PRICE").alias("AMOUNT"),
        F.col(("DISCOUNT")*100).alias("DISCOUNT")
    )

    return renamed_df
