import snowflake.snowpark.functions as F
import holidays

def is_holiday(date_col):

    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["holidays"]
    )

    raw_payments_df = dbt.source("sales", "order_items")

    df = raw_payments_df.to_pandas()
    df["IS_HOLIDAY"] = df["ORDER_DATE"].apply(is_holiday)
    return df
