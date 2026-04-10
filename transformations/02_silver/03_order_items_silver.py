import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="order_items_silver",
    comment="Cleaned and standardized order items data"
)
def order_items_silver():

    df = dlt.read("order_items_bronze")

    # -------------------------------
    # 1. Data validation
    # -------------------------------
    df = df.withColumn(
        "quantity",
        F.when(F.col("quantity") <= 0, None).otherwise(F.col("quantity"))
    ).withColumn(
        "unit_price_usd",
        F.when(F.col("unit_price_usd") < 0, None).otherwise(F.col("unit_price_usd"))
    ).withColumn(
        "discount_amount_usd",
        F.when(F.col("discount_amount_usd") < 0, 0).otherwise(F.col("discount_amount_usd"))
    ).withColumn(
        "total_price_usd",
        F.when(F.col("total_price_usd") < 0, None).otherwise(F.col("total_price_usd"))
    ).withColumn(
        "tax_usd",
        F.when(F.col("tax_usd") < 0, 0).otherwise(F.col("tax_usd"))
    ).withColumn(
        "discount_percent",
        F.when((F.col("discount_percent") < 0) | (F.col("discount_percent") > 100), None)
         .otherwise(F.col("discount_percent"))
    )

    # -------------------------------
    # 2. Derived metrics
    # -------------------------------
    df = df.withColumn(
        "net_revenue_usd",
        F.col("total_price_usd") - F.coalesce(F.col("discount_amount_usd"), F.lit(0))
    )

    # -------------------------------
    # 3. Drop metadata columns
    # -------------------------------
    df = df.drop("_source_file", "_load_type")

    # -------------------------------
    # 4. Final column selection
    # -------------------------------
    df = df.select(
        "order_id", "product_id", "customer_id",
        "quantity", "unit_price_usd", "discount_percent", "discount_amount_usd",
        "total_price_usd", "tax_usd", "net_revenue_usd",
        "cost_usd", "profit_usd", "profit_margin_percent",
        "_ingestion_timestamp"
    )

    return df
