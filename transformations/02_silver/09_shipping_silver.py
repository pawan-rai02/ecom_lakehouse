import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="shipping_silver",
    comment="Cleaned and standardized shipping data"
)
def shipping_silver():

    df = dlt.read("shipping_bronze")

    # -------------------------------
    # 1. Text cleaning
    # -------------------------------
    df = df.withColumn("shipping_method", F.lower(F.trim(F.col("shipping_method")))) \
           .withColumn("delivery_status", F.lower(F.trim(F.col("delivery_status")))) \
           .withColumn("shipping_country", F.lower(F.trim(F.col("shipping_country")))) \
           .withColumn("warehouse_location", F.lower(F.trim(F.col("warehouse_location"))))

    # -------------------------------
    # 2. Data validation
    # -------------------------------
    df = df.withColumn(
        "shipping_cost_usd",
        F.when(F.col("shipping_cost_usd") < 0, None)
         .otherwise(F.col("shipping_cost_usd"))
    ).withColumn(
        "delivery_days",
        F.when(F.col("delivery_days") < 0, None)
         .otherwise(F.col("delivery_days"))
    )

    # -------------------------------
    # 3. Derived metrics
    # -------------------------------
    df = df.withColumn(
        "is_delayed",
        F.when(F.col("delivery_days") > 7, 1).otherwise(0)
    )

    # -------------------------------
    # 4. Drop metadata columns
    # -------------------------------
    df = df.drop("_source_file", "_load_type")

    # -------------------------------
    # 5. Final column selection
    # -------------------------------
    df = df.select(
        "order_id", "customer_id", "shipping_method",
        "shipping_cost_usd", "delivery_days", "is_delayed",
        "shipping_country", "warehouse_location", "delivery_status",
        "_ingestion_timestamp"
    )

    return df
