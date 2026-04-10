import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="orders_silver",
    comment="Cleaned and standardized order-level dataset"
)
def orders_silver():

    df = dlt.read("orders_bronze")

    # -------------------------------
    # 1. Drop redundant time columns
    # -------------------------------
    df = df.drop(
        "order_year", "order_month", "order_day",
        "order_hour", "order_minute", "order_second"
    )

    # -------------------------------
    # 2. Standardize categorical columns
    # -------------------------------
    df = df.withColumn("order_status", F.trim(F.lower(F.col("order_status")))) \
           .withColumn("order_priority", F.trim(F.lower(F.col("order_priority")))) \
           .withColumn("coupon_used", F.trim(F.lower(F.col("coupon_used")))) \
           .withColumn("is_weekend", F.trim(F.lower(F.col("is_weekend")))) \
           .withColumn("campaign_source", F.trim(F.lower(F.col("campaign_source"))))

    # -------------------------------
    # 3. Convert flags (overwrite)
    # -------------------------------
    df = df.withColumn(
        "coupon_used",
        F.when(F.col("coupon_used") == "yes", 1).otherwise(0)
    ).withColumn(
        "is_weekend",
        F.when(F.col("is_weekend") == "yes", 1).otherwise(0)
    )

    # -------------------------------
    # 4. Enforce coupon logic
    # -------------------------------
    df = df.withColumn(
        "coupon_code",
        F.when(F.col("coupon_used") == 0, None)
         .otherwise(F.col("coupon_code"))
    )

    # -------------------------------
    # 5. Drop unnecessary metadata
    # -------------------------------
    df = df.drop("_source_file", "_load_type")

    # -------------------------------
    # 6. Create surrogate key (CRITICAL)
    # -------------------------------
    df = df.withColumn(
        "order_key",
        F.concat_ws("_", F.col("order_id"), F.col("order_date"))
    )

    # -------------------------------
    # 7. Final column selection
    # -------------------------------
    df = df.select(
        "order_key",
        "order_id",
        "customer_id",
        "country",
        "city",
        "order_date",
        "order_status",
        "order_priority",
        "currency",
        "coupon_used",
        "coupon_code",
        "campaign_source",
        "_ingestion_timestamp"
    )

    return df
