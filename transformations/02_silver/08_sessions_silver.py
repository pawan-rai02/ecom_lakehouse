import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="sessions_silver",
    comment="Cleaned and standardized session data"
)
def sessions_silver():

    df = dlt.read("sessions_bronze")

    # -------------------------------
    # 1. Text cleaning
    # -------------------------------
    df = df.withColumn("device_type", F.lower(F.trim(F.col("device_type")))) \
           .withColumn("traffic_source", F.lower(F.trim(F.col("traffic_source")))) \
           .withColumn("campaign_source", F.lower(F.trim(F.col("campaign_source"))))

    # -------------------------------
    # 2. Data validation
    # -------------------------------
    df = df.withColumn(
        "session_duration_minutes",
        F.when(F.col("session_duration_minutes") < 0, None)
         .otherwise(F.col("session_duration_minutes"))
    ).withColumn(
        "pages_visited",
        F.when(F.col("pages_visited") < 0, 0)
         .otherwise(F.col("pages_visited"))
    )

    # -------------------------------
    # 3. Device type standardization
    # -------------------------------
    df = df.withColumn(
        "device_type",
        F.when(F.col("device_type").isin("mobile", "desktop", "tablet"), F.col("device_type"))
         .otherwise("other")
    )

    # -------------------------------
    # 4. Derived metrics
    # -------------------------------
    df = df.withColumn(
        "is_mobile",
        F.when(F.col("device_type") == "mobile", 1).otherwise(0)
    )

    # -------------------------------
    # 5. Drop metadata columns
    # -------------------------------
    df = df.drop("_source_file", "_load_type")

    # -------------------------------
    # 6. Final column selection
    # -------------------------------
    df = df.select(
        "order_id", "customer_id", "device_type", "is_mobile",
        "traffic_source", "campaign_source",
        "session_duration_minutes", "pages_visited", "abandoned_cart_before",
        "_ingestion_timestamp"
    )

    return df
