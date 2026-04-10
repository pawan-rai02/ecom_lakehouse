import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="risk_support_silver",
    comment="Cleaned and standardized risk and support data"
)
def risk_support_silver():

    df = dlt.read("risk_support_bronze")

    # -------------------------------
    # 1. Text cleaning
    # -------------------------------
    df = df.withColumn("order_status", F.lower(F.trim(F.col("order_status")))) \
           .withColumn("return_reason", F.lower(F.trim(F.col("return_reason")))) \
           .withColumn("support_ticket_created", F.lower(F.trim(F.col("support_ticket_created"))))

    # -------------------------------
    # 2. Data validation
    # -------------------------------
    df = df.withColumn(
        "fraud_risk_score",
        F.when((F.col("fraud_risk_score") < 0) | (F.col("fraud_risk_score") > 100), None)
         .otherwise(F.col("fraud_risk_score"))
    )

    # -------------------------------
    # 3. Order status standardization
    # -------------------------------
    df = df.withColumn(
        "order_status",
        F.when(F.col("order_status").isin("completed", "pending", "cancelled", "returned", "processing"), 
               F.col("order_status"))
         .otherwise("other")
    )

    # -------------------------------
    # 4. Derived metrics
    # -------------------------------
    df = df.withColumn(
        "is_high_risk",
        F.when(F.col("fraud_risk_score") >= 70, 1).otherwise(0)
    ).withColumn(
        "has_support_ticket",
        F.when(F.col("support_ticket_created") == "yes", 1).otherwise(0)
    ).withColumn(
        "has_return",
        F.when(F.col("return_reason").isNotNull(), 1).otherwise(0)
    )

    # -------------------------------
    # 5. Drop metadata columns
    # -------------------------------
    df = df.drop("_source_file", "_load_type")

    # -------------------------------
    # 6. Final column selection
    # -------------------------------
    df = df.select(
        "order_id", "customer_id", "fraud_risk_score", "is_high_risk",
        "order_status", "return_reason", "has_return",
        "support_ticket_created", "has_support_ticket",
        "_ingestion_timestamp"
    )

    return df
