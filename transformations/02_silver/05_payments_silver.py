import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Helper function for cleaning text columns
def clean_col(col_name):
    return F.when(
        F.trim(F.col(col_name)) == "", None
    ).otherwise(
        F.lower(F.trim(F.col(col_name)))
    )

@dlt.table(
    name="payments_silver",
    comment="Cleaned and standardized payment data from bronze layer"
)
def payments_silver():

    df = dlt.read("payments_bronze")

    # -------------------------------
    # 1. Text Cleaning
    # -------------------------------
    df = df.select(
        "order_id",
        "customer_id",
        clean_col("payment_method").alias("payment_method"),
        clean_col("payment_status").alias("payment_status"),
        clean_col("installment_plan").alias("installment_plan"),
        "fraud_risk_score",
        "total_price_usd",
        clean_col("currency").alias("currency"),
        "_ingestion_timestamp",
        "_source_file",
        "_load_type"
    )

    # -------------------------------
    # 2. Data Type Enforcement
    # -------------------------------
    df = df.withColumn("order_id", F.col("order_id").cast("string")) \
           .withColumn("customer_id", F.col("customer_id").cast("string")) \
           .withColumn("fraud_risk_score", F.col("fraud_risk_score").cast("double")) \
           .withColumn("total_price_usd", F.col("total_price_usd").cast("double")) \
           .withColumn("_ingestion_timestamp", F.col("_ingestion_timestamp").cast("timestamp"))

    # -------------------------------
    # 3. Data Validation
    # -------------------------------
    df = df.withColumn(
        "fraud_risk_score",
        F.when((F.col("fraud_risk_score") < 0) | (F.col("fraud_risk_score") > 100), None)
         .otherwise(F.col("fraud_risk_score"))
    ).withColumn(
        "total_price_usd",
        F.when(F.col("total_price_usd") < 0, None)
         .otherwise(F.col("total_price_usd"))
    )

    # -------------------------------
    # 4. Deduplication (Keep Latest)
    # -------------------------------
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_ingestion_timestamp").desc())

    df = df.withColumn(
        "row_num",
        F.row_number().over(window)
    ).filter(
        F.col("row_num") == 1
    ).drop("row_num")

    return df
