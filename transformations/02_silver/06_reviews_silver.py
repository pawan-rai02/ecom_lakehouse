import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="reviews_silver",
    comment="Cleaned and standardized review data"
)
def reviews_silver():

    df = dlt.read("reviews_bronze")

    # -------------------------------
    # 1. Text cleaning and renaming
    # -------------------------------
    df = df.withColumn("sentiment", F.lower(F.trim(F.col("review_sentiment"))))

    # -------------------------------
    # 2. Data validation
    # -------------------------------
    df = df.withColumn(
        "rating",
        F.when((F.col("rating") < 1) | (F.col("rating") > 5), None)
         .otherwise(F.col("rating"))
    )

    # -------------------------------
    # 3. Sentiment standardization
    # -------------------------------
    df = df.withColumn(
        "sentiment",
        F.when(F.col("sentiment").isin("positive", "negative", "neutral"), F.col("sentiment"))
         .otherwise("unknown")
    )

    # -------------------------------
    # 4. Derived metrics
    # -------------------------------
    df = df.withColumn(
        "is_positive",
        F.when(F.col("sentiment") == "positive", 1).otherwise(0)
    ).withColumn(
        "is_negative",
        F.when(F.col("sentiment") == "negative", 1).otherwise(0)
    )

    # -------------------------------
    # 5. Drop metadata columns
    # -------------------------------
    df = df.drop("_source_file", "_load_type")

    # -------------------------------
    # 6. Final column selection
    # -------------------------------
    df = df.select(
        "order_id", "product_id", "customer_id", "rating",
        "sentiment", "is_positive", "is_negative",
        "customer_feedback", "order_date",
        "_ingestion_timestamp"
    )

    return df
