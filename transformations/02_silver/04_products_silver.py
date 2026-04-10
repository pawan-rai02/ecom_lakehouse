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
    name="products_silver",
    comment="Cleaned and standardized product data from bronze layer"
)
def products_silver():

    df = dlt.read("products_bronze")

    # -------------------------------
    # 1. Text Cleaning
    # -------------------------------
    df = df.select(
        "product_id",
        "unit_price_usd",
        clean_col("product_name").alias("product_name"),
        clean_col("category").alias("category"),
        clean_col("sub_category").alias("sub_category"),
        clean_col("brand").alias("brand"),
        "product_rating_avg",
        "product_reviews_count",
        "stock_quantity",
        "_ingestion_timestamp",
        "_source_file",
        "_load_type"
    )

    # -------------------------------
    # 2. Data Type Enforcement
    # -------------------------------
    df = df.withColumn("product_id", F.col("product_id").cast("string")) \
           .withColumn("unit_price_usd", F.col("unit_price_usd").cast("double")) \
           .withColumn("product_rating_avg", F.col("product_rating_avg").cast("double")) \
           .withColumn("product_reviews_count", F.col("product_reviews_count").cast("int")) \
           .withColumn("stock_quantity", F.col("stock_quantity").cast("int")) \
           .withColumn("_ingestion_timestamp", F.col("_ingestion_timestamp").cast("timestamp"))

    # -------------------------------
    # 3. Data Validation
    # -------------------------------
    df = df.withColumn(
        "unit_price_usd",
        F.when(F.col("unit_price_usd") < 0, None)
         .otherwise(F.col("unit_price_usd"))
    ).withColumn(
        "product_rating_avg",
        F.when((F.col("product_rating_avg") < 0) | (F.col("product_rating_avg") > 5), None)
         .otherwise(F.col("product_rating_avg"))
    ).withColumn(
        "product_reviews_count",
        F.when(F.col("product_reviews_count") < 0, None)
         .otherwise(F.col("product_reviews_count"))
    ).withColumn(
        "stock_quantity",
        F.when(F.col("stock_quantity") < 0, None)
         .otherwise(F.col("stock_quantity"))
    )

    # -------------------------------
    # 4. Deduplication (Keep Latest)
    # -------------------------------
    window = Window.partitionBy("product_id").orderBy(F.col("_ingestion_timestamp").desc())

    df = df.withColumn(
        "row_num",
        F.row_number().over(window)
    ).filter(
        F.col("row_num") == 1
    ).drop("row_num")

    return df