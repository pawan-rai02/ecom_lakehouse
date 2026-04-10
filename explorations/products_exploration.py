# Databricks notebook source
df = spark.read.table("ecom.bronze.products_bronze")
display(df.limit(40))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df.select([
    F.sum(F.col(c).isNull().cast('int')).alias(c)
    for c in df.columns
]).display()

# COMMAND ----------

threshold = 0.95  # drop columns with >95% nulls

total_rows = df.count()

cols_to_keep = [
    c for c in df.columns
    if (df.filter(F.col(c).isNull()).count() / total_rows) < threshold
]

new_df = df.select(cols_to_keep)

# COMMAND ----------

new_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

def clean_col(col_name):
    return F.when(
        F.trim(F.col(col_name)) == "", None
    ).otherwise(
        F.lower(F.trim(F.col(col_name)))
    ).alias(col_name)

new_df = new_df.select(
    "product_id",
    "unit_price_usd",
    
    clean_col("product_name"),
    clean_col("category"),
    clean_col("sub_category"),
    clean_col("brand"),
    
    "product_rating_avg",
    "product_reviews_count",
    "stock_quantity",
    "_ingestion_timestamp",
    "_source_file",
    "_load_type"
)

# COMMAND ----------

new_df.display(limit = 50)

# COMMAND ----------

new_df.select(
    F.sum((F.col("unit_price_usd") < 0).cast("int")).alias("invalid_price"),
    F.sum((F.col("product_rating_avg") < 0).cast("int")).alias("rating_below_0"),
    F.sum((F.col("product_rating_avg") > 5).cast("int")).alias("rating_above_5"),
    F.sum((F.col("product_reviews_count") < 0).cast("int")).alias("negative_reviews"),
    F.sum((F.col("stock_quantity") < 0).cast("int")).alias("negative_stock")
).show()

# COMMAND ----------

new_df = new_df.withColumn(
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

# COMMAND ----------

new_df.groupBy("product_id").count().filter("count > 1").show()

# COMMAND ----------

new_df = new_df.withColumn("product_id", F.col("product_id").cast("string")) \
       .withColumn("unit_price_usd", F.col("unit_price_usd").cast("double")) \
       .withColumn("product_rating_avg", F.col("product_rating_avg").cast("double")) \
       .withColumn("product_reviews_count", F.col("product_reviews_count").cast("int")) \
       .withColumn("stock_quantity", F.col("stock_quantity").cast("int")) \
       .withColumn("_ingestion_timestamp", F.col("_ingestion_timestamp").cast("timestamp"))

# COMMAND ----------

