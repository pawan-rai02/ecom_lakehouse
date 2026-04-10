# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.read.table("ecom.bronze.order_items_bronze")
display(df.limit(40))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).display(truncate=False)

# COMMAND ----------

df.filter(F.col('customer_name').isNull()).count()

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

validation_df = new_df.select(
    "*",
    (F.col("quantity") * F.col("unit_price_usd")).alias("calc_total"),
    (F.col("total_price_usd") - F.col("cost_usd")).alias("calc_profit"),
    ((F.col("profit_usd") / F.col("total_price_usd")) * 100).alias("calc_margin")
)

# COMMAND ----------

# compare actual vs calculated
validation_df.select(
    F.sum(
        (F.abs(F.col("total_price_usd") - F.col("calc_total")) > 0.01)
        .cast("int")
    ).alias("total_price_mismatch"),

    F.sum(
        (F.abs(F.col("profit_usd") - F.col("calc_profit")) > 0.01)
        .cast("int")
    ).alias("profit_mismatch"),

    F.sum(
        (F.abs(F.col("profit_margin_percent") - F.col("calc_margin")) > 0.1)
        .cast("int")
    ).alias("margin_mismatch")

).show()

# COMMAND ----------

validation_df = new_df \
    .withColumn(
        "calc_total",
        (F.col("quantity") * F.col("unit_price_usd")) - F.col("discount_amount_usd")
    )

# COMMAND ----------

validation_df.select(
    F.sum(
        (F.abs(F.col("total_price_usd") - F.col("calc_total")) > 0.01)
        .cast("int")
    )).display()

# COMMAND ----------

# standardized

new_df = new_df.withColumn(
    "total_price_usd",
    (F.col("quantity") * F.col("unit_price_usd")) - F.col("discount_amount_usd")
).withColumn(
    "profit_usd",
    F.col("total_price_usd") - F.col("cost_usd")
).withColumn(
    "profit_margin_percent",
    (F.col("profit_usd") / F.col("total_price_usd")) * 100
)

# COMMAND ----------

