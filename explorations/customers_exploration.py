# Databricks notebook source
df = spark.read.table("ecom.bronze.customers_bronze")
display(df.limit(40))

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(
    df.select([
        F.sum(F.col(c).isNull().cast('int')).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### explore bronze customers table in this order:
# MAGIC
# MAGIC - Basic sanity check
# MAGIC - Schema understanding
# MAGIC - Null analysis (you already did partly)
# MAGIC - Data distribution
# MAGIC - Data quality issues
# MAGIC - Insights - then design silver layer

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df.select("customer_id").distinct().count()

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("customer_id").count().filter("count > 1").orderBy("count", ascending=False).display()

# COMMAND ----------

df.filter(F.col("total_price_usd") < 0).display()

# COMMAND ----------

# ivalid ages 
df.filter((F.col("age") < 0) | (F.col("age") > 100)).display()

# COMMAND ----------

df.filter(F.col("order_id").isNotNull()).count()

# COMMAND ----------

customer_df = df.select(
    F.col("customer_id"),
    F.col("customer_name"),
    F.col("gender"),
    F.col("age"),
    F.col("customer_segment"),
    F.col("country"),
    F.col("city"),
    F.col("customer_loyalty_score"),
    F.col("total_orders_by_customer"),
    F.col("account_creation_date")
)

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

customer_df.select(["gender", 'customer_segment', 'country', 'city']).show()

# COMMAND ----------

customer_df = customer_df.select(
    F.col("customer_id"),
    F.initcap(F.trim(F.col("customer_name"))).alias("customer_name"),
    F.initcap(F.trim(F.col("gender"))).alias("gender"),
    F.initcap(F.trim(F.col("customer_segment"))).alias("customer_segment"),
    F.initcap(F.trim(F.col("country"))).alias("country"),
    F.initcap(F.trim(F.col("city"))).alias("city"),
    F.col("age"),
    F.col("customer_loyalty_score"),
    F.col("total_orders_by_customer"),
    F.col("account_creation_date")
)

# COMMAND ----------

customer_df.select("country").distinct().display()

# COMMAND ----------

customer_df.select("age").describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep row IF:
# MAGIC
# MAGIC - age is NULL 
# MAGIC - OR age is between 10 and 100 

# COMMAND ----------

customer_df = customer_df.filter(
    (F.col("age").isNull()) | 
    ((F.col("age") >= 10) & (F.col("age") <= 100))
)

# COMMAND ----------

customer_df.select("age").describe().display()

# COMMAND ----------

customer_df.select("customer_loyalty_score").describe().display()

# COMMAND ----------

customer_df.filter(
    (F.col("customer_loyalty_score").isNull()) |
    (
        (F.col("customer_loyalty_score") >= 0) &
        (F.col("customer_loyalty_score") <= 100)
    )
).count()

# COMMAND ----------

customer_df = customer_df.filter(
    (F.col("customer_loyalty_score").isNull()) |
    (
        (F.col("customer_loyalty_score") >= 0) &
        (F.col("customer_loyalty_score") <= 100)
    )
)

# COMMAND ----------

customer_df.groupBy(customer_df.columns).count().filter(F.col("count") > 1).display()

# COMMAND ----------

customer_df.select("gender").distinct().display()

# COMMAND ----------

customer_df = customer_df.withColumn(
    "gender",
    F.lower(F.trim(F.col("gender")))
)

# COMMAND ----------

customer_df = customer_df.withColumn(
    "gender",
    F.when(F.col("gender").isNull(), "unknown")
     .when(F.col("gender") == "male", "male")
     .when(F.col("gender") == "female", "female")
     .otherwise("other")
)

# COMMAND ----------

customer_df.select("customer_segment").distinct().display()

# COMMAND ----------

customer_df = customer_df.withColumn(
    "customer_segment",
    F.lower(F.trim(F.col("customer_segment")))
)

# COMMAND ----------

customer_df.select("customer_segment").distinct().display()

# COMMAND ----------

customer_df.select("country").distinct().display()


# COMMAND ----------

customer_df.select("city").distinct().display()

# COMMAND ----------

customer_df = customer_df.withColumn(
    "country",
    F.lower(F.trim(F.col("country")))
).withColumn(
    "city",
    F.lower(F.trim(F.col("city")))
)

# COMMAND ----------

customer_df.select("country", "city").distinct().display()

# COMMAND ----------

display(customer_df.select("account_creation_date").describe())

# COMMAND ----------

customer_df.filter(
    F.col("account_creation_date") > F.current_date()
).display()

# COMMAND ----------

customer_df = customer_df.withColumn(
    "account_year", F.year(F.col("account_creation_date"))
).withColumn(
    "account_month", F.month(F.col("account_creation_date"))
).withColumn(
    "account_day", F.dayofmonth(F.col("account_creation_date"))
)

# COMMAND ----------

customer_df = customer_df.select(
    "customer_id",
    "customer_name",
    "gender",
    "age",
    "customer_segment",
    "country",
    "city",
    "customer_loyalty_score",
    "total_orders_by_customer",
    "account_creation_date",
    "account_year",
    "account_month",
    "account_day"
)

# COMMAND ----------

customer_df.display(limit=50)

# COMMAND ----------

