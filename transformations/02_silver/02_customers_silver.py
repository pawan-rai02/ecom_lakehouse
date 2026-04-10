import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="customers_silver",
    comment="Cleaned and standardized customer data from bronze layer"
)
def customers_silver():

    df = dlt.read("customers_bronze")

    # STEP 1: Select required columns
    df = df.select(
        "customer_id",
        "customer_name",
        "gender",
        "age",
        "customer_segment",
        "country",
        "city",
        "customer_loyalty_score",
        "total_orders_by_customer",
        "account_creation_date"
    )

    # STEP 2: Remove null customer_id
    df = df.filter(F.col("customer_id").isNotNull())

    # STEP 3: Normalize strings
    df = df.withColumn("gender", F.lower(F.trim(F.col("gender")))) \
           .withColumn("customer_segment", F.lower(F.trim(F.col("customer_segment")))) \
           .withColumn("country", F.lower(F.trim(F.col("country")))) \
           .withColumn("city", F.lower(F.trim(F.col("city")))) \
           .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))

    # STEP 4: Enforce gender categories
    df = df.withColumn(
        "gender",
        F.when(F.col("gender").isNull(), "unknown")
         .when(F.col("gender") == "male", "male")
         .when(F.col("gender") == "female", "female")
         .otherwise("other")
    )

    # STEP 5: Age validation
    df = df.filter(
        (F.col("age").isNull()) |
        ((F.col("age") >= 10) & (F.col("age") <= 100))
    )

    # STEP 6: Loyalty score validation
    df = df.filter(
        (F.col("customer_loyalty_score").isNull()) |
        ((F.col("customer_loyalty_score") >= 0) & 
         (F.col("customer_loyalty_score") <= 100))
    )

    # STEP 7: Date feature extraction
    df = df.withColumn("account_year", F.year("account_creation_date")) \
           .withColumn("account_month", F.month("account_creation_date")) \
           .withColumn("account_day", F.dayofmonth("account_creation_date"))

    # STEP 8: Final column ordering
    df = df.select(
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

    return df