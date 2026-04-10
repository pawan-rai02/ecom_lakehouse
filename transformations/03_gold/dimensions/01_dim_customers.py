from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="dim_customers",
    comment="Customer dimension table with current state and attributes"
)
def dim_customers():
    df = spark.read.table("customers_silver")
    
    # Add derived customer segmentation
    df = df.withColumn(
        "age_group",
        F.when(F.col("age") < 25, "18-24")
         .when(F.col("age") < 35, "25-34")
         .when(F.col("age") < 45, "35-44")
         .when(F.col("age") < 55, "45-54")
         .when(F.col("age") < 65, "55-64")
         .otherwise("65+")
    )
    
    # Loyalty tier
    df = df.withColumn(
        "loyalty_tier",
        F.when(F.col("customer_loyalty_score") >= 80, "platinum")
         .when(F.col("customer_loyalty_score") >= 60, "gold")
         .when(F.col("customer_loyalty_score") >= 40, "silver")
         .when(F.col("customer_loyalty_score") >= 20, "bronze")
         .otherwise("basic")
    )
    
    # Customer lifetime indicator
    df = df.withColumn(
        "customer_tenure_years",
        F.round(F.datediff(F.current_date(), F.col("account_creation_date")) / 365.25, 1)
    )
    
    return df.select(
        "customer_id",
        "customer_name",
        "gender",
        "age",
        "age_group",
        "customer_segment",
        "country",
        "city",
        "customer_loyalty_score",
        "loyalty_tier",
        "total_orders_by_customer",
        "account_creation_date",
        "customer_tenure_years"
    )
