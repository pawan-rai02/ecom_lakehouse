from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="agg_customer_metrics",
    comment="Customer-level aggregated metrics for segmentation and analysis"
)
def agg_customer_metrics():
    # Customer dimension
    dim_customers = spark.read.table("dim_customers")
    
    # Order facts
    fact_orders = spark.read.table("fact_orders")
    
    # Calculate customer metrics
    customer_orders = fact_orders.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_order_value").alias("lifetime_value"),
        F.sum("order_net_revenue").alias("lifetime_net_revenue"),
        F.round(F.avg("total_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("order_net_revenue"), 2).alias("avg_net_revenue"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.sum("total_items_quantity").alias("total_items_purchased"),
        F.countDistinct("country").alias("countries_shopped_from"),
        F.sum(F.when(F.col("order_status") == "completed", 1).otherwise(0)).alias("completed_orders"),
        F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders")
    )
    
    # Session facts
    fact_sessions = spark.read.table("fact_sessions")
    customer_sessions = fact_sessions.groupBy("customer_id").agg(
        F.count("*").alias("total_sessions"),
        F.sum("pages_visited").alias("total_pages_visited"),
        F.round(F.avg("engagement_score"), 2).alias("avg_engagement_score")
    )
    
    # Review facts
    fact_reviews = spark.read.table("fact_reviews")
    customer_reviews = fact_reviews.groupBy("customer_id").agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("rating"), 2).alias("avg_rating_given")
    )
    
    # Join all customer metrics
    df = dim_customers \
        .join(customer_orders, "customer_id", "left") \
        .join(customer_sessions, "customer_id", "left") \
        .join(customer_reviews, "customer_id", "left")
    
    # Calculate derived metrics
    df = df.withColumn(
        "days_since_first_order",
        F.datediff(F.current_date(), F.col("first_order_date"))
    ).withColumn(
        "days_since_last_order",
        F.datediff(F.current_date(), F.col("last_order_date"))
    ).withColumn(
        "customer_recency_segment",
        F.when(F.col("days_since_last_order") <= 30, "active")
         .when(F.col("days_since_last_order") <= 90, "at_risk")
         .when(F.col("days_since_last_order") <= 180, "dormant")
         .otherwise("churned")
    ).withColumn(
        "customer_value_segment",
        F.when(F.col("lifetime_value") >= 1000, "high_value")
         .when(F.col("lifetime_value") >= 500, "medium_value")
         .otherwise("low_value")
    )
    
    return df
