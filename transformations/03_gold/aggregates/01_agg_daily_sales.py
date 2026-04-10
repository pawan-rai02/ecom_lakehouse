from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="agg_daily_sales",
    comment="Daily sales summary with revenue, orders, and customer metrics"
)
def agg_daily_sales():
    fact_orders = spark.read.table("fact_orders")
    
    df = fact_orders.groupBy("order_date").agg(
        # Order counts
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        
        # Revenue metrics
        F.sum("order_gross_amount").alias("gross_revenue"),
        F.sum("order_discount_amount").alias("total_discounts"),
        F.sum("order_net_revenue").alias("net_revenue"),
        F.sum("shipping_cost").alias("total_shipping_revenue"),
        F.sum("total_order_value").alias("total_revenue"),
        
        # Average order value
        F.round(F.avg("total_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("order_net_revenue"), 2).alias("avg_net_revenue"),
        
        # Product metrics
        F.sum("total_items_quantity").alias("total_items_sold"),
        F.sum("total_line_items").alias("total_line_items"),
        F.round(F.avg("total_line_items"), 2).alias("avg_items_per_order"),
        
        # Order status breakdown
        F.sum(F.when(F.col("order_status") == "completed", 1).otherwise(0)).alias("completed_orders"),
        F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("order_status") == "pending", 1).otherwise(0)).alias("pending_orders"),
        
        # Delivery metrics
        F.sum("is_delayed").alias("delayed_deliveries"),
        
        # Payment metrics
        F.sum(F.when(F.col("payment_status") == "completed", 1).otherwise(0)).alias("successful_payments"),
        F.sum(F.when(F.col("payment_status") == "failed", 1).otherwise(0)).alias("failed_payments"),
        F.round(F.avg("fraud_risk_score"), 2).alias("avg_fraud_risk_score")
    )
    
    # Calculate conversion rates
    df = df.withColumn(
        "completion_rate",
        F.round((F.col("completed_orders") / F.col("total_orders")) * 100, 2)
    ).withColumn(
        "cancellation_rate",
        F.round((F.col("cancelled_orders") / F.col("total_orders")) * 100, 2)
    ).withColumn(
        "on_time_delivery_rate",
        F.round(((F.col("total_orders") - F.col("delayed_deliveries")) / F.col("total_orders")) * 100, 2)
    )
    
    return df.orderBy(F.col("order_date").desc())
