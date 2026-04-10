from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fact_shipping",
    comment="Shipping fact table with delivery performance and logistics metrics"
)
def fact_shipping():
    shipping = spark.read.table("shipping_silver")
    
    # Join with orders for additional context
    orders = spark.read.table("orders_silver")
    order_context = orders.select(
        "order_id",
        F.col("customer_id").alias("order_customer_id"),
        "order_date",
        "country",
        "city"
    )
    
    df = shipping.join(order_context, "order_id", "left")
    
    # Delivery speed category based on delivery_days
    df = df.withColumn(
        "delivery_speed",
        F.when(F.col("delivery_days") <= 2, "express")
         .when(F.col("delivery_days") <= 5, "fast")
         .when(F.col("delivery_days") <= 10, "standard")
         .otherwise("slow")
    )
    
    # Shipping cost per day
    df = df.withColumn(
        "shipping_cost_per_day",
        F.when(
            F.col("delivery_days") > 0,
            F.round(F.col("shipping_cost_usd") / F.col("delivery_days"), 2)
        ).otherwise(F.col("shipping_cost_usd"))
    )
    
    return df.select(
        # Keys
        "order_id",
        "customer_id",
        "order_date",
        
        # Location
        "country",
        "city",
        
        # Shipping attributes
        "shipping_method",
        "shipping_country",
        "warehouse_location",
        
        # Cost
        "shipping_cost_usd",
        "shipping_cost_per_day",
        
        # Delivery metrics
        "delivery_days",
        "delivery_speed",
        
        # Status
        "delivery_status",
        "is_delayed"
    )
