from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fact_orders",
    comment="Order fact table with aggregated metrics and foreign keys to dimensions"
)
def fact_orders():
    # Base order data
    orders = spark.read.table("orders_silver")
    
    # Order items aggregated to order level
    order_items = spark.read.table("order_items_silver")
    order_metrics = order_items.groupBy("order_id").agg(
        F.sum("quantity").alias("total_items_quantity"),
        F.count("*").alias("total_line_items"),
        F.sum("total_price_usd").alias("order_gross_amount"),
        F.sum("discount_amount_usd").alias("order_discount_amount"),
        F.sum("tax_usd").alias("order_tax_amount"),
        F.sum("net_revenue_usd").alias("order_net_revenue"),
        F.countDistinct("product_id").alias("unique_products_count")
    )
    
    # Payment data
    payments = spark.read.table("payments_silver")
    payment_metrics = payments.groupBy("order_id").agg(
        F.first("payment_method").alias("payment_method"),
        F.first("payment_status").alias("payment_status"),
        F.first("fraud_risk_score").alias("fraud_risk_score"),
        F.sum("total_price_usd").alias("payment_amount")
    )
    
    # Shipping data
    shipping = spark.read.table("shipping_silver")
    shipping_metrics = shipping.groupBy("order_id").agg(
        F.first("shipping_method").alias("shipping_method"),
        F.sum("shipping_cost_usd").alias("shipping_cost"),
        F.first("delivery_status").alias("delivery_status"),
        F.first("is_delayed").alias("is_delayed")
    )
    
    # Join all metrics to orders
    df = orders \
        .join(order_metrics, "order_id", "left") \
        .join(payment_metrics, "order_id", "left") \
        .join(shipping_metrics, "order_id", "left")
    
    # Calculate total order value
    df = df.withColumn(
        "total_order_value",
        F.coalesce(F.col("order_gross_amount"), F.lit(0)) + 
        F.coalesce(F.col("shipping_cost"), F.lit(0))
    )
    
    # Fraud risk category
    df = df.withColumn(
        "fraud_risk_category",
        F.when(F.col("fraud_risk_score") >= 70, "high")
         .when(F.col("fraud_risk_score") >= 40, "medium")
         .when(F.col("fraud_risk_score").isNotNull(), "low")
         .otherwise("unknown")
    )
    
    return df.select(
        # Surrogate key
        "order_key",
        
        # Foreign keys
        "order_id",
        "customer_id",
        "order_date",
        
        # Order attributes
        "country",
        "city",
        "order_status",
        "order_priority",
        "currency",
        "coupon_used",
        "coupon_code",
        "campaign_source",
        
        # Order metrics
        "total_items_quantity",
        "total_line_items",
        "unique_products_count",
        
        # Financial metrics
        "order_gross_amount",
        "order_discount_amount",
        "order_tax_amount",
        "order_net_revenue",
        "shipping_cost",
        "total_order_value",
        
        # Payment attributes
        "payment_method",
        "payment_status",
        "payment_amount",
        "fraud_risk_score",
        "fraud_risk_category",
        
        # Shipping attributes
        "shipping_method",
        "delivery_status",
        "is_delayed"
    )
