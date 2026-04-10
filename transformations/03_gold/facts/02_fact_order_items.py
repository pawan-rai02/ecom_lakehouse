from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fact_order_items",
    comment="Order item fact table with product-level transaction details"
)
def fact_order_items():
    # Base order items
    order_items = spark.read.table("order_items_silver")
    
    # Join with orders for additional context
    orders = spark.read.table("orders_silver")
    order_context = orders.select(
        "order_id",
        F.col("customer_id").alias("order_customer_id"),
        "order_date",
        "order_status",
        "country",
        "city"
    )
    
    df = order_items.join(order_context, "order_id", "left")
    
    # Calculate discount percentage
    df = df.withColumn(
        "discount_percentage",
        F.when(
            F.col("total_price_usd") > 0,
            F.round((F.col("discount_amount_usd") / F.col("total_price_usd")) * 100, 2)
        ).otherwise(0)
    )
    
    # Revenue per unit
    df = df.withColumn(
        "revenue_per_unit",
        F.when(
            F.col("quantity") > 0,
            F.round(F.col("net_revenue_usd") / F.col("quantity"), 2)
        ).otherwise(0)
    )
    
    # Item profitability indicator
    df = df.withColumn(
        "is_profitable_item",
        F.when(F.col("net_revenue_usd") > 0, 1).otherwise(0)
    )
    
    return df.select(
        # Keys
        "order_id",
        "product_id",
        "customer_id",
        "order_date",
        
        # Location
        "country",
        "city",
        
        # Quantity metrics
        "quantity",
        
        # Financial metrics
        "unit_price_usd",
        "discount_percent",
        "discount_amount_usd",
        "discount_percentage",
        "total_price_usd",
        "tax_usd",
        "net_revenue_usd",
        "revenue_per_unit",
        "cost_usd",
        "profit_usd",
        "profit_margin_percent",
        
        # Status indicators
        "order_status",
        "is_profitable_item"
    )
