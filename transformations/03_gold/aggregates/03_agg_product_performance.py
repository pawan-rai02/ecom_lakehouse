from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="agg_product_performance",
    comment="Product-level performance metrics for merchandising and inventory decisions"
)
def agg_product_performance():
    # Product dimension
    dim_products = spark.read.table("dim_products")
    
    # Order item facts
    fact_order_items = spark.read.table("fact_order_items")
    
    # Calculate product sales metrics
    product_sales = fact_order_items.groupBy("product_id").agg(
        F.count("*").alias("total_units_sold"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("net_revenue_usd").alias("total_revenue"),
        F.sum("discount_amount_usd").alias("total_discounts_given"),
        F.round(F.avg("unit_price_usd"), 2).alias("avg_selling_price"),
        F.round(F.avg("discount_percentage"), 2).alias("avg_discount_percentage"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("order_id").alias("unique_orders")
    )
    
    # Review metrics
    fact_reviews = spark.read.table("fact_reviews")
    product_reviews = fact_reviews.groupBy("product_id").agg(
        F.count("*").alias("review_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("is_positive").alias("positive_reviews"),
        F.sum("is_negative").alias("negative_reviews")
    )
    
    # Join product dimension with metrics
    df = dim_products \
        .join(product_sales, "product_id", "left") \
        .join(product_reviews, "product_id", "left")
    
    # Calculate derived metrics
    df = df.withColumn(
        "revenue_per_unit",
        F.when(
            F.col("total_quantity_sold") > 0,
            F.round(F.col("total_revenue") / F.col("total_quantity_sold"), 2)
        ).otherwise(0)
    ).withColumn(
        "positive_review_rate",
        F.when(
            F.col("review_count") > 0,
            F.round((F.col("positive_reviews") / F.col("review_count")) * 100, 2)
        ).otherwise(0)
    ).withColumn(
        "product_performance_score",
        F.round(
            (F.coalesce(F.col("avg_rating"), F.lit(3)) * 10) +
            (F.col("total_quantity_sold") / 10),
            2
        )
    ).withColumn(
        "inventory_turnover_category",
        F.when(F.col("total_quantity_sold") >= 100, "fast_moving")
         .when(F.col("total_quantity_sold") >= 50, "moderate")
         .when(F.col("total_quantity_sold") >= 10, "slow_moving")
         .otherwise("stagnant")
    )
    
    return df.orderBy(F.col("total_revenue").desc())
