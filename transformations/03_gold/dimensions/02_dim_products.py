from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="dim_products",
    comment="Product dimension table with attributes and classifications"
)
def dim_products():
    df = spark.read.table("products_silver")
    
    # Price tier classification
    df = df.withColumn(
        "price_tier",
        F.when(F.col("unit_price_usd") < 20, "budget")
         .when(F.col("unit_price_usd") < 50, "economy")
         .when(F.col("unit_price_usd") < 100, "standard")
         .when(F.col("unit_price_usd") < 200, "premium")
         .otherwise("luxury")
    )
    
    # Rating classification
    df = df.withColumn(
        "rating_category",
        F.when(F.col("product_rating_avg") >= 4.5, "excellent")
         .when(F.col("product_rating_avg") >= 4.0, "very_good")
         .when(F.col("product_rating_avg") >= 3.5, "good")
         .when(F.col("product_rating_avg") >= 3.0, "average")
         .when(F.col("product_rating_avg").isNotNull(), "below_average")
         .otherwise("not_rated")
    )
    
    # Stock availability status
    df = df.withColumn(
        "stock_status",
        F.when(F.col("stock_quantity") == 0, "out_of_stock")
         .when(F.col("stock_quantity") < 10, "low_stock")
         .when(F.col("stock_quantity") < 50, "in_stock")
         .otherwise("high_stock")
    )
    
    return df.select(
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "brand",
        "unit_price_usd",
        "price_tier",
        "product_rating_avg",
        "rating_category",
        "product_reviews_count",
        "stock_quantity",
        "stock_status"
    )
