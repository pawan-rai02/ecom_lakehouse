from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fact_reviews",
    comment="Review fact table with product feedback and sentiment analysis"
)
def fact_reviews():
    reviews = spark.read.table("reviews_silver")
    
    # Rating category
    df = reviews.withColumn(
        "rating_category",
        F.when(F.col("rating") == 5, "excellent")
         .when(F.col("rating") == 4, "good")
         .when(F.col("rating") == 3, "average")
         .when(F.col("rating") == 2, "poor")
         .when(F.col("rating") == 1, "very_poor")
         .otherwise("not_rated")
    )
    
    # Review recency
    df = df.withColumn(
        "days_since_review",
        F.datediff(F.current_date(), F.col("order_date"))
    )
    
    # Sentiment alignment with rating
    df = df.withColumn(
        "sentiment_rating_aligned",
        F.when(
            ((F.col("rating") >= 4) & (F.col("sentiment") == "positive")) |
            ((F.col("rating") == 3) & (F.col("sentiment") == "neutral")) |
            ((F.col("rating") <= 2) & (F.col("sentiment") == "negative")),
            1
        ).otherwise(0)
    )
    
    return df.select(
        # Keys
        "order_id",
        "product_id",
        "customer_id",
        "order_date",
        
        # Rating metrics
        "rating",
        "rating_category",
        
        # Sentiment
        "sentiment",
        "is_positive",
        "is_negative",
        
        # Review attributes
        "customer_feedback",
        "days_since_review",
        "sentiment_rating_aligned"
    )
