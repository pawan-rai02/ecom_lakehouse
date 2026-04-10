from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fact_sessions",
    comment="Session fact table with web analytics and customer behavior metrics"
)
def fact_sessions():
    sessions = spark.read.table("sessions_silver")
    
    # Session engagement score (based on duration and pages)
    df = sessions.withColumn(
        "engagement_score",
        F.round(
            (F.col("session_duration_minutes") * 2) + 
            (F.col("pages_visited") * 5),
            2
        )
    )
    
    # Session quality classification
    df = df.withColumn(
        "session_quality",
        F.when(F.col("engagement_score") >= 50, "high")
         .when(F.col("engagement_score") >= 20, "medium")
         .otherwise("low")
    )
    
    return df.select(
        # Keys
        "order_id",
        "customer_id",
        
        # Duration metrics
        "session_duration_minutes",
        
        # Engagement metrics
        "pages_visited",
        "engagement_score",
        "session_quality",
        
        # Device and traffic attributes
        "device_type",
        "is_mobile",
        "traffic_source",
        "campaign_source",
        
        # Conversion indicator
        "abandoned_cart_before"
    )
