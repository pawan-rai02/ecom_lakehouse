from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

@dp.materialized_view(
    name="dim_date",
    comment="Date dimension table with calendar attributes for analytics"
)
def dim_date():
    # Get unique dates from orders_silver
    orders = spark.read.table("orders_silver")
    
    # Extract unique dates
    dates_df = orders.select(F.col("order_date").alias("date")).distinct()
    
    # Add date attributes
    df = dates_df.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("year", F.year("date")) \
        .withColumn("quarter", F.quarter("date")) \
        .withColumn("month", F.month("date")) \
        .withColumn("month_name", F.date_format("date", "MMMM")) \
        .withColumn("week_of_year", F.weekofyear("date")) \
        .withColumn("day_of_month", F.dayofmonth("date")) \
        .withColumn("day_of_week", F.dayofweek("date")) \
        .withColumn("day_name", F.date_format("date", "EEEE"))
    
    # Weekend flag
    df = df.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)
    )
    
    # Weekday/Weekend label
    df = df.withColumn(
        "day_type",
        F.when(F.col("is_weekend") == 1, "weekend").otherwise("weekday")
    )
    
    # Quarter name
    df = df.withColumn(
        "quarter_name",
        F.concat(F.lit("Q"), F.col("quarter"))
    )
    
    # Year-Month
    df = df.withColumn(
        "year_month",
        F.date_format("date", "yyyy-MM")
    )
    
    # Year-Quarter
    df = df.withColumn(
        "year_quarter",
        F.concat(F.col("year"), F.lit("-"), F.col("quarter_name"))
    )
    
    # Month start/end dates
    df = df.withColumn(
        "month_start_date",
        F.trunc("date", "month")
    ).withColumn(
        "month_end_date",
        F.last_day("date")
    )
    
    return df.select(
        "date",
        "date_key",
        "year",
        "quarter",
        "quarter_name",
        "year_quarter",
        "month",
        "month_name",
        "year_month",
        "week_of_year",
        "day_of_month",
        "day_of_week",
        "day_name",
        "is_weekend",
        "day_type",
        "month_start_date",
        "month_end_date"
    )
