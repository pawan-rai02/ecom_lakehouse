from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit, col

# ============================================================
# 📌 PATH CONFIG
# ============================================================

# Full load - single CSV per table
DBFS_FULL = "s3://ecom-pr1/raw/full-load/"

# ============================================================
# 📌 TABLE CONFIG
# ============================================================

# Tables with both full + incremental data
INCREMENTAL_TABLES = [
    "orders",
    "order_items",
    "payments",
    "products",
    "reviews",
    "risk_support",
    "sessions",
    "shipping"
]

# Tables with only full load (no incremental updates)
FULL_LOAD_ONLY_TABLES = [
    "customers"
]

# All tables in the system
ALL_TABLES = FULL_LOAD_ONLY_TABLES + INCREMENTAL_TABLES

# ============================================================
# 📌 HELPER: FULL LOAD (Batch CSV read)
# ============================================================

def read_full(table_name: str):
    """
    Reads full-load CSV using batch read.
    Used with once=True append flows for one-time backfill.
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        
        # Input path - specific CSV file
        .load(f"{DBFS_FULL}{table_name}.csv")

        # Metadata columns
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_type", lit("full_load"))
    )


# ============================================================
# 📌 STEP 1 — CREATE BRONZE TABLES (ALL TABLES)
# ============================================================

# Table properties (optional but good practice)
TABLE_PROPS = {
    "quality": "bronze",

    # Optimize write performance
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",

    # Enable Change Data Feed (needed for Silver CDC later)
    "delta.enableChangeDataFeed": "true"
}

# Create Bronze tables for ALL tables
# This ensures silver layer can reference them even before incremental data arrives
for tbl in ALL_TABLES:
    dp.create_streaming_table(
        name=f"{tbl}_bronze",
        comment=f"Bronze raw table for {tbl}",
        table_properties=TABLE_PROPS
    )


# ============================================================
# 📌 STEP 2 — FULL LOAD FLOW (One-time backfill with once=True)
# ============================================================

# Only create full load flows for ALL tables
# This performs the initial backfill from the full-load CSVs
for tbl in ALL_TABLES:

    def full_flow(table_name=tbl):
        """
        Each table gets its own full-load flow.
        Runs once per pipeline trigger (backfill pattern).
        """
        @dp.append_flow(
            target=f"{table_name}_bronze",
            name=f"{table_name}_full_load",
            comment=f"Initial full load for {table_name}",
            once=True  # This makes it a backfill flow
        )
        def flow():
            return read_full(table_name)

    full_flow()


# ============================================================
# 📌 NOTES ON INCREMENTAL PROCESSING
# ============================================================
#
# The incremental/*.py files contain the incremental CDC patterns
# but are currently commented out because:
#
# 1. The incremental S3 folders are empty
# 2. Schema inference fails on empty directories
#
# Once you have files in s3://ecom-pr1/raw/incremental_load/{table}/,
# uncomment the code in incremental/*.py files:
#
# - 01_staging.py: Creates staging tables from incremental CSVs
# - 02_append.py: Append-only pattern (reviews, order_items, sessions)
# - 03_upsert.py: SCD Type 1 upserts (orders, payments, shipping)
# - 04_scd2.py: SCD Type 2 history tracking (products, risk_support)
#
# The bronze tables already exist, so incremental flows can write to them
# when activated.
#
