from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
import sys
import importlib.util

sys.path.append("/Workspace/Users/pawanvirat32@gmail.com/e-commerce")

spec = importlib.util.spec_from_file_location("paths", "/Workspace/Users/pawanvirat32@gmail.com/e-commerce/transformations/00_common/paths.py")
paths_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(paths_module)
get_staging_path = paths_module.get_staging_path

INCREMENTAL_TABLES = [
    "orders", "order_items", "payments",
    "reviews", "risk_support", "sessions", "shipping"
]


def create_bronze_staging_table(table_name):
    @dp.table(name=f"{table_name}_bronze_staging")
    def table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(get_staging_path(table_name))
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("ingestion_timestamp", current_timestamp())
        )


for tbl in INCREMENTAL_TABLES:
    create_bronze_staging_table(tbl)
