from pyspark import pipelines as dp

INCREMENTAL_TABLES = [
    "orders", "order_items", "payments",
    "reviews", "risk_support", "sessions", "shipping"
]


def create_append_flow(table_name):
    @dp.append_flow(
        target=f"{table_name}_bronze",
        name=f"{table_name}_incremental_append"
    )
    def flow():
        return spark.readStream.table(f"{table_name}_bronze_staging")


for tbl in INCREMENTAL_TABLES:
    create_append_flow(tbl)
