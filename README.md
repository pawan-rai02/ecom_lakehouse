# E-Commerce Data Pipeline

> **Production-Grade DLT Pipeline** | Medallion Architecture | Serverless Databricks | Unity Catalog

## 📋 Table of Contents

* [Overview](#overview)
* [S3 Bucket Structure](#s3-bucket-structure)
* [Architecture](#architecture)
* [Pipeline Configuration](#pipeline-configuration)
* [Data Layers](#data-layers)
  * [Bronze Layer](#bronze-layer-raw-data-ingestion)
  * [Silver Layer](#silver-layer-cleaned-data)
  * [Gold Layer](#gold-layer-analytics-ready)
* [Table Catalog](#table-catalog)
* [Spark Declarative Pipelines Functions](#spark-declarative-pipelines-functions)
* [Data Flow Patterns](#data-flow-patterns)
* [Use Cases](#use-cases)
* [Running the Pipeline](#running-the-pipeline)

---

## Overview

This project implements a **production-grade e-commerce analytics data pipeline** built on **Databricks Spark Declarative Pipelines** (formerly Delta Live Tables). It ingests, cleans, and transforms raw transactional e-commerce data through a **Medallion Architecture** (Bronze → Silver → Gold), powering business intelligence, fraud detection, and customer analytics at scale.

### 🏭 Production-Ready Characteristics

| Characteristic | Implementation |
|---|---|
| **Scalability** | Auto Loader handles 700+ incremental CSV files per entity; designed for millions |
| **Reliability** | Exactly-once streaming semantics, checkpointing, idempotent transformations |
| **Data Quality** | Built-in expectations (warn/drop/fail) at every layer |
| **Governance** | Unity Catalog with fine-grained access control, audit logging |
| **Change Management** | SCD Type 1 & Type 2 patterns for historical tracking |
| **Performance** | Photon-enabled serverless compute, auto-optimize & auto-compact |
| **Observability** | Pipeline graph visualization, table-level metrics, performance profiling |
| **Schema Evolution** | Auto Loader schema inference + additive column evolution |

### Business Value

* **360° Customer View** — Unified customer metrics across orders, sessions, reviews, and payments
* **Product Performance** — Real-time insights into product sales, returns, and ratings
* **Fraud Detection** — Risk scoring, anomaly patterns, and support ticket correlation
* **Operational Analytics** — Shipping performance, delivery delays, logistics optimization

---

## S3 Bucket Structure

Data is stored in Amazon S3 bucket **`ecom-pr1`** under the `raw/` prefix. The pipeline ingests from two distinct load patterns:

```
s3://ecom-pr1/
├── raw/
│   ├── full-load/                    ← One-time historical load
│   │   ├── customers.csv             ← Single CSV per entity
│   │   ├── orders.csv
│   │   ├── order_items.csv
│   │   ├── payments.csv
│   │   ├── products.csv
│   │   ├── reviews.csv
│   │   ├── risk_support.csv
│   │   ├── sessions.csv
│   │   └── shipping.csv
│   │
│   └── incremental_load/             ← Continuous streaming data
│       ├── orders/
│       │   └── staging/              ← 700+ CSVs (same schema)
│       ├── order_items/
│       │   └── staging/              ← 700+ CSVs
│       ├── payments/
│       │   └── staging/              ← 700+ CSVs
│       ├── products/
│       │   └── staging/              ← 700+ CSVs
│       ├── reviews/
│       │   └── staging/              ← 700+ CSVs
│       ├── risk_support/
│       │   └── staging/              ← 700+ CSVs
│       ├── sessions/
│       │   └── staging/              ← 700+ CSVs
│       └── shipping/
│           └── staging/              ← 700+ CSVs
│
└── schema/                           ← Auto Loader schema checkpoints
```

> **⚠️ Critical Detail — Incremental Load Pattern:**  
> Each entity in `incremental_load/` contains a **`staging/`** subfolder holding **700+ CSV files** with the same schema. New files land in these staging folders as they arrive from upstream systems. Auto Loader picks them up via `cloudFiles` streaming reads, deduplicates processed files via checkpoints, and appends to the corresponding bronze staging tables.

### Pipeline Graph

![Pipeline Graph](../images/pipeline.png)

### Pipeline Runtime — Run Complete

![Pipeline Runtime](../images/pipelineRUNTIME.png)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCE DATA (S3)                            │
│                                                                  │
│  ┌──────────────────────┐    ┌───────────────────────────────┐ │
│  │  full-load/           │    │  incremental_load/            │ │
│  │  • customers.csv      │    │  ├── orders/staging/          │ │
│  │  ├── orders.csv       │    │  ├── order_items/staging/     │ │
│  │  ├── order_items.csv  │    │  ├── payments/staging/        │ │
│  │  ├── payments.csv     │    │  ├── products/staging/        │ │
│  │  ├── products.csv     │    │  ├── reviews/staging/         │ │
│  │  ├── reviews.csv      │    │  ├── risk_support/staging/    │ │
│  │  ├── risk_support.csv │    │  ├── sessions/staging/        │ │
│  │  ├── sessions.csv     │    │  └── shipping/staging/        │ │
│  │  └── shipping.csv     │    │   (700+ CSVs each)            │ │
│  └──────────────────────┘    └───────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (RAW)                          │
│  • Auto Loader ingestion from S3                                 │
│  • Schema inference & evolution                                  │
│  • Metadata enrichment (_ingestion_timestamp, _source_file)      │
│  • Staging tables for incremental (cloudFiles streaming)         │
│  • Tables: customers, orders, order_items, payments, products,   │
│            reviews, risk_support, sessions, shipping             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (CLEANED)                        │
│  • Data validation & cleansing                                   │
│  • Type casting & standardization                                │
│  • Derived metrics & surrogate keys                              │
│  • SCD Type 1 (orders, payments, shipping)                       │
│  • SCD Type 2 (products, risk_support)                           │
│  • Tables: *_silver (same entities)                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (ANALYTICS-READY)                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ DIMENSIONS: dim_customers, dim_products, dim_date        │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ FACTS: fact_orders, fact_order_items, fact_sessions,     │   │
│  │        fact_reviews, fact_shipping                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ AGGREGATES: agg_daily_sales, agg_customer_metrics,       │   │
│  │             agg_product_performance                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Configuration

| Setting | Value | Description |
|---------|-------|-------------|
| **Catalog** | `ecom` | Unity Catalog for all tables |
| **Schema** | `silver` | Default schema (can be overridden) |
| **Serverless** | `true` | Uses serverless compute |
| **Photon** | `true` | Accelerated query engine |
| **Channel** | `CURRENT` | Latest stable runtime |
| **Root Path** | `/Workspace/Users/pawanvirat32@gmail.com/e-commerce` | Pipeline source code |
| **Mode** | Triggered | Manual or scheduled runs |
| **Cluster Size** | Serverless (auto-scaling) | No fixed worker count |

---

## Data Layers

### Bronze Layer (Raw Data Ingestion)

**Purpose**: Ingest raw data from S3 with minimal transformation. Acts as the immutable source of truth for all downstream processing.

#### Data Ingestion Strategy

| Load Type | Source | Pattern | Tables |
|-----------|--------|---------|--------|
| **Full Load** | `s3://ecom-pr1/raw/full-load/{table}.csv` | One-time batch backfill (`once=True`) | All 9 tables |
| **Incremental** | `s3://ecom-pr1/raw/incremental_load/{table}/staging/` | Continuous streaming via Auto Loader | 8 tables (all except customers) |

#### Bronze Tables

| Table | Type | Description | Incremental Pattern |
|-------|------|-------------|---------------------|
| `customers_bronze` | Streaming Table | Customer master data | Full load only |
| `orders_bronze` | Streaming Table | Order transactions | Append via staging |
| `order_items_bronze` | Streaming Table | Line items per order | Append via staging |
| `payments_bronze` | Streaming Table | Payment transactions | Upsert via staging |
| `products_bronze` | Streaming Table | Product catalog | SCD Type 2 via staging |
| `reviews_bronze` | Streaming Table | Customer reviews | Append via staging |
| `risk_support_bronze` | Streaming Table | Fraud & support data | SCD Type 2 via staging |
| `sessions_bronze` | Streaming Table | Web analytics sessions | Append via staging |
| `shipping_bronze` | Streaming Table | Shipping & delivery data | Upsert via staging |

#### Incremental Processing Flow

```
S3 staging/ folder (700+ CSVs)
        ↓
Auto Loader (cloudFiles) — reads new files, tracks checkpoints
        ↓
{table}_bronze_staging — streaming table with metadata
        ↓
append_flow → {table}_bronze — writes to final bronze table
```

#### Special Functions Used

**Auto Loader** (`spark.readStream.format("cloudFiles")`):
* Automatically ingests new CSV files from S3 staging folders
* Infers schema from data; supports additive schema evolution
* Tracks processed files via checkpoint to avoid duplicates
* Handles late-arriving data gracefully

**Append Flows** (`@dp.append_flow()`):
* One-time backfill: `once=True` for initial full load from `full-load/`
* Continuous streaming: Regular append flows from `incremental_load/{table}/staging/`

---

### Silver Layer (Cleaned Data)

**Purpose**: Clean, validate, and enrich bronze data into analysis-ready tables. This is where business rules, data quality gates, and type enforcement are applied.

#### Transformation Types

1. **Text Cleaning**: Lowercase, trim whitespace, handle nulls
2. **Type Casting**: Enforce correct data types (string, double, date, timestamp)
3. **Data Validation**: Range checks (ratings 1-5, prices > 0, fraud scores 0-100)
4. **Standardization**: Normalize categorical values (status, sentiment, device types)
5. **Derived Metrics**: Net revenue, loyalty scores, delay calculations, engagement scores
6. **Metadata Cleanup**: Drop internal columns (`_ingestion_timestamp`, `_source_file`, `_load_type`)

#### Incremental Processing Patterns

| Pattern | Tables | Description |
|---------|--------|-------------|
| **Append-Only** | `reviews`, `order_items`, `sessions` | New rows only; no updates to existing data |
| **SCD Type 1 (Upsert)** | `orders`, `payments`, `shipping` | Overwrite existing rows on key match |
| **SCD Type 2 (History)** | `products`, `risk_support` | Preserve full change history with `__start_date` / `__end_date` |

#### Silver Tables

| Table | Description | Key Transformations |
|-------|-------------|---------------------|
| `customers_silver` | Cleaned customer data | Segment standardization, loyalty scoring |
| `orders_silver` | Cleaned order data | Date parsing, status normalization, surrogate keys |
| `order_items_silver` | Cleaned line items | Validation (price > 0), net revenue calculation |
| `payments_silver` | Cleaned payments | Amount validation, fraud score normalization |
| `products_silver` | Cleaned product catalog | Brand/category standardization, SCD Type 2 history |
| `reviews_silver` | Cleaned reviews | Sentiment mapping, rating validation (1-5) |
| `risk_support_silver` | Cleaned risk/support | Fraud score validation (0-100), SCD Type 2 history |
| `sessions_silver` | Cleaned sessions | Device type normalization, engagement metrics |
| `shipping_silver` | Cleaned shipping | Delivery metrics, delay calculations, SCD Type 1 |

---

### Gold Layer (Analytics-Ready)

**Purpose**: Create dimensional models and aggregations optimized for business intelligence dashboards, reporting, and ad-hoc analysis.

#### Dimensions

| Table | Type | Description | Use Case |
|-------|------|-------------|----------|
| `dim_customers` | Materialized View | Customer dimension with SCD Type 2 | Customer segmentation, RFM analysis |
| `dim_products` | Materialized View | Product dimension with SCD Type 2 | Product hierarchy, category analysis |
| `dim_date` | Materialized View | Date dimension | Time-series analysis, seasonality |

#### Facts

| Table | Description | Grain | Key Metrics |
|-------|-------------|-------|-------------|
| `fact_orders` | Order-level transactions | One row per order | Gross/net revenue, shipping cost, fraud risk, item counts |
| `fact_order_items` | Line-item details | One row per product per order | Unit price, discounts, profit margins, quantity |
| `fact_sessions` | Web analytics | One row per session | Duration, pages visited, engagement score, device type |
| `fact_reviews` | Customer feedback | One row per review | Rating, sentiment, alignment score |
| `fact_shipping` | Delivery performance | One row per shipment | Delivery days, cost, delays, performance category |

#### Aggregates

| Table | Aggregation Level | Key Metrics | Use Case |
|-------|------------------|-------------|----------|
| `agg_daily_sales` | Daily | Orders, revenue, customers, fulfillment | Daily dashboards, trend analysis |
| `agg_customer_metrics` | Customer | Lifetime value, order frequency, recency | Customer segmentation, churn prediction |
| `agg_product_performance` | Product | Sales, revenue, ratings, reviews | Inventory planning, merchandising |

---

## Table Catalog

### Complete Table Inventory (36 tables)

```
ecom.silver.*_bronze (9 streaming tables)
  ├── customers_bronze              (full load only)
  ├── orders_bronze                 (full + incremental)
  ├── order_items_bronze            (full + incremental)
  ├── payments_bronze               (full + incremental)
  ├── products_bronze               (full + incremental, SCD2)
  ├── reviews_bronze                (full + incremental)
  ├── risk_support_bronze           (full + incremental, SCD2)
  ├── sessions_bronze               (full + incremental)
  └── shipping_bronze               (full + incremental)

ecom.silver.*_bronze_staging (8 streaming tables — incremental)
  ├── orders_bronze_staging
  ├── order_items_bronze_staging
  ├── payments_bronze_staging
  ├── products_bronze_staging
  ├── reviews_bronze_staging
  ├── risk_support_bronze_staging
  ├── sessions_bronze_staging
  └── shipping_bronze_staging

ecom.silver.*_silver (9 materialized views)
  ├── customers_silver
  ├── orders_silver
  ├── order_items_silver
  ├── payments_silver
  ├── products_silver
  ├── reviews_silver
  ├── risk_support_silver
  ├── sessions_silver
  └── shipping_silver

ecom.silver.dim_* (3 materialized views)
  ├── dim_customers
  ├── dim_products
  └── dim_date

ecom.silver.fact_* (5 materialized views)
  ├── fact_orders
  ├── fact_order_items
  ├── fact_sessions
  ├── fact_reviews
  └── fact_shipping

ecom.silver.agg_* (3 materialized views)
  ├── agg_daily_sales
  ├── agg_customer_metrics
  └── agg_product_performance
```

---

## Spark Declarative Pipelines Functions

### Core Functions Explained

#### 1. Streaming Tables (`@dp.table()`)

**Purpose**: Define streaming tables for continuous data processing

**When to Use**:
* Source is continuously growing (files from S3 staging)
* Need incremental processing with exactly-once semantics
* Append-only or CDC patterns

**Key Features**:
* Stateful processing with automatic checkpointing
* Exactly-once delivery guarantees
* Automatic recovery from failures

---

#### 2. Materialized Views (`@dp.materialized_view()`)

**Purpose**: Define batch-processed tables with full refresh or incremental updates

**When to Use**:
* Silver and Gold layer transformations
* Aggregations across the full dataset
* Joins between multiple source tables

**Key Features**:
* Full table refresh on each triggered run
* Incremental refresh on serverless (automatic)
* Optimized for complex aggregations and joins

---

#### 3. Auto Loader (`spark.readStream.format("cloudFiles")`)

**Purpose**: Automatically ingest files from cloud storage (S3)

**Configuration**:
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv")        # File format
    .option("cloudFiles.schemaLocation", path) # Schema checkpoint
    .option("header", "true")                   # CSV has header row
    .option("cloudFiles.inferColumnTypes", "true") # Type inference
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # Schema evolution
    .load("s3://ecom-pr1/raw/incremental_load/{table}/staging/")
```

**Key Features**:
* **Schema Inference**: Automatically detects column types from CSV data
* **Schema Evolution**: Handles new columns gracefully (additive mode)
* **File Tracking**: Remembers processed files via checkpoints; never reprocesses
* **Scalability**: Optimized for millions of small files

---

#### 4. Append Flows (`@dp.append_flow()`)

**Purpose**: Append data to target tables (streaming → streaming)

**Types**:

**One-Time Backfill**:
```python
@dp.append_flow(
    target="orders_bronze",
    name="orders_full_load",
    once=True  # Runs once per pipeline trigger, then stops
)
def orders_full_load():
    return read_full("orders")
```

**Continuous Streaming**:
```python
@dp.append_flow(
    target="orders_bronze",
    name="orders_incremental_append"
)
def orders_incremental():
    return spark.readStream.table("orders_bronze_staging")
```

**Key Features**:
* Combines multiple sources into one target table
* Ideal for backfill + live data patterns
* Maintains streaming semantics end-to-end

---

#### 5. Data Quality Expectations

**Purpose**: Enforce data quality rules with automated monitoring and alerting

**Types**:

| Expectation | Behavior | Use Case |
|-------------|----------|----------|
| `@dp.expect()` | Warn — logs violation count | Non-critical checks |
| `@dp.expect_or_drop()` | Drop — filters out invalid rows | Invalid data that should be excluded |
| `@dp.expect_or_fail()` | Fail — stops pipeline execution | Critical constraint violations |

**Example**:
```python
@dp.table(name="orders_clean")
@dp.expect_or_drop("valid_total", "total_price_usd > 0")
@dp.expect("has_customer", "customer_id IS NOT NULL")
def orders_clean():
    return spark.read.table("orders_bronze")
```

---

## Data Flow Patterns

### 1. Full Load + Incremental Pattern

**Use Case**: Initial historical load followed by continuous streaming updates from S3 staging folders.

```python
# Step 1: One-time full load (backfill)
@dp.append_flow(target="orders_bronze", name="orders_full_load", once=True)
def orders_full():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("s3://ecom-pr1/raw/full-load/orders.csv")

# Step 2: Continuous incremental from staging
@dp.append_flow(target="orders_bronze", name="orders_incremental_append")
def orders_incremental():
    return spark.readStream.table("orders_bronze_staging")
```

---

### 2. Staging → Bronze Pattern (Incremental)

**Use Case**: Auto Loader reads 700+ CSVs from S3 staging, creates a streaming staging table, then appends to the final bronze table.

```python
# Step 1: Staging table from S3
@dp.table(name="orders_bronze_staging")
def orders_staging():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load("s3://ecom-pr1/raw/incremental_load/orders/staging/") \
        .withColumn("source_file", F.col("_metadata.file_path")) \
        .withColumn("ingestion_timestamp", F.current_timestamp())

# Step 2: Append to bronze
@dp.append_flow(target="orders_bronze", name="orders_incremental_append")
def orders_append():
    return spark.readStream.table("orders_bronze_staging")
```

---

### 3. Join Pattern (Facts)

**Use Case**: Enrich fact tables with dimension data for analytics.

```python
@dp.materialized_view(name="fact_orders")
def fact_orders():
    orders = spark.read.table("orders_silver")
    customers = spark.read.table("dim_customers")

    return orders.join(customers, "customer_id", "left")
```

---

### 4. Aggregation Pattern

**Use Case**: Roll up metrics to higher grain for dashboards.

```python
@dp.materialized_view(name="agg_daily_sales")
def agg_daily_sales():
    return spark.read.table("fact_orders") \
        .groupBy("order_date") \
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_net_revenue").alias("total_revenue")
        )
```

---

## Use Cases

### 1. Customer Lifetime Value (CLV) Analysis

```sql
SELECT
    customer_id,
    lifetime_value,
    total_orders,
    avg_order_value,
    customer_value_segment
FROM ecom.silver.agg_customer_metrics
WHERE customer_value_segment = 'high_value'
ORDER BY lifetime_value DESC;
```

---

### 2. Product Performance Dashboard

```sql
SELECT
    product_id,
    total_revenue,
    total_quantity_sold,
    avg_rating,
    positive_review_rate,
    inventory_turnover_category
FROM ecom.silver.agg_product_performance
WHERE inventory_turnover_category = 'fast_moving'
ORDER BY total_revenue DESC
LIMIT 50;
```

---

### 3. Daily Sales Tracking

```sql
SELECT
    order_date,
    total_orders,
    unique_customers,
    net_revenue,
    avg_order_value,
    completion_rate,
    on_time_delivery_rate
FROM ecom.silver.agg_daily_sales
ORDER BY order_date DESC
LIMIT 30;
```

---

### 4. Fraud Detection

```sql
SELECT
    o.order_id,
    o.customer_id,
    o.fraud_risk_score,
    o.fraud_risk_category,
    r.is_high_risk,
    r.support_ticket_created
FROM ecom.silver.fact_orders o
LEFT JOIN ecom.silver.risk_support_silver r ON o.order_id = r.order_id
WHERE o.fraud_risk_category = 'high'
ORDER BY o.fraud_risk_score DESC;
```

---

### 5. Shipping Performance

```sql
SELECT
    shipping_method,
    COUNT(*) as shipment_count,
    AVG(delivery_days) as avg_delivery_days,
    AVG(shipping_cost_usd) as avg_cost,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) as delayed_count,
    ROUND(AVG(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) * 100, 2) as delay_rate_pct
FROM ecom.silver.fact_shipping
GROUP BY shipping_method
ORDER BY shipment_count DESC;
```

---

## Running the Pipeline

### Start an Update

```bash
# Via Databricks UI
# Workflows → Delta Live Tables → e-commerce → Start

# Via CLI
databricks pipelines start --pipeline-id a5e7e6b9-8fac-4937-a463-e6cd989873a4
```

### Refresh Specific Tables

```bash
# Refresh only silver layer
databricks pipelines start \
    --pipeline-id a5e7e6b9-8fac-4937-a463-e6cd989873a4 \
    --refresh-selection "ecom.silver.*_silver"
```

### Monitor Pipeline

```bash
# Check status
databricks pipelines get --pipeline-id a5e7e6b9-8fac-4937-a463-e6cd989873a4

# View recent updates
databricks pipelines list-updates --pipeline-id a5e7e6b9-8fac-4937-a463-e6cd989873a4
```

---

## Project Structure

```
e-commerce_dltPipeline/
│
├── images/                              ← Pipeline screenshots
│   ├── pipeline.png                     ← Full pipeline graph (Bronze → Silver → Gold)
│   └── pipelineRUNTIME.png              ← Runtime view — 36 tables, Run complete
│
├── e-commerce/                          ← Pipeline source code
│   ├── README.md                        ← This file
│   │
│   ├── explorations/                    ← Data profiling scripts
│   │   ├── customers_exploration.py
│   │   ├── orders_exploration.py
│   │   ├── order_items_exploration.py
│   │   └── products_exploration.py
│   │
│   └── transformations/
│       ├── 00_common/
│       │   ├── config.py                ← Table configs (keys, patterns)
│       │   └── paths.py                 ← S3 path helpers
│       │
│       ├── 01_bronze/
│       │   ├── full_load/
│       │   │   └── 01_full_load.py      ← Initial CSV backfill (all 9 tables)
│       │   └── incremental/
│       │       ├── 01_staging.py        ← Staging tables from S3 cloudFiles
│       │       └── 02_append.py         ← Append flows to bronze tables
│       │
│       ├── 02_silver/
│       │   ├── 01_orders_silver.py
│       │   ├── 02_customers_silver.py
│       │   ├── 03_order_items_silver.py
│       │   ├── 04_products_silver.py
│       │   ├── 05_payments_silver.py
│       │   ├── 06_reviews_silver.py
│       │   ├── 07_risk_support_silver.py
│       │   ├── 08_sessions_silver.py
│       │   └── 09_shipping_silver.py
│       │
│       └── 03_gold/
│           ├── dimensions/
│           │   ├── 01_dim_customers.py
│           │   ├── 02_dim_products.py
│           │   └── 03_dim_date.py
│           ├── facts/
│           │   ├── 01_fact_orders.py
│           │   ├── 02_fact_order_items.py
│           │   ├── 03_fact_sessions.py
│           │   ├── 04_fact_reviews.py
│           │   └── 05_fact_shipping.py
│           └── aggregates/
│               ├── 01_agg_daily_sales.py
│               ├── 02_agg_customer_metrics.py
│               └── 03_agg_product_performance.py
```

---

## Key Technologies

| Technology | Role |
|------------|------|
| **Databricks** | Unified analytics platform |
| **Spark Declarative Pipelines** | Declarative ETL framework (formerly Delta Live Tables) |
| **Delta Lake** | ACID transactions, time travel, schema enforcement |
| **Auto Loader (cloudFiles)** | Incremental file ingestion from S3 |
| **Unity Catalog** | Unified governance, access control, audit logging |
| **Photon** | Native vectorized query engine for performance |
| **Serverless** | Instant compute, auto-scaling, zero cluster management |

---

## Best Practices Applied

✅ **Medallion Architecture** — Clear separation of raw → clean → analytics  
✅ **Idempotency** — All transformations are repeatable and deterministic  
✅ **Data Quality** — Expectations (warn/drop/fail) at every layer  
✅ **Incremental Processing** — Auto Loader + streaming for 700+ file ingestion  
✅ **Schema Evolution** — Additive column handling via cloudFiles  
✅ **SCD Type 1 & Type 2** — Historical change tracking for products, risk_support  
✅ **Surrogate Keys** — Generated for dimensions and fact tables  
✅ **Derived Metrics** — Business logic embedded in transformations  
✅ **Documentation** — Inline comments, table comments, comprehensive README  
✅ **Production Hardening** — Photon, serverless, auto-optimize, auto-compact  

---

## Future Enhancements

* [ ] Add Change Data Capture (CDC) for orders and payments
* [ ] Implement real-time alerting on fraud scores
* [ ] Add ML models for churn prediction
* [ ] Implement data retention policies
* [ ] Add data masking for PII fields
* [ ] Create Lakeview dashboards
* [ ] Set up monitoring and alerting
* [ ] Add unit tests for transformations

---

## Contact & Support

* **Owner**: pawanvirat32@gmail.com
* **Pipeline ID**: `a5e7e6b9-8fac-4937-a463-e6cd989873a4`
* **Workspace**: Databricks
* **Catalog**: `ecom`
* **S3 Bucket**: `s3://ecom-pr1/`

---

**Last Updated**: April 11, 2026  
**Version**: 1.0  
**Status**: ✅ Production Ready
