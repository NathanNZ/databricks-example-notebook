# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bakehouse Data Ingestion to Bronze Layer
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook ingests all tables from the `samples.bakehouse` schema into the bronze layer.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC * **Widget**: `catalog_name` (default: "workspace") - The target catalog for bronze tables
# MAGIC * **Target Schema**: `{catalog_name}.bronze_raw`
# MAGIC * **Source Schema**: `samples.bakehouse`
# MAGIC
# MAGIC ## Tables Ingested
# MAGIC This notebook ingests the following 6 tables:
# MAGIC 1. `media_customer_reviews` → `bakehouse_media_customer_reviews`
# MAGIC 2. `media_gold_reviews_chunked` → `bakehouse_media_gold_reviews_chunked`
# MAGIC 3. `sales_customers` → `bakehouse_sales_customers`
# MAGIC 4. `sales_franchises` → `bakehouse_sales_franchises`
# MAGIC 5. `sales_suppliers` → `bakehouse_sales_suppliers`
# MAGIC 6. `sales_transactions` → `bakehouse_sales_transactions`
# MAGIC
# MAGIC ## Process
# MAGIC Each table is:
# MAGIC 1. Read from the source using `spark.table()`
# MAGIC 2. Row count is captured before writing
# MAGIC 3. Written to the bronze layer with `overwrite` mode
# MAGIC 4. Prefixed with `bakehouse_` to identify the source schema

# COMMAND ----------

# DBTITLE 1,Setup widget
# Create widget for catalog name
dbutils.widgets.text("catalog_name", "workspace", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# DBTITLE 1,Create bronze_raw schema
# Create bronze_raw schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze_raw")

# COMMAND ----------

# DBTITLE 1,Define ingestion function
from common.table_utils import get_source_table, get_target_table

def ingest_table(table_name):
    """
    Ingest a table from samples.bakehouse into the bronze layer.
    
    Args:
        table_name: Name of the table in samples.bakehouse schema
    
    Returns:
        int: Number of rows ingested
    """
    source_table = get_source_table("bakehouse", table_name)
    target_table = get_target_table(catalog_name, "bronze_raw", "bakehouse", table_name)
    
    # Read from source using spark.sql
    df = spark.sql(f"SELECT * FROM {source_table}")
    row_count = df.count()
    
    # Write to target
    df.write.mode("overwrite").saveAsTable(target_table)
    
    return row_count

# COMMAND ----------

# DBTITLE 1,Define table names array
# Array of table names to ingest from samples.bakehouse
table_names = [
    "media_customer_reviews",
    "media_gold_reviews_chunked",
    "sales_customers",
    "sales_franchises",
    "sales_suppliers",
    "sales_transactions"
]

# COMMAND ----------

# DBTITLE 1,Ingest all tables
# Loop through each table and ingest
for table_name in table_names:
    row_count = ingest_table(table_name)
    print(f"Ingested {table_name}: {row_count} rows")

print(f"\nCompleted ingestion of {len(table_names)} tables from samples.bakehouse to {catalog_name}.bronze_raw")

# COMMAND ----------

# DBTITLE 1,Show all tables in bronze_raw
# MAGIC %sql
# MAGIC SHOW TABLES IN IDENTIFIER(:catalog_name).bronze_raw
