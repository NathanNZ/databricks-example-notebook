# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Wanderbricks Data Ingestion to Bronze Layer
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook ingests all tables from the `samples.wanderbricks` schema into the bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC * **Widget**: `catalog_name` (default: "workspace") - The target catalog for bronze tables
# MAGIC * **Target Schema**: `{catalog_name}.bronze_raw`
# MAGIC * **Source Schema**: `samples.wanderbricks`
# MAGIC
# MAGIC ## Tables Ingested
# MAGIC This notebook ingests the following 16 tables:
# MAGIC 1. `amenities` → `wanderbricks_amenities`
# MAGIC 2. `booking_updates` → `wanderbricks_booking_updates`
# MAGIC 3. `bookings` → `wanderbricks_bookings`
# MAGIC 4. `clickstream` → `wanderbricks_clickstream`
# MAGIC 5. `countries` → `wanderbricks_countries`
# MAGIC 6. `customer_support_logs` → `wanderbricks_customer_support_logs`
# MAGIC 7. `destinations` → `wanderbricks_destinations`
# MAGIC 8. `employees` → `wanderbricks_employees`
# MAGIC 9. `hosts` → `wanderbricks_hosts`
# MAGIC 10. `page_views` → `wanderbricks_page_views`
# MAGIC 11. `payments` → `wanderbricks_payments`
# MAGIC 12. `properties` → `wanderbricks_properties`
# MAGIC 13. `property_amenities` → `wanderbricks_property_amenities`
# MAGIC 14. `property_images` → `wanderbricks_property_images`
# MAGIC 15. `reviews` → `wanderbricks_reviews`
# MAGIC 16. `users` → `wanderbricks_users`
# MAGIC
# MAGIC ## Process
# MAGIC Each table is:
# MAGIC 1. Read from the source using `spark.table()`
# MAGIC 2. Row count is captured before writing
# MAGIC 3. Written to the bronze layer with `overwrite` mode
# MAGIC 4. Prefixed with `wanderbricks_` to identify the source schema

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
    Ingest a table from samples.wanderbricks into the bronze layer.
    
    Args:
        table_name: Name of the table in samples.wanderbricks schema
    
    Returns:
        int: Number of rows ingested
    """
    source_table = get_source_table("wanderbricks", table_name)
    target_table = get_target_table(catalog_name, "bronze_raw", "wanderbricks", table_name)
    
    # Read from source using SQL
    df = spark.sql(f"SELECT * FROM {source_table}")
    row_count = df.count()
    
    # Write to target
    df.write.mode("overwrite").saveAsTable(target_table)
    
    return row_count

# COMMAND ----------

# DBTITLE 1,Define table names array
# Array of table names to ingest from samples.wanderbricks
table_names = [
    "amenities",
    "booking_updates",
    "bookings",
    "clickstream",
    "countries",
    "customer_support_logs",
    "destinations",
    "employees",
    "hosts",
    "page_views",
    "payments",
    "properties",
    "property_amenities",
    "property_images",
    "reviews",
    "users"
]

# COMMAND ----------

# DBTITLE 1,Ingest all tables
# Loop through each table and ingest
for table_name in table_names:
    row_count = ingest_table(table_name)
    print(f"Ingested {table_name}: {row_count} rows")

print(f"\nCompleted ingestion of {len(table_names)} tables from samples.wanderbricks to {catalog_name}.bronze_raw")

# COMMAND ----------

# DBTITLE 1,Show all tables in bronze_raw
# MAGIC %sql
# MAGIC SHOW TABLES IN IDENTIFIER(:catalog_name).bronze_raw
