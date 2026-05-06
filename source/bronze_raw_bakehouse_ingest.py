# Databricks notebook source
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

# DBTITLE 1,Ingest media_customer_reviews
df = spark.table("samples.bakehouse.media_customer_reviews")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.bakehouse_media_customer_reviews")
print(f"Ingested media_customer_reviews: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest media_gold_reviews_chunked
df = spark.table("samples.bakehouse.media_gold_reviews_chunked")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.bakehouse_media_gold_reviews_chunked")
print(f"Ingested media_gold_reviews_chunked: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest sales_customers
df = spark.table("samples.bakehouse.sales_customers")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.bakehouse_sales_customers")
print(f"Ingested sales_customers: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest sales_franchises
df = spark.table("samples.bakehouse.sales_franchises")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.bakehouse_sales_franchises")
print(f"Ingested sales_franchises: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest sales_suppliers
df = spark.table("samples.bakehouse.sales_suppliers")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.bakehouse_sales_suppliers")
print(f"Ingested sales_suppliers: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest sales_transactions
df = spark.table("samples.bakehouse.sales_transactions")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.bakehouse_sales_transactions")
print(f"Ingested sales_transactions: {row_count} rows")
