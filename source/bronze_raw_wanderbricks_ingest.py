# Databricks notebook source
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

# DBTITLE 1,Ingest amenities
df = spark.table("samples.wanderbricks.amenities")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_amenities")
print(f"Ingested amenities: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest booking_updates
df = spark.table("samples.wanderbricks.booking_updates")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_booking_updates")
print(f"Ingested booking_updates: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest bookings
df = spark.table("samples.wanderbricks.bookings")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_bookings")
print(f"Ingested bookings: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest clickstream
df = spark.table("samples.wanderbricks.clickstream")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_clickstream")
print(f"Ingested clickstream: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest countries
df = spark.table("samples.wanderbricks.countries")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_countries")
print(f"Ingested countries: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest customer_support_logs
df = spark.table("samples.wanderbricks.customer_support_logs")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_customer_support_logs")
print(f"Ingested customer_support_logs: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest destinations
df = spark.table("samples.wanderbricks.destinations")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_destinations")
print(f"Ingested destinations: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest employees
df = spark.table("samples.wanderbricks.employees")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_employees")
print(f"Ingested employees: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest hosts
df = spark.table("samples.wanderbricks.hosts")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_hosts")
print(f"Ingested hosts: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest page_views
df = spark.table("samples.wanderbricks.page_views")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_page_views")
print(f"Ingested page_views: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest payments
df = spark.table("samples.wanderbricks.payments")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_payments")
print(f"Ingested payments: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest properties
df = spark.table("samples.wanderbricks.properties")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_properties")
print(f"Ingested properties: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest property_amenities
df = spark.table("samples.wanderbricks.property_amenities")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_property_amenities")
print(f"Ingested property_amenities: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest property_images
df = spark.table("samples.wanderbricks.property_images")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_property_images")
print(f"Ingested property_images: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest reviews
df = spark.table("samples.wanderbricks.reviews")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_reviews")
print(f"Ingested reviews: {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Ingest users
df = spark.table("samples.wanderbricks.users")
row_count = df.count()
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.bronze_raw.wanderbricks_users")
print(f"Ingested users: {row_count} rows")
