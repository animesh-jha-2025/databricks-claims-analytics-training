# Databricks notebook source
# MAGIC %md
# MAGIC # (Optional) How data gets here: a 10-minute ETL overview
# MAGIC **Self-study, or use it if there's time.** You don't build pipelines in your day-to-day work, but knowing the layers explains *where* tables come from and *which* layer to query.
# MAGIC
# MAGIC ```
# MAGIC  files land (CSV)      →   BRONZE (raw)         →   SILVER (cleaned)          →   GOLD (business-ready)
# MAGIC  /Volumes/.../landing      bronze_rx_claims         silver_rx_claims             gold_rx_monthly
# MAGIC                            as-is + load time        typed, deduped, valid        aggregated for reports
# MAGIC ```
# MAGIC **Analysts usually query Silver and Gold.** Bronze is for troubleshooting ("what exactly did the file contain?").

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: simulate a file delivery
# MAGIC We write a CSV file into a **volume** (governed file storage in Unity Catalog), the way a source system would.

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS landing")
landing = f"/Volumes/{CATALOG}/{SCHEMA}/landing/rx_claims"
(spark.table("pharmacy_claims").where("fill_date >= '2025-07-01'")
     .withColumn("paid_amount", F.col("paid_amount").cast("string"))   # files arrive as text
     .write.mode("overwrite").option("header", True).csv(landing))
display(dbutils.fs.ls(landing)[:5])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: load the files as they are

# COMMAND ----------

bronze = (spark.read.option("header", True).csv(landing)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_loaded_at", F.current_timestamp()))
bronze.write.mode("overwrite").saveAsTable("bronze_rx_claims")
print(f"bronze_rx_claims: {spark.table('bronze_rx_claims').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: fix types, drop duplicates, keep valid rows

# COMMAND ----------

silver = (spark.table("bronze_rx_claims")
          .withColumn("fill_date", F.to_date("fill_date"))
          .withColumn("paid_amount", F.col("paid_amount").cast("double"))
          .withColumn("days_supply", F.col("days_supply").cast("int"))
          .filter(F.col("claim_status") == "Paid")
          .dropDuplicates(["member_id", "ndc", "pharmacy_id", "fill_date"]))
silver.write.mode("overwrite").saveAsTable("silver_rx_claims")
print(f"silver_rx_claims: {spark.table('silver_rx_claims').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: aggregate for reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_rx_monthly AS
# MAGIC SELECT date_trunc('MONTH', s.fill_date) AS month, d.therapeutic_class,
# MAGIC        count(*) AS fills, round(sum(s.paid_amount), 0) AS paid_amount
# MAGIC FROM silver_rx_claims s JOIN drugs d USING (ndc)
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT * FROM gold_rx_monthly ORDER BY month, paid_amount DESC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## In production
# MAGIC Your platform team builds this kind of flow with **Lakeflow Declarative Pipelines** and **Auto Loader**, which pick up only new files, enforce data-quality rules (*expectations*), and track lineage. Open `gold_rx_monthly` in Catalog Explorer → **Lineage** to see how it traces back to bronze.
