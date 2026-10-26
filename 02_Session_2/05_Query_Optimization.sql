-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Query optimization: find where the time goes
-- MAGIC **Session 2 · about 30 minutes**
-- MAGIC
-- MAGIC **The goal is to learn to diagnose, not to memorize settings.** On serverless, Databricks handles most tuning for you. Your biggest wins come from **how you write the query** and **how the data is laid out**.
-- MAGIC
-- MAGIC **How to open a query profile:**
-- MAGIC * **Notebook:** after a cell runs, click **See performance** under the result → choose the query → **Query profile**.
-- MAGIC * **SQL Editor:** run the query → click **See query profile** below the results.
-- MAGIC * **History:** left sidebar → **Query History** → click any query.
-- MAGIC
-- MAGIC **What to look at first:**
-- MAGIC | Metric | What it tells you |
-- MAGIC |---|---|
-- MAGIC | **Total wall-clock time** | Overall duration, and how much of it was spent waiting vs. working |
-- MAGIC | **Bytes read / files read / files pruned** | Did we read only what we needed? |
-- MAGIC | **Rows** going into and out of each operator | Did a join **blow up** the row count? |
-- MAGIC | **The operator with the most time** | Is the slow part the scan, the join, the aggregation, or the sort? |
-- MAGIC
-- MAGIC > Uses `medical_claims_large` (2M rows), created by the setup notebook.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "workspace", "Catalog")
-- MAGIC dbutils.widgets.text("schema", "claims_training", "Schema")
-- MAGIC spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern 1: read only the columns you need
-- MAGIC Delta tables store data **by column**, so naming the columns you need means less data to read.
-- MAGIC
-- MAGIC Run both cells, then compare **bytes read** in each query profile.

-- COMMAND ----------

-- ❌ Reads every column
SELECT * FROM medical_claims_large WHERE dx_code = 'E11.9'

-- COMMAND ----------

-- ✅ Reads 3 columns
SELECT claim_id, service_date, paid_amount FROM medical_claims_large WHERE dx_code = 'E11.9'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern 2: watch for exploding joins
-- MAGIC The most common reason an analyst's query is slow is joining on a key that **isn't unique** on one side. Every claim matches many rows, so the row count multiplies.
-- MAGIC
-- MAGIC `file_deliveries` has **many rows per client**. Joining claims to it on `client_id` alone multiplies each claim by about 17.

-- COMMAND ----------

-- ❌ Join on a non-unique key: check "rows output" of the join in the profile
SELECT count(*) AS rows_after_join
FROM medical_claims_large c
JOIN file_deliveries f ON c.client_id = f.client_id

-- COMMAND ----------

-- ✅ Join on the full key: client and month identify exactly one delivery
SELECT count(*) AS rows_after_join
FROM medical_claims_large c
JOIN file_deliveries f
  ON  c.client_id = f.client_id
  AND date_trunc('MONTH', c.paid_date) = date_trunc('MONTH', f.due_date)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 🔎 **Quick check before any join:** is my join key unique on the lookup side?

-- COMMAND ----------

SELECT client_id, count(*) AS rows_per_key
FROM file_deliveries
GROUP BY client_id
HAVING count(*) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern 3: filter early and directly on the column
-- MAGIC Write filters as **ranges on the raw column**. Databricks can then skip whole files using the min/max statistics it keeps for each file.

-- COMMAND ----------

-- ❌ Harder to skip files: the column is wrapped in functions
SELECT count(*), sum(paid_amount)
FROM medical_claims_large
WHERE date_format(service_date, 'yyyy-MM') = '2025-03'

-- COMMAND ----------

-- ✅ Range on the raw column
SELECT count(*), sum(paid_amount)
FROM medical_claims_large
WHERE service_date >= '2025-03-01' AND service_date < '2025-04-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern 4: lay the data out for the way you filter (liquid clustering)
-- MAGIC Our large table was written in random order, so **every file contains every month**. Even a one-month filter has to open **all** the files.
-- MAGIC
-- MAGIC **Liquid clustering** stores rows with similar dates in the same files. A one-month filter can then **skip** the files it doesn't need.
-- MAGIC
-- MAGIC We'll run **the same query twice**, before and after clustering, and compare the query profiles:
-- MAGIC
-- MAGIC ```
-- MAGIC  Step 1: BEFORE          Step 2: cluster the data       Step 3: AFTER            Step 4: compare
-- MAGIC  one-month query    ──►  CLUSTER BY (service_date)  ──► same one-month query ──► files read vs skipped
-- MAGIC  on the original table                                  on the clustered table
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 · BEFORE: one month of claims from the original table
-- MAGIC Run the cell → **See performance** → click the query → **Query profile**. Write down **Files read**, **Files pruned**, and **Bytes read**.

-- COMMAND ----------

-- BEFORE: original (unclustered) table
SELECT count(*) AS claims, sum(paid_amount) AS paid
FROM medical_claims_large
WHERE service_date >= '2025-03-01' AND service_date < '2025-04-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 · Cluster a copy of the table by `service_date`
-- MAGIC `CLUSTER BY` tells Databricks how to lay out the data, and `OPTIMIZE` rewrites the files in that order. This takes about 20 seconds.
-- MAGIC
-- MAGIC > ℹ️ The output is `OPTIMIZE`'s log, and you can ignore it. We set a **small target file size** only so that our small demo table has enough files to show skipping. On real tables, let Databricks choose the file size.

-- COMMAND ----------

CREATE OR REPLACE TABLE medical_claims_clustered
CLUSTER BY (service_date)
TBLPROPERTIES ('delta.targetFileSize' = '2mb')
AS SELECT * FROM medical_claims_large;

OPTIMIZE medical_claims_clustered;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 · AFTER: the exact same query on the clustered table
-- MAGIC Same answer (same claim count and dollars). Open its **query profile** and compare it with Step 1.

-- COMMAND ----------

-- AFTER: clustered table (same filter, same result)
SELECT count(*) AS claims, sum(paid_amount) AS paid
FROM medical_claims_clustered
WHERE service_date >= '2025-03-01' AND service_date < '2025-04-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 4 · Compare: how many files did each query have to open?

-- COMMAND ----------

-- Files in each table, and how many of them hold March 2025 data (= files the query must open)
WITH t AS (
  SELECT 'BEFORE: original' AS table_version, _metadata.file_path AS file, service_date FROM medical_claims_large
  UNION ALL
  SELECT 'AFTER: clustered',                  _metadata.file_path,         service_date FROM medical_claims_clustered
)
SELECT table_version,
       count(DISTINCT file)                                                                   AS files_in_table,
       count(DISTINCT CASE WHEN service_date >= '2025-03-01' AND service_date < '2025-04-01' THEN file END) AS files_read,
       count(DISTINCT file)
         - count(DISTINCT CASE WHEN service_date >= '2025-03-01' AND service_date < '2025-04-01' THEN file END) AS files_skipped
FROM t
GROUP BY table_version
ORDER BY table_version DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ✅ What you should see (instructor's test run)
-- MAGIC | From the query profile | BEFORE (original) | AFTER (clustered) |
-- MAGIC |---|---|---|
-- MAGIC | Files read | **8 of 8** (every file) | **2 of 16** |
-- MAGIC | Files pruned (skipped) | 0 | **14** |
-- MAGIC | Bytes read | 9.8 MB | **1.5 MB** (about 85% less) |
-- MAGIC | Total time (first run) | about 3.6 s | about 1.3 s |
-- MAGIC | Result | 97,402 claims | 97,402 claims (same) |
-- MAGIC
-- MAGIC **How to read it:** the clustered table has *more* files (16) because each one is smaller, but the query opens only the **2 files** that hold March data and skips the other 14.
-- MAGIC
-- MAGIC > ⏱️ **Why time is the weakest signal here:** with only 34 MB of data, every query is fast, and **running a query again returns a cached result** in well under a second. Judge the change by **files and bytes read**. On a table with billions of rows, the same 85% cut in data read turns minutes into seconds.
-- MAGIC
-- MAGIC 💡 For managed tables in Unity Catalog you can also use `CLUSTER BY AUTO`. Databricks then chooses the clustering columns based on how the table is queried (**predictive optimization**).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pattern 5: compute once, reuse many times
-- MAGIC If five dashboards each run the same heavy joins, compute the result **once** into a table, like `scorecard_monthly` in notebook 04, and refresh it on a schedule (notebook 06).
-- MAGIC
-- MAGIC ## Pattern 6: approximate when exact isn't needed
-- MAGIC For exploration, `approx_count_distinct` is usually within about 1–2% of the exact count and much cheaper on big tables.

-- COMMAND ----------

SELECT count(DISTINCT member_id) AS exact_members, approx_count_distinct(member_id) AS approx_members
FROM medical_claims_large

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🧾 Slow-query checklist
-- MAGIC 1. Open the **query profile**. Which operator takes the most time?
-- MAGIC 2. **Bytes or files read** are high → select fewer columns, filter on raw columns, or cluster the table on the columns you filter by.
-- MAGIC 3. **Rows out of a join** are much higher than rows in → check that the join key is unique.
-- MAGIC 4. The same heavy logic runs again and again → **materialize** it into a table and schedule the refresh.
-- MAGIC 5. Still slow? **Bring the query profile to your platform team.** Warehouse sizing and table maintenance are their call.
-- MAGIC
-- MAGIC ## ✋ Check-in
-- MAGIC A query joins 1M claims to a provider table and returns 40M rows. What do you check first?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Clean up (optional)

-- COMMAND ----------

-- DROP TABLE IF EXISTS medical_claims_clustered
