-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Workspace tour and finding trusted data with Unity Catalog
-- MAGIC **Session 1 · about 25 minutes**
-- MAGIC
-- MAGIC **You will learn to:**
-- MAGIC 1. Find your way around the workspace (Workspace, Catalog, SQL Editor, Jobs, Dashboards, Genie).
-- MAGIC 2. Use the three-level namespace: `catalog.schema.table`.
-- MAGIC 3. Find a table, read its description and columns, and check who can use it.
-- MAGIC 4. Run your first SQL query on serverless compute.
-- MAGIC
-- MAGIC > **Before you start:** run `00_Setup/00_Generate_Synthetic_Data` once.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Point every cell in this notebook at the training schema.
-- MAGIC dbutils.widgets.text("catalog", "workspace", "Catalog")
-- MAGIC dbutils.widgets.text("schema", "claims_training", "Schema")
-- MAGIC spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. The three-level namespace
-- MAGIC
-- MAGIC ```
-- MAGIC catalog        →  schema            →  table / view / volume
-- MAGIC workspace      →  claims_training   →  medical_claims
-- MAGIC ```
-- MAGIC * A **catalog** is the top-level container (often one per environment or business area).
-- MAGIC * A **schema** (also called a database) groups related tables.
-- MAGIC * **Unity Catalog** controls permissions, lineage, and auditing for all of them in one place.

-- COMMAND ----------

SELECT current_catalog(), current_schema(), current_user()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Learn about a table before you query it
-- MAGIC 👉 **In the UI:** open **Catalog** in the left sidebar → `claims_training` → `medical_claims`. Look at the **Overview**, **Sample Data**, **Details**, **Permissions**, and **Lineage** tabs.
-- MAGIC
-- MAGIC You can get the same information with SQL:

-- COMMAND ----------

DESCRIBE TABLE EXTENDED medical_claims

-- COMMAND ----------

-- Who can read this table? (In your Free Edition workspace you are the owner.)
SHOW GRANTS ON TABLE medical_claims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Your first queries
-- MAGIC Tip: start with `LIMIT` while you explore, so you don't pull back everything.

-- COMMAND ----------

SELECT * FROM medical_claims LIMIT 10

-- COMMAND ----------

SELECT
  claim_status,
  count(*)                    AS claim_lines,
  round(sum(paid_amount), 0)  AS total_paid
FROM medical_claims
GROUP BY claim_status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Tables, views, and where data comes from
-- MAGIC * A **table** stores data.
-- MAGIC * A **view** is a saved query. It stores no data and always shows the latest results.
-- MAGIC
-- MAGIC Let's create a view of **paid** claims only. After that, open the view in Catalog Explorer → **Lineage** to see which table it reads from.

-- COMMAND ----------

CREATE OR REPLACE VIEW v_paid_medical_claims
COMMENT 'Medical claim lines with status Paid'
AS SELECT * FROM medical_claims WHERE claim_status = 'Paid'

-- COMMAND ----------

SELECT count(*) AS paid_lines FROM v_paid_medical_claims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✋ Check-in
-- MAGIC 1. What is the full three-part name of the pharmacy claims table?
-- MAGIC 2. Where in the UI do you check whether you're allowed to query a table?
-- MAGIC
-- MAGIC ## 🏋️ Try it (3 min)
-- MAGIC Write a query that shows how many members are in each `plan_type`.

-- COMMAND ----------

-- Your query here


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <details><summary>Show solution</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT plan_type, count(*) AS members
-- MAGIC FROM members
-- MAGIC GROUP BY plan_type
-- MAGIC ORDER BY members DESC
-- MAGIC ```
-- MAGIC </details>
