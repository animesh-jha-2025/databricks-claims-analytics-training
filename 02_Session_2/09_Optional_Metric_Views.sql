-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 09 · (Optional) Metric views: define a KPI once, use it everywhere
-- MAGIC **Session 2 · about 5 minutes, only if time allows**
-- MAGIC
-- MAGIC A **metric view** stores the official definition of your KPIs (for example *orphan rate* or *total paid*) in Unity Catalog. Dashboards, Genie, and SQL users all get the **same number** because they all use the same definition.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "workspace", "Catalog")
-- MAGIC dbutils.widgets.text("schema", "claims_training", "Schema")
-- MAGIC spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

CREATE OR REPLACE VIEW scorecard_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Official scorecard KPIs"
source: scorecard_monthly
dimensions:
  - name: Client
    expr: client_name
  - name: Month
    expr: month
measures:
  - name: Total Claims
    expr: SUM(medical_claims + pharmacy_claims)
  - name: Total Paid
    expr: SUM(medical_paid + pharmacy_paid)
  - name: Avg Orphan Rate Pct
    expr: AVG(orphan_rate_pct)
$$

-- COMMAND ----------

-- Query a metric view with MEASURE(); group by any dimension
SELECT Client, MEASURE(`Total Claims`) AS total_claims, MEASURE(`Total Paid`) AS total_paid
FROM scorecard_metrics
GROUP BY Client
ORDER BY total_paid DESC
