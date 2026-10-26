-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06b · Job task 2: refresh the scorecard
-- MAGIC **Task 2** of the `Scorecard Refresh` job. It runs only if `06a_Task_Data_Quality_Gate` succeeds.
-- MAGIC It rebuilds `scorecard_monthly` (the same logic as notebook 04) and keeps a run log so you can see every refresh.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "workspace", "Catalog")
-- MAGIC dbutils.widgets.text("schema", "claims_training", "Schema")
-- MAGIC spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

CREATE OR REPLACE TABLE scorecard_monthly
COMMENT 'Monthly client scorecard: volume, spend and data quality KPIs (refreshed by job)'
AS
WITH med AS (
  SELECT c.client_id, date_trunc('MONTH', c.service_date) AS month,
         count(*) AS medical_claims, sum(c.paid_amount) AS medical_paid,
         count_if(m.member_id IS NULL) AS medical_orphans
  FROM medical_claims c LEFT JOIN members m ON c.member_id = m.member_id
  GROUP BY ALL
),
rx AS (
  SELECT p.client_id, date_trunc('MONTH', p.fill_date) AS month,
         count(*) AS pharmacy_claims, sum(p.paid_amount) AS pharmacy_paid,
         count_if(m.member_id IS NULL) AS pharmacy_orphans,
         count(*) - count(DISTINCT p.member_id, p.ndc, p.pharmacy_id, p.fill_date) AS pharmacy_duplicates
  FROM pharmacy_claims p LEFT JOIN members m ON p.member_id = m.member_id
  GROUP BY ALL
)
SELECT cl.client_name, med.client_id, med.month, med.medical_claims, rx.pharmacy_claims,
       round(med.medical_paid, 2) AS medical_paid, round(rx.pharmacy_paid, 2) AS pharmacy_paid,
       round((med.medical_orphans + rx.pharmacy_orphans) * 100.0 / (med.medical_claims + rx.pharmacy_claims), 2) AS orphan_rate_pct,
       round(rx.pharmacy_duplicates * 100.0 / rx.pharmacy_claims, 2) AS duplicate_rate_pct,
       current_timestamp() AS refreshed_at
FROM med
JOIN rx ON med.client_id = rx.client_id AND med.month = rx.month
JOIN clients cl ON med.client_id = cl.client_id

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS scorecard_refresh_log (refreshed_at TIMESTAMP, row_count BIGINT);

INSERT INTO scorecard_refresh_log
SELECT current_timestamp(), count(*) FROM scorecard_monthly;

-- COMMAND ----------

SELECT * FROM scorecard_refresh_log ORDER BY refreshed_at DESC LIMIT 10
