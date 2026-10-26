-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Reporting and dashboards: build a client scorecard
-- MAGIC **Session 1 · about 15 minutes**
-- MAGIC
-- MAGIC **Pattern:** do the heavy logic **once** in a scorecard table, and point the dashboard at that table. Dashboards then load quickly, every chart shows the same numbers, and in Session 2 we'll **schedule** the refresh.
-- MAGIC
-- MAGIC ```
-- MAGIC claims tables ──► scorecard_monthly (table) ──► AI/BI Dashboard ──► scheduled refresh / email
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "workspace", "Catalog")
-- MAGIC dbutils.widgets.text("schema", "claims_training", "Schema")
-- MAGIC spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Build the scorecard table
-- MAGIC One row per **client per month**, with volume, dollars, and data-quality KPIs.

-- COMMAND ----------

CREATE OR REPLACE TABLE scorecard_monthly
COMMENT 'Monthly client scorecard: volume, spend and data quality KPIs'
AS
WITH med AS (
  SELECT
    c.client_id,
    date_trunc('MONTH', c.service_date)                  AS month,
    count(*)                                             AS medical_claims,
    sum(c.paid_amount)                                   AS medical_paid,
    count_if(m.member_id IS NULL)                        AS medical_orphans
  FROM medical_claims c
  LEFT JOIN members m ON c.member_id = m.member_id
  GROUP BY ALL
),
rx AS (
  SELECT
    p.client_id,
    date_trunc('MONTH', p.fill_date)                     AS month,
    count(*)                                             AS pharmacy_claims,
    sum(p.paid_amount)                                   AS pharmacy_paid,
    count_if(m.member_id IS NULL)                        AS pharmacy_orphans,
    count(*) - count(DISTINCT p.member_id, p.ndc, p.pharmacy_id, p.fill_date) AS pharmacy_duplicates
  FROM pharmacy_claims p
  LEFT JOIN members m ON p.member_id = m.member_id
  GROUP BY ALL
)
SELECT
  cl.client_name,
  med.client_id,
  med.month,
  med.medical_claims,
  rx.pharmacy_claims,
  round(med.medical_paid, 2)                                              AS medical_paid,
  round(rx.pharmacy_paid, 2)                                              AS pharmacy_paid,
  round((med.medical_orphans + rx.pharmacy_orphans) * 100.0
        / (med.medical_claims + rx.pharmacy_claims), 2)                   AS orphan_rate_pct,
  round(rx.pharmacy_duplicates * 100.0 / rx.pharmacy_claims, 2)           AS duplicate_rate_pct,
  current_timestamp()                                                     AS refreshed_at
FROM med
JOIN rx      ON med.client_id = rx.client_id AND med.month = rx.month
JOIN clients cl ON med.client_id = cl.client_id

-- COMMAND ----------

SELECT * FROM scorecard_monthly ORDER BY client_id, month LIMIT 24

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Dashboard datasets
-- MAGIC Copy these queries into the dashboard's **Data** tab (step 3). Each one feeds one or more charts.

-- COMMAND ----------

-- Dataset 1: KPI tiles and trend lines
SELECT month, client_name,
       medical_claims + pharmacy_claims AS total_claims,
       medical_paid + pharmacy_paid     AS total_paid,
       orphan_rate_pct, duplicate_rate_pct
FROM scorecard_monthly

-- COMMAND ----------

-- Dataset 2: file delivery timeliness
SELECT c.client_name, date_trunc('MONTH', f.due_date) AS month,
       count(*)                          AS deliveries,
       round(avg(CAST(f.on_time AS INT)) * 100, 1) AS on_time_pct,
       count_if(f.status <> 'Delivered') AS resends
FROM file_deliveries f JOIN clients c USING (client_id)
GROUP BY ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Build the AI/BI Dashboard (live demo, about 10 minutes)
-- MAGIC 1. Left sidebar → **Dashboards** → **Create dashboard**.
-- MAGIC 2. **Data** tab → **Create from SQL**, paste **Dataset 1**, run it, and rename it `scorecard`. Repeat for **Dataset 2** (`deliveries`).
-- MAGIC 3. **Canvas** tab → add widgets:
-- MAGIC    * **Counter:** `SUM(total_paid)` → "Total paid"
-- MAGIC    * **Line chart:** X = `month`, Y = `SUM(total_claims)`, color = `client_name`
-- MAGIC    * **Bar chart:** X = `client_name`, Y = `AVG(orphan_rate_pct)`
-- MAGIC    * **Table** from `deliveries`, sorted by `on_time_pct`
-- MAGIC 4. Add a **Filter** widget on `client_name` and another on `month` (date range).
-- MAGIC 5. Try the **Ask the AI / Genie** button on the dashboard: *"Which client had the highest duplicate rate last quarter?"*
-- MAGIC 6. Click **Publish**. Then use **Schedule** to refresh it and email subscribers (for example, every Monday at 7 am).
-- MAGIC
-- MAGIC > 💡 **When is a Databricks dashboard enough?** Operational scorecards, data-quality monitoring, and internal sharing. **Tableau or another BI tool** makes sense when you need an existing enterprise report, pixel-perfect formatting, or external distribution. Tableau connects to a Databricks SQL warehouse through its built-in Databricks connector.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✋ Check-in
-- MAGIC * Why build `scorecard_monthly` as a table instead of putting all the logic in the dashboard? (One place to change the logic, dashboards that load fast, and the same numbers on every chart.)
-- MAGIC
-- MAGIC ## 🏋️ Try it (optional)
-- MAGIC Add a chart of **pharmacy paid by therapeutic class** to your dashboard. Hint: create a new dataset that joins `pharmacy_claims` to `drugs`.
