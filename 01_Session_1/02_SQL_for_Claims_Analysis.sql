-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · SQL for claims analysis
-- MAGIC **Session 1 · about 25 minutes**
-- MAGIC
-- MAGIC We'll use SQL to answer two questions the team works on today:
-- MAGIC 1. **Orphan claims:** which claims don't match any member?
-- MAGIC 2. **Duplicate pharmacy claims:** which fills appear more than once?
-- MAGIC
-- MAGIC Along the way we'll use joins, CTEs (`WITH`), date functions, window functions, and `QUALIFY`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "workspace", "Catalog")
-- MAGIC dbutils.widgets.text("schema", "claims_training", "Schema")
-- MAGIC spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Joins: add member details to each claim

-- COMMAND ----------

SELECT
  c.claim_id, c.service_date, c.dx_code, c.paid_amount,
  m.plan_type, m.region, m.birth_year
FROM medical_claims AS c
JOIN members AS m
  ON c.member_id = m.member_id
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Use case: orphan claims
-- MAGIC An **orphan claim** has a `member_id` that isn't in the member table. A `LEFT ANTI JOIN` returns only the rows on the left that have **no match** on the right.

-- COMMAND ----------

SELECT c.*
FROM medical_claims AS c
LEFT ANTI JOIN members AS m
  ON c.member_id = m.member_id
LIMIT 20

-- COMMAND ----------

-- How big is the problem, per client and claim type?
WITH orphans AS (
  SELECT 'Medical' AS claim_type, c.client_id, c.paid_amount
  FROM medical_claims c LEFT ANTI JOIN members m ON c.member_id = m.member_id
  UNION ALL
  SELECT 'Pharmacy', p.client_id, p.paid_amount
  FROM pharmacy_claims p LEFT ANTI JOIN members m ON p.member_id = m.member_id
)
SELECT
  claim_type,
  client_id,
  count(*)                   AS orphan_claims,
  round(sum(paid_amount), 0) AS orphan_paid_amount
FROM orphans
GROUP BY ALL
ORDER BY orphan_claims DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Use case: duplicate pharmacy claims
-- MAGIC Our rule: two fills are duplicates when they have the **same member, drug (NDC), pharmacy, and fill date**.
-- MAGIC
-- MAGIC **Step 1:** find the groups that appear more than once.

-- COMMAND ----------

SELECT member_id, ndc, pharmacy_id, fill_date, count(*) AS copies
FROM pharmacy_claims
GROUP BY ALL
HAVING count(*) > 1
ORDER BY copies DESC
LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Step 2:** a **window function** numbers the rows inside each group. `QUALIFY` filters on that number, so we keep one copy and flag the rest.

-- COMMAND ----------

SELECT
  rx_claim_id, member_id, ndc, pharmacy_id, fill_date, paid_amount,
  row_number() OVER (PARTITION BY member_id, ndc, pharmacy_id, fill_date ORDER BY rx_claim_id) AS copy_number
FROM pharmacy_claims
QUALIFY count(*) OVER (PARTITION BY member_id, ndc, pharmacy_id, fill_date) > 1
ORDER BY member_id, fill_date, copy_number
LIMIT 20

-- COMMAND ----------

-- Dollar impact of duplicates (every copy after the first)
WITH extra_copies AS (
  SELECT paid_amount
  FROM pharmacy_claims
  QUALIFY row_number() OVER (PARTITION BY member_id, ndc, pharmacy_id, fill_date ORDER BY rx_claim_id) > 1
)
SELECT
  count(*)                   AS duplicate_rows,
  round(sum(paid_amount), 0) AS duplicate_paid_amount
FROM extra_copies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Dates: monthly trends
-- MAGIC `date_trunc('MONTH', ...)` rolls dates up to the first of the month, which is handy for scorecards.

-- COMMAND ----------

SELECT
  date_trunc('MONTH', p.fill_date) AS fill_month,
  d.therapeutic_class,
  count(*)                         AS fills,
  round(sum(p.paid_amount), 0)     AS paid_amount
FROM pharmacy_claims p
JOIN drugs d USING (ndc)
WHERE d.therapeutic_class = 'GLP-1 Agonists'
GROUP BY ALL
ORDER BY fill_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 💡 **Tip:** click **+** above a result → **Visualization** → **Line** to chart this result without leaving the notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✋ Check-in
-- MAGIC * What is the difference between `LEFT JOIN ... WHERE m.member_id IS NULL` and `LEFT ANTI JOIN`? (They return the same rows. `ANTI JOIN` is shorter and makes the intent clear.)
-- MAGIC * Why use `row_number()` instead of `DISTINCT` to remove duplicates? (With `row_number()` you choose which copy to keep, and you can still see the others.)
-- MAGIC
-- MAGIC ## 🏋️ Try it (5 min)
-- MAGIC Find the **top 5 therapeutic classes by paid amount** in 2025, counting only `Paid` claims.

-- COMMAND ----------

-- Your query here


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <details><summary>Show solution</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT d.therapeutic_class, round(sum(p.paid_amount), 0) AS paid_amount
-- MAGIC FROM pharmacy_claims p
-- MAGIC JOIN drugs d USING (ndc)
-- MAGIC WHERE p.claim_status = 'Paid'
-- MAGIC   AND p.fill_date >= '2025-01-01' AND p.fill_date < '2026-01-01'
-- MAGIC GROUP BY d.therapeutic_class
-- MAGIC ORDER BY paid_amount DESC
-- MAGIC LIMIT 5
-- MAGIC ```
-- MAGIC </details>
