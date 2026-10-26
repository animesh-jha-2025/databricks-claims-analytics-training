# Databricks notebook source
# MAGIC %md
# MAGIC # 06a · Job task 1: data-quality gate
# MAGIC This notebook is **task 1** of the `Scorecard Refresh` job. If the data fails a check, the notebook **raises an error**. The task then fails, the scorecard refresh is skipped, and the job sends a failure email.
# MAGIC
# MAGIC Parameters come from the job (or from the widgets when you run it by hand).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
dbutils.widgets.text("max_orphan_rate_pct", "3.0", "Max orphan rate %")
spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")
max_orphan = float(dbutils.widgets.get("max_orphan_rate_pct"))

# COMMAND ----------

result = spark.sql("""
  SELECT
    count(*)                                        AS claims,
    count_if(m.member_id IS NULL)                   AS orphans,
    round(count_if(m.member_id IS NULL) * 100.0 / count(*), 2) AS orphan_rate_pct,
    count_if(c.paid_amount IS NULL)                 AS null_paid
  FROM medical_claims c
  LEFT JOIN members m ON c.member_id = m.member_id
""").first()

print(f"Claims: {result.claims:,} | Orphans: {result.orphans:,} ({result.orphan_rate_pct}%) | Null paid: {result.null_paid}")

# COMMAND ----------

failures = []
if result.claims == 0:
    failures.append("medical_claims is empty")
if result.orphan_rate_pct > max_orphan:
    failures.append(f"orphan rate {result.orphan_rate_pct}% is above the {max_orphan}% threshold")

if failures:
    raise ValueError("Data-quality gate FAILED: " + "; ".join(failures))

# Pass a value to later tasks in the job (read with {{tasks.data_quality_gate.values.orphan_rate_pct}})
dbutils.jobs.taskValues.set(key="orphan_rate_pct", value=float(result.orphan_rate_pct))
print("✅ Data-quality gate passed")
