# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · PySpark for analysts
# MAGIC **Session 1 · about 20 minutes**
# MAGIC
# MAGIC Same data, same questions, this time in Python. You don't have to pick one language: **SQL and PySpark run on the same engine**, and you can switch between them in one notebook.
# MAGIC
# MAGIC | Use SQL when… | Use PySpark when… |
# MAGIC |---|---|
# MAGIC | The logic is joins, filters, and aggregations | You need loops, parameters, or reusable functions |
# MAGIC | You want to share it with SQL users or put it on a dashboard | You're building a multi-step pipeline or validation framework |
# MAGIC | | You want to use Python libraries (pandas, plotting) on a **small** result |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read a table into a DataFrame
# MAGIC A DataFrame is **lazy**: nothing runs until you ask for a result (`display`, `count`, `write`).

# COMMAND ----------

claims = spark.table("medical_claims")
members = spark.table("members")

claims.printSchema()
display(claims.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Filter, select, group: the PySpark version of SQL

# COMMAND ----------

summary = (
    claims
    .filter(F.col("claim_status") == "Paid")
    .groupBy("place_of_service")
    .agg(
        F.count("*").alias("claim_lines"),
        F.round(F.sum("paid_amount"), 0).alias("total_paid"),
        F.round(F.avg("paid_amount"), 2).alias("avg_paid"),
    )
    .orderBy(F.desc("total_paid"))
)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Orphan claims in PySpark (`left_anti` join)

# COMMAND ----------

orphans = claims.join(members, "member_id", "left_anti")
print(f"Orphan medical claims: {orphans.count():,} of {claims.count():,}")
display(orphans.groupBy("client_id").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A reusable data-quality check
# MAGIC Python lets you write a check once and run it on many tables. This is the start of a **UAT validation framework**.

# COMMAND ----------

def data_quality_report(table_name, key_cols, required_cols):
    """Return one row of data-quality metrics for a table."""
    df = spark.table(table_name)
    total = df.count()
    dup_rows = total - df.dropDuplicates(key_cols).count()
    null_counts = {c: df.filter(F.col(c).isNull()).count() for c in required_cols}
    return {
        "table": table_name,
        "rows": total,
        "duplicate_rows_on_key": dup_rows,
        **{f"nulls_{c}": n for c, n in null_counts.items()},
    }

checks = [
    data_quality_report("medical_claims", ["claim_id"], ["member_id", "allowed_amount", "paid_amount"]),
    data_quality_report("pharmacy_claims", ["member_id", "ndc", "pharmacy_id", "fill_date"], ["member_id", "paid_amount"]),
]
display(spark.createDataFrame(checks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mixing SQL and Python
# MAGIC Run SQL from Python with `spark.sql()`. Use Python variables as **parameters** (`:name`) instead of pasting them into the SQL string; it's safer and easier to read.

# COMMAND ----------

therapeutic_class = "GLP-1 Agonists"

glp1_by_region = spark.sql(
    """
    SELECT m.region, count(*) AS fills, round(sum(p.paid_amount), 0) AS paid
    FROM pharmacy_claims p
    JOIN drugs d   USING (ndc)
    JOIN members m USING (member_id)
    WHERE d.therapeutic_class = :tc
    GROUP BY m.region
    ORDER BY paid DESC
    """,
    args={"tc": therapeutic_class},
)
display(glp1_by_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. pandas: only for small results
# MAGIC `toPandas()` brings **all** rows onto one machine. Aggregate in Spark first, then convert the small result.

# COMMAND ----------

pdf = glp1_by_region.toPandas()   # ✅ a handful of rows
pdf["paid_share_pct"] = (pdf["paid"] / pdf["paid"].sum() * 100).round(1)
pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✋ Check-in
# MAGIC * Why didn't `spark.table("medical_claims")` take any time to run? (It's lazy. Nothing is read until you ask for a result.)
# MAGIC * When is `toPandas()` a bad idea? (When the DataFrame has millions of rows. Aggregate first.)
# MAGIC
# MAGIC ## 🏋️ Try it (5 min)
# MAGIC Use PySpark to find, for each `plan_type`, the **number of members with at least one GLP-1 fill**.

# COMMAND ----------

# Your code here


# COMMAND ----------

# MAGIC %md
# MAGIC <details><summary>Show solution</summary>
# MAGIC
# MAGIC ```python
# MAGIC glp1_ndcs = spark.table("drugs").filter(F.col("therapeutic_class") == "GLP-1 Agonists").select("ndc")
# MAGIC result = (
# MAGIC     spark.table("pharmacy_claims")
# MAGIC     .join(glp1_ndcs, "ndc")
# MAGIC     .join(members, "member_id")
# MAGIC     .groupBy("plan_type")
# MAGIC     .agg(F.countDistinct("member_id").alias("members_on_glp1"))
# MAGIC )
# MAGIC display(result)
# MAGIC ```
# MAGIC </details>
