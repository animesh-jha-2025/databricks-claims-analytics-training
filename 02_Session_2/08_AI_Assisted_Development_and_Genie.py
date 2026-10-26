# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · AI-assisted development and an introduction to Genie
# MAGIC **Session 2 · about 15 minutes**
# MAGIC
# MAGIC ## Which tool for which job?
# MAGIC | You want to… | Use | Why |
# MAGIC |---|---|---|
# MAGIC | Write, explain, or fix SQL/PySpark **in a notebook** | **Databricks Assistant** (in notebooks and the SQL editor) | It knows your tables, columns, and the error you just got |
# MAGIC | Ask questions of data in **plain English**, or check results during UAT | **Genie space** | It answers from curated tables, shows the SQL it ran, and works for business users |
# MAGIC | Work on a **large multi-file codebase** in an editor | **Cursor / VS Code + Copilot**, plus the Databricks extension | Full editor experience, with the code still running on Databricks |
# MAGIC | Version control, review, and audit | **GitHub + Git folders** (notebook 07) | History, pull requests, and rollback |
# MAGIC
# MAGIC 💡 They work well together. For example, write the code in Cursor, keep it in GitHub, run it as a job in Databricks, and let business users check the results in Genie.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
spark.sql(f"USE {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Databricks Assistant (live demo)
# MAGIC Open the Assistant with the ✨ icon (top right), or press **Cmd/Ctrl + I** inside a cell.
# MAGIC
# MAGIC ### Prompting tips that improve the output
# MAGIC 1. **Name the tables and columns:** *"Using `pharmacy_claims` and `drugs`…"*
# MAGIC 2. **State the business rule:** *"A duplicate is the same member, ndc, pharmacy_id and fill_date."*
# MAGIC 3. **Say what the output should look like:** *"Return one row per client with columns client_id, duplicate_rows, duplicate_paid."*
# MAGIC 4. **Give the constraints:** *"Use SQL, not Python. Keep the first claim by rx_claim_id."*
# MAGIC 5. **Iterate:** *"Now add the percent of total claims."* Short follow-ups work better than one giant prompt.
# MAGIC 6. **Always check the result:** run it on a small sample and compare a known number.
# MAGIC
# MAGIC ### Try these prompts
# MAGIC | Prompt | Where |
# MAGIC |---|---|
# MAGIC | `/explain` | Select the scorecard query in notebook 04 |
# MAGIC | *"Write a SQL query that finds members with GLP-1 fills in 3 or more different months in 2025, using pharmacy_claims and drugs."* | New cell below |
# MAGIC | *"Convert this SQL to PySpark."* | On any SQL cell |
# MAGIC | `/fix` | Run the broken cell below, then click **Diagnose error** |
# MAGIC | *"Write a reconciliation query that compares total paid in medical_claims by month to scorecard_monthly and flags differences."* | New cell, for UAT |

# COMMAND ----------

# 🐞 Broken on purpose: run it, then click "Diagnose error" (or type /fix in the Assistant)
display(
    spark.table("pharmacy_claims")
    .groupBy("client_id")
    .agg({"paid_amt": "sum"})
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Genie for business validation and UAT (introduction)
# MAGIC A **Genie space** lets people ask questions about a curated set of tables in plain English. Genie writes the SQL, runs it, and **shows you the SQL** so you can check it.
# MAGIC
# MAGIC ### Build a small Genie space (5 minutes)
# MAGIC 1. Left sidebar → **Genie** → **New**.
# MAGIC 2. Add tables: `scorecard_monthly`, `pharmacy_claims`, `drugs`, `members`, `clients`.
# MAGIC 3. **Instructions** (paste these):
# MAGIC    ```
# MAGIC    - "Paid" claims means claim_status = 'Paid'.
# MAGIC    - An orphan claim is a claim whose member_id is not in members.
# MAGIC    - A duplicate pharmacy claim has the same member_id, ndc, pharmacy_id and fill_date as another claim.
# MAGIC    - Always show money rounded to whole dollars.
# MAGIC    ```
# MAGIC 4. Ask:
# MAGIC    * *"What is the orphan rate by client for 2025?"*
# MAGIC    * *"Which therapeutic class had the biggest increase in paid amount from Q1 to Q2 2025?"*
# MAGIC    * *"Show monthly GLP-1 fills for Client A as a line chart."*
# MAGIC 5. Click **Show code** on an answer to see the SQL. **This is the UAT habit:** check the logic, not only the number.
# MAGIC
# MAGIC ### What makes Genie accurate
# MAGIC * **Clear table and column descriptions.** The setup notebook added them, and Genie reads them.
# MAGIC * **A small, focused set of tables** for one business area.
# MAGIC * **Instructions and example SQL** for your business rules.
# MAGIC * **Benchmarks:** a list of questions with known answers that you re-run after each change.
# MAGIC
# MAGIC ### Genie vs. a query vs. an agent
# MAGIC | | Best for |
# MAGIC |---|---|
# MAGIC | **SQL query / dashboard** | Repeatable, approved numbers that everyone sees the same way |
# MAGIC | **Genie** | One-off questions, exploration, and UAT spot checks on curated data |
# MAGIC | **Agent** (preview only) | Multi-step tasks that combine tools, documents, and data. A topic for a later session |
# MAGIC
# MAGIC > A deeper **Genie hands-on workshop** (benchmarks, SQL expressions, governance) is planned as a follow-up session.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✋ Check-in
# MAGIC * Genie gives you a number that looks wrong. What's the first thing you do? (Click **Show code** and read the SQL.)
# MAGIC * Name one thing that makes the Assistant's answers better. (Naming the tables and columns, and stating the business rule.)

# COMMAND ----------

# MAGIC %md
# MAGIC <details><summary>Solution for the broken cell</summary>
# MAGIC
# MAGIC The column is called `paid_amount`, not `paid_amt`:
# MAGIC ```python
# MAGIC display(spark.table("pharmacy_claims").groupBy("client_id").agg({"paid_amount": "sum"}))
# MAGIC ```
# MAGIC </details>
