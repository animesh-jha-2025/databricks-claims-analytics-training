# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Automating reporting with Lakeflow Jobs
# MAGIC **Session 2 · about 20 minutes**
# MAGIC
# MAGIC **Goal:** refresh the client scorecard automatically every week, check data quality first, and get an email if something fails. No one has to remember to run it.
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────┐   on success   ┌──────────────────────────┐    ┌───────────────────────┐
# MAGIC │ Task 1: data_quality_gate│ ─────────────► │ Task 2: refresh_scorecard│ ─► │ Dashboard (scheduled)  │
# MAGIC │ 06a_Task_Data_Quality_Gate│               │ 06b_Task_Refresh_Scorecard│   │ shows new numbers      │
# MAGIC └──────────────────────────┘                └──────────────────────────┘    └───────────────────────┘
# MAGIC          │ fails → job stops, email sent
# MAGIC ```
# MAGIC
# MAGIC | Concept | What it means |
# MAGIC |---|---|
# MAGIC | **Job** | A set of tasks that run together |
# MAGIC | **Task** | One step (notebook, SQL file, dashboard refresh, …) |
# MAGIC | **Dependency** | Task 2 waits for Task 1 to finish |
# MAGIC | **Trigger** | When the job runs: a schedule (cron), a file arriving, or a table update |
# MAGIC | **Retries** | Re-run a failed task automatically, for example once after 5 minutes |
# MAGIC | **Notifications** | Email on start, success, or failure |
# MAGIC | **Parameters** | Values like `catalog` and `schema` passed to every task |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A: build the job in the UI (live demo)
# MAGIC 1. Left sidebar → **Jobs & Pipelines** → **Create** → **Job**. Name it `Scorecard Refresh`.
# MAGIC 2. **Task 1:** name `data_quality_gate`, type **Notebook**, path `02_Session_2/06a_Task_Data_Quality_Gate`. Compute: **Serverless**.
# MAGIC 3. **+ Add task** → name `refresh_scorecard`, type **Notebook**, path `02_Session_2/06b_Task_Refresh_Scorecard`, **Depends on** `data_quality_gate`.
# MAGIC 4. In the job's side panel → **Parameters** → add `catalog` = `workspace` and `schema` = `claims_training`. Job parameters are passed to every task.
# MAGIC 5. On Task 1 → **Retries** → 1 retry after 5 minutes.
# MAGIC 6. **Schedules & Triggers** → **Add trigger** → **Scheduled** → weekly, Monday 6:00 am.
# MAGIC 7. **Job notifications** → **Add** → your email → **Failure**.
# MAGIC 8. Click **Run now** → open the run → watch the task graph turn green.
# MAGIC 9. **Break it on purpose:** change `max_orphan_rate_pct` on Task 1 to `1.0` and run again. Task 1 fails, Task 2 is skipped, and you get an email.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B: create the same job with code (Databricks SDK)
# MAGIC Jobs can also be defined in code, which makes them easy to review in GitHub and to copy to other environments. Set the widget `create_job` to `yes` and run the cell below.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
dbutils.widgets.dropdown("create_job", "no", ["no", "yes"], "Create job?")
dbutils.widgets.text("notify_email", "", "Failure email (optional)")

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

if dbutils.widgets.get("create_job") == "yes":
    w = WorkspaceClient()
    folder = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    email = dbutils.widgets.get("notify_email").strip()

    job = w.jobs.create(
        name="Scorecard Refresh (training)",
        parameters=[
            jobs.JobParameterDefinition(name="catalog", default=dbutils.widgets.get("catalog")),
            jobs.JobParameterDefinition(name="schema", default=dbutils.widgets.get("schema")),
        ],
        tasks=[
            jobs.Task(
                task_key="data_quality_gate",
                notebook_task=jobs.NotebookTask(notebook_path=f"{folder}/06a_Task_Data_Quality_Gate"),
                max_retries=1, min_retry_interval_millis=5 * 60 * 1000,
            ),
            jobs.Task(
                task_key="refresh_scorecard",
                depends_on=[jobs.TaskDependency(task_key="data_quality_gate")],
                notebook_task=jobs.NotebookTask(notebook_path=f"{folder}/06b_Task_Refresh_Scorecard"),
            ),
        ],
        schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 ? * MON", timezone_id="America/Chicago",
                                   pause_status=jobs.PauseStatus.PAUSED),
        email_notifications=jobs.JobEmailNotifications(on_failure=[email]) if email else None,
    )
    run = w.jobs.run_now(job_id=job.job_id)
    print(f"Created job {job.job_id}. Schedule is PAUSED so it won't run weekly unless you turn it on.")
    print(f"Open it: {w.config.host}/jobs/{job.job_id}")
else:
    print("Set the 'create_job' widget to 'yes' to create the job with code.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where Power Automate fits
# MAGIC Keep **data work** (refreshes, checks, transformations) in Databricks Jobs, which run next to the data and record lineage and run history. Tools like Microsoft Power Automate can **start** a job through the Jobs API or **react** to job notifications, for example by posting to a Teams channel. Building Power Automate flows is outside the scope of this training.
# MAGIC
# MAGIC ## ✋ Check-in
# MAGIC * What happens to Task 2 if Task 1 fails? (It's skipped, and the failure email goes out.)
# MAGIC * Where would you look to see why last Monday's run failed? (**Jobs & Pipelines** → the job → **Runs** → click the failed run → open the task output.)
