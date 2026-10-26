# Databricks notebook source
# MAGIC %md
# MAGIC # 04a · Create the Client Scorecard dashboard in one click
# MAGIC **Session 1 · about 2 minutes** (run after `04_Reporting_and_Dashboards`, which builds `scorecard_monthly`)
# MAGIC
# MAGIC Click **Run all**. This notebook creates and publishes a finished **AI/BI Dashboard** in your home folder and prints its link:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │ Client Scorecard: claims volume, spend and data quality     │
# MAGIC ├───────────────────┬───────────────────┬────────────────────┤
# MAGIC │ Total paid        │ Avg orphan rate % │ Avg duplicate rate %│
# MAGIC ├───────────────────┴───┬───────────────┴────────────────────┤
# MAGIC │ Monthly claims (line) │ Orphan rate by client (bar)         │
# MAGIC ├───────────────────────┴────────────────────────────────────┤
# MAGIC │ File delivery timeliness by client (table)                  │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC   Global filters: Client, Month
# MAGIC ```
# MAGIC
# MAGIC Then, in class, we **edit** it: change a chart, add a widget, republish, and ask **Genie** a question on the published view.
# MAGIC Running it again updates the same dashboard instead of making a copy.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
dbutils.widgets.text("dashboard_name", "Client Scorecard", "Dashboard name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dashboard_name = dbutils.widgets.get("dashboard_name")
fq = f"{catalog}.{schema}"

# The scorecard table has to exist first (it's built in notebook 04)
assert spark.catalog.tableExists(f"{fq}.scorecard_monthly"), "Run 04_Reporting_and_Dashboards first to build scorecard_monthly."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Datasets
# MAGIC The same two queries from notebook 04, with two small fixes:
# MAGIC * **The last, partial month is left out.** The synthetic data ends on Oct 2, 2025, so October has only 2 days of claims and would look like a crash.
# MAGIC * **Deliveries are grouped by client**, so on-time % is meaningful (each client gets one file a month, so a monthly on-time % is always 0 or 100).

# COMMAND ----------

datasets = [
    {
        "name": "scorecard",
        "displayName": "scorecard",
        "queryLines": [
            "SELECT CAST(month AS DATE) AS month, client_name, ",
            "       medical_claims + pharmacy_claims AS total_claims, ",
            "       medical_paid + pharmacy_paid     AS total_paid, ",
            "       orphan_rate_pct, duplicate_rate_pct ",
            f"FROM {fq}.scorecard_monthly ",
            "WHERE month < DATE'2025-10-01'",
        ],
    },
    {
        "name": "deliveries",
        "displayName": "deliveries",
        "queryLines": [
            "SELECT c.client_name, ",
            "       count(*)                                      AS deliveries, ",
            "       count_if(NOT f.on_time)                       AS late_deliveries, ",
            "       round(avg(CAST(f.on_time AS INT)) * 100, 1)   AS on_time_pct, ",
            "       count_if(f.status <> 'Delivered')             AS resends, ",
            "       max(f.delivered_date)                         AS last_delivered ",
            f"FROM {fq}.file_deliveries f JOIN {fq}.clients c USING (client_id) ",
            "GROUP BY c.client_name",
        ],
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Layout
# MAGIC A dashboard is a JSON definition: datasets, plus widgets placed on a 6-column grid. You don't need to write this yourself. It's here so you can see what the dashboard editor saves for you.

# COMMAND ----------

def text(name, line, y):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [line]}},
            "position": {"x": 0, "y": y, "width": 6, "height": 1}}

def counter(name, title, expr, x):
    field = {"name": name, "expression": expr}
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": "scorecard", "fields": [field], "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": name, "displayName": title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": 2, "width": 2, "height": 3}}

def chart(name, title, kind, x_field, x_type, y_name, y_expr, y_title, x):
    fields = [{"name": x_field, "expression": f"`{x_field}`"}, {"name": y_name, "expression": y_expr}]
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": "scorecard", "fields": fields, "disaggregated": False}}],
                       "spec": {"version": 3, "widgetType": kind,
                                "encodings": {"x": {"fieldName": x_field, "scale": {"type": x_type}, "displayName": x_field.replace("_", " ").title()},
                                              "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": 5, "width": 3, "height": 6}}

delivery_cols = [("client_name", "Client"), ("on_time_pct", "On-time %"), ("deliveries", "Deliveries"),
                 ("late_deliveries", "Late"), ("resends", "Resends"), ("last_delivered", "Last delivered")]
delivery_table = {
    "widget": {"name": "delivery_table",
               "queries": [{"name": "main_query", "query": {"datasetName": "deliveries",
                            "fields": [{"name": c, "expression": f"`{c}`"} for c, _ in delivery_cols], "disaggregated": True}}],
               "spec": {"version": 2, "widgetType": "table",
                        "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in delivery_cols]},
                        "frame": {"showTitle": True, "title": "File delivery timeliness by client (lowest on-time % first)"}}},
    "position": {"x": 0, "y": 11, "width": 6, "height": 6},
}

def filter_widget(name, title, kind, field, dataset_names, y):
    queries = [{"name": f"{name}_{ds}", "query": {"datasetName": ds, "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": False}}
               for ds in dataset_names]
    return {"widget": {"name": name, "queries": queries,
                       "spec": {"version": 2, "widgetType": kind,
                                "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": q["name"]} for q in queries]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": 0, "y": y, "width": 1, "height": 2}}

dashboard = {
    "datasets": datasets,
    "pages": [
        {"name": "scorecard_page", "displayName": "Scorecard", "pageType": "PAGE_TYPE_CANVAS", "layout": [
            text("title", "## Client Scorecard: claims volume, spend and data quality", 0),
            text("subtitle", "Synthetic de-identified claims · Jan 2024 to Sep 2025 · source: `scorecard_monthly` and `file_deliveries`", 1),
            counter("total_paid", "Total paid ($)", "SUM(`total_paid`)", 0),
            counter("avg_orphan_rate", "Avg orphan rate (%)", "AVG(`orphan_rate_pct`)", 2),
            counter("avg_duplicate_rate", "Avg pharmacy duplicate rate (%)", "AVG(`duplicate_rate_pct`)", 4),
            chart("claims_trend", "Monthly claims (medical + pharmacy)", "line", "month", "temporal",
                  "sum(total_claims)", "SUM(`total_claims`)", "Claims", 0),
            chart("orphans_by_client", "Avg orphan rate by client (%)", "bar", "client_name", "categorical",
                  "avg(orphan_rate_pct)", "AVG(`orphan_rate_pct`)", "Orphan rate (%)", 3),
            delivery_table,
        ]},
        {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layout": [
            filter_widget("filter_client", "Client", "filter-multi-select", "client_name", ["scorecard", "deliveries"], 0),
            filter_widget("filter_month", "Month", "filter-date-range-picker", "month", ["scorecard"], 2),
        ]},
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create (or update) and publish the dashboard

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
me = w.current_user.me().user_name
parent_path = f"/Users/{me}"

# Free Edition has one SQL warehouse (Serverless Starter Warehouse); use the first one we find
warehouses = list(w.warehouses.list())
assert warehouses, "No SQL warehouse found. Open SQL Warehouses in the sidebar and start one."
warehouse_id = warehouses[0].id

body = {"display_name": dashboard_name, "warehouse_id": warehouse_id, "serialized_dashboard": json.dumps(dashboard)}

# Re-running updates the existing dashboard instead of creating "Client Scorecard (1)"
try:
    existing = w.workspace.get_status(f"{parent_path}/{dashboard_name}.lvdash.json")
    dashboard_id = existing.resource_id
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body=body)
    action = "Updated"
except NotFound:
    dashboard_id = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body={**body, "parent_path": parent_path})["dashboard_id"]
    action = "Created"

# Publish with shared data permission (embed_credentials), so viewers and Genie can use it
w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
                body={"embed_credentials": True, "warehouse_id": warehouse_id})

url = f"{w.config.host.rstrip('/')}/dashboardsv3/{dashboard_id}/published"
print(f"{action} and published '{dashboard_name}' in {parent_path}")
displayHTML(f'<a href="{url}" target="_blank" style="font-size:16px">👉 Open the Client Scorecard dashboard</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✋ Next: make it yours (live, about 8 minutes)
# MAGIC 1. Open the dashboard → **Edit draft**.
# MAGIC 2. **Change a chart:** click the line chart → set **Color** to `client_name` (then set it back: 8 lines are hard to read, which is why the filter is there).
# MAGIC 3. **Add a widget:** **Add a visualization** → bar chart, X = `client_name`, Y = `AVG(duplicate_rate_pct)`.
# MAGIC 4. **Publish** again (keep **Share data permission** on).
# MAGIC 5. On the **published** view, open **Genie** and ask: *"Which client had the highest duplicate rate last quarter?"*
# MAGIC 6. **Schedule** → refresh every Monday at 7 am and email yourself.
