# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: generate the synthetic claims dataset
# MAGIC
# MAGIC **Run this notebook once before the training.** It takes about 1–2 minutes on Databricks Free Edition (serverless).
# MAGIC
# MAGIC It creates a small, **fully synthetic, de-identified** pharmacy and medical claims dataset. No real people, providers, or clients are represented. Every ID and name is generated.
# MAGIC
# MAGIC | Table | Rows (default) | What it represents |
# MAGIC |---|---|---|
# MAGIC | `clients` | 8 | Data licensing clients (Client A … H) |
# MAGIC | `members` | 5,000 | De-identified members (hashed IDs, birth year only) |
# MAGIC | `providers` | 400 | Synthetic prescribers / rendering providers |
# MAGIC | `drugs` | 40 | Drug reference (fake NDCs, real generic names) |
# MAGIC | `medical_claims` | ~60,000 | Medical claim lines |
# MAGIC | `pharmacy_claims` | ~50,000 | Pharmacy fills |
# MAGIC | `file_deliveries` | 140 | History of data files delivered to clients |
# MAGIC | `medical_claims_large` | 2,000,000 | Bigger copy used only in the query-optimization lab |
# MAGIC
# MAGIC **Data quality issues are planted on purpose** so we can find them in class:
# MAGIC * ~1.5% of claims are **orphan claims**: their `member_id` does not exist in `members`.
# MAGIC * ~2% of pharmacy claims are **duplicates**: same member, drug, pharmacy, and fill date, with a new claim ID.
# MAGIC * A few claims have missing amounts or dates.
# MAGIC
# MAGIC ▶ **How to run:** click **Run all** at the top. Change the widgets only if your instructor asks you to.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "claims_training", "Schema")
dbutils.widgets.dropdown("build_large_table", "yes", ["yes", "no"], "Build 2M-row table for optimization lab")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BUILD_LARGE = dbutils.widgets.get("build_large_table") == "yes"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Synthetic claims data for Databricks training'")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Writing tables to {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions
# MAGIC Everything below uses `spark.range()` and built-in functions with fixed seeds, so every attendee gets **the same data**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def pick(values, seed_col):
    """Deterministically pick one value from a Python list using a numeric column."""
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.abs(F.hash(seed_col)) % len(values) + 1).cast("int"))

def rand_int(lo, hi, seed):
    """Random integer in [lo, hi] with a fixed seed."""
    return (F.floor(F.rand(seed) * (hi - lo + 1)) + lo).cast("int")

def save(df, name, comment):
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name))
    spark.sql(f"COMMENT ON TABLE {name} IS '{comment}'")
    print(f"✔ {name:<22} {spark.table(name).count():>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference tables: clients, members, providers, drugs

# COMMAND ----------

clients = spark.createDataFrame(
    [
        ("C001", "Client A", "Life Sciences", "Monthly"),
        ("C002", "Client B", "Life Sciences", "Monthly"),
        ("C003", "Client C", "Consulting", "Quarterly"),
        ("C004", "Client D", "Life Sciences", "Monthly"),
        ("C005", "Client E", "Research", "Quarterly"),
        ("C006", "Client F", "Life Sciences", "Monthly"),
        ("C007", "Client G", "Media Measurement", "Weekly"),
        ("C008", "Client H", "Research", "Monthly"),
    ],
    "client_id STRING, client_name STRING, segment STRING, delivery_frequency STRING",
)
save(clients, "clients", "Synthetic data-licensing clients")

# COMMAND ----------

N_MEMBERS = 5000
states = ["CT", "TX", "FL", "MO", "TN", "AZ", "CO", "IL", "PA", "NC", "GA", "OH"]
region_of = {"CT": "Northeast", "PA": "Northeast", "TX": "South", "FL": "South", "TN": "South",
             "NC": "South", "GA": "South", "MO": "Midwest", "IL": "Midwest", "OH": "Midwest",
             "AZ": "West", "CO": "West"}
region_map = F.create_map(*[F.lit(x) for kv in region_of.items() for x in kv])

members = (
    spark.range(N_MEMBERS)
    .withColumn("member_id", F.concat(F.lit("M"), F.substring(F.sha2(F.col("id").cast("string"), 256), 1, 10)))
    .withColumn("birth_year", rand_int(1940, 2010, 1))
    .withColumn("gender", pick(["F", "M"], F.col("id") * 7))
    .withColumn("state", pick(states, F.col("id") * 13))
    .withColumn("region", region_map[F.col("state")])
    .withColumn("plan_type", pick(["Commercial", "Commercial", "Commercial", "Medicare Advantage", "Medicaid"], F.col("id") * 17))
    .withColumn("enroll_start", F.date_add(F.lit("2023-01-01"), rand_int(0, 540, 2)))
    .withColumn("enroll_end", F.when(F.rand(3) < 0.15, F.date_add(F.col("enroll_start"), rand_int(120, 600, 4))))
    .drop("id")
)
save(members, "members", "Synthetic de-identified members (hashed IDs, birth year only)")

# COMMAND ----------

specialties = ["Family Medicine", "Internal Medicine", "Cardiology", "Endocrinology", "Oncology",
               "Orthopedics", "Psychiatry", "Dermatology", "Pediatrics", "Rheumatology"]
providers = (
    spark.range(400)
    .withColumn("provider_id", F.format_string("P%05d", F.col("id") + 10000))
    .withColumn("specialty", pick(specialties, F.col("id") * 3))
    .withColumn("provider_state", pick(states, F.col("id") * 5))
    .withColumn("network_status", F.when(F.rand(5) < 0.85, "In-Network").otherwise("Out-of-Network"))
    .drop("id")
)
save(providers, "providers", "Synthetic providers")

# COMMAND ----------

drug_rows = [
    ("atorvastatin", "Statins", "Generic", 12.0), ("rosuvastatin", "Statins", "Generic", 15.0),
    ("metformin", "Antidiabetics", "Generic", 8.0), ("semaglutide", "GLP-1 Agonists", "Brand", 950.0),
    ("tirzepatide", "GLP-1 Agonists", "Brand", 1050.0), ("dulaglutide", "GLP-1 Agonists", "Brand", 890.0),
    ("insulin glargine", "Insulins", "Brand", 310.0), ("empagliflozin", "SGLT2 Inhibitors", "Brand", 560.0),
    ("dapagliflozin", "SGLT2 Inhibitors", "Brand", 540.0), ("lisinopril", "ACE Inhibitors", "Generic", 6.0),
    ("losartan", "ARBs", "Generic", 9.0), ("amlodipine", "Calcium Channel Blockers", "Generic", 7.0),
    ("metoprolol", "Beta Blockers", "Generic", 8.0), ("apixaban", "Anticoagulants", "Brand", 520.0),
    ("rivaroxaban", "Anticoagulants", "Brand", 510.0), ("adalimumab", "Immunology", "Specialty", 6800.0),
    ("ustekinumab", "Immunology", "Specialty", 21000.0), ("etanercept", "Immunology", "Specialty", 6500.0),
    ("pembrolizumab", "Oncology", "Specialty", 11000.0), ("palbociclib", "Oncology", "Specialty", 13500.0),
    ("sertraline", "Antidepressants", "Generic", 10.0), ("escitalopram", "Antidepressants", "Generic", 11.0),
    ("bupropion", "Antidepressants", "Generic", 18.0), ("aripiprazole", "Antipsychotics", "Generic", 25.0),
    ("levothyroxine", "Thyroid", "Generic", 9.0), ("omeprazole", "Proton Pump Inhibitors", "Generic", 10.0),
    ("pantoprazole", "Proton Pump Inhibitors", "Generic", 11.0), ("albuterol", "Respiratory", "Generic", 35.0),
    ("fluticasone-salmeterol", "Respiratory", "Brand", 330.0), ("montelukast", "Respiratory", "Generic", 12.0),
    ("gabapentin", "Anticonvulsants", "Generic", 14.0), ("amoxicillin", "Antibiotics", "Generic", 9.0),
    ("azithromycin", "Antibiotics", "Generic", 12.0), ("prednisone", "Corticosteroids", "Generic", 6.0),
    ("hydrochlorothiazide", "Diuretics", "Generic", 5.0), ("furosemide", "Diuretics", "Generic", 6.0),
    ("tamsulosin", "Urology", "Generic", 14.0), ("sildenafil", "Urology", "Generic", 20.0),
    ("dupilumab", "Immunology", "Specialty", 3600.0), ("evolocumab", "PCSK9 Inhibitors", "Brand", 580.0),
]
drugs = (
    spark.createDataFrame(drug_rows, "drug_name STRING, therapeutic_class STRING, drug_type STRING, unit_cost DOUBLE")
    .withColumn("ndc", F.format_string("99999-%04d-01", F.row_number().over(Window.orderBy("drug_name")) + 1000))
)
save(drugs, "drugs", "Synthetic drug reference (fake NDCs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact tables: medical and pharmacy claims
# MAGIC Claims are linked to members by row position. About 1.5% of claims get a member ID that does not exist (**orphan claims**).

# COMMAND ----------

member_ids = members.select("member_id", "region").withColumn("m_idx", F.row_number().over(
    Window.orderBy("member_id")) - 1)
provider_ids = providers.select("provider_id").withColumn("p_idx", F.row_number().over(
    Window.orderBy("provider_id")) - 1)
drug_ids = drugs.select("ndc", "unit_cost", "drug_type").withColumn("d_idx", F.row_number().over(
    Window.orderBy("ndc")) - 1)
client_ids = clients.select("client_id").withColumn("c_idx", F.row_number().over(
    Window.orderBy("client_id")) - 1)

dx_codes = ["E11.9", "I10", "E78.5", "J45.909", "M54.5", "F32.9", "K21.9", "E66.9", "I25.10", "M06.9",
            "C50.911", "L40.0", "G47.33", "N18.3", "Z00.00"]
cpt_codes = ["99213", "99214", "99215", "80053", "83036", "93000", "71046", "36415", "97110", "99396"]

def build_medical_claims(n_rows, seed_offset=0):
    base = (
        spark.range(n_rows)
        .withColumn("m_idx", (F.abs(F.hash(F.col("id") + seed_offset)) % N_MEMBERS).cast("long"))
        .withColumn("p_idx", (F.abs(F.hash(F.col("id") * 31 + seed_offset)) % 400).cast("long"))
        .withColumn("c_idx", (F.abs(F.hash(F.col("id") * 7 + seed_offset)) % 8).cast("long"))
        .withColumn("is_orphan", F.rand(11 + seed_offset) < 0.015)
    )
    df = (
        base.join(F.broadcast(member_ids), "m_idx", "left")
        .join(F.broadcast(provider_ids), "p_idx", "left")
        .join(F.broadcast(client_ids), "c_idx", "left")
        .withColumn("member_id", F.when(F.col("is_orphan"),
                    F.concat(F.lit("M"), F.substring(F.sha2(F.concat(F.lit("orphan"), F.col("id").cast("string")), 256), 1, 10)))
                    .otherwise(F.col("member_id")))
        .withColumn("claim_id", F.format_string("MC%09d", F.col("id") + seed_offset))
        .withColumn("service_date", F.date_add(F.lit("2024-01-01"), rand_int(0, 640, 12 + seed_offset)))
        .withColumn("paid_date", F.date_add(F.col("service_date"), rand_int(5, 60, 13 + seed_offset)))
        .withColumn("dx_code", pick(dx_codes, F.col("id") * 3 + seed_offset))
        .withColumn("cpt_code", pick(cpt_codes, F.col("id") * 5 + seed_offset))
        .withColumn("place_of_service", pick(["Office", "Office", "Outpatient", "Telehealth", "Inpatient", "ER"], F.col("id") * 11 + seed_offset))
        .withColumn("billed_amount", F.round(F.exp(F.randn(14 + seed_offset) * 0.9 + 5.3), 2))
        .withColumn("allowed_amount", F.round(F.col("billed_amount") * (0.45 + F.rand(15 + seed_offset) * 0.35), 2))
        .withColumn("paid_amount", F.round(F.col("allowed_amount") * (0.70 + F.rand(16 + seed_offset) * 0.30), 2))
        .withColumn("claim_status", F.when(F.rand(17 + seed_offset) < 0.06, "Denied").otherwise("Paid"))
        .withColumn("paid_amount", F.when(F.col("claim_status") == "Denied", F.lit(0.0)).otherwise(F.col("paid_amount")))
        # A few missing values for data-quality checks
        .withColumn("allowed_amount", F.when(F.rand(18 + seed_offset) < 0.003, None).otherwise(F.col("allowed_amount")))
        .withColumn("source_file", F.concat(F.lit("med_"), F.col("client_id"), F.lit("_"), F.date_format("paid_date", "yyyyMM"), F.lit(".csv")))
        .select("claim_id", "member_id", "provider_id", "client_id", "service_date", "paid_date",
                "dx_code", "cpt_code", "place_of_service", "billed_amount", "allowed_amount",
                "paid_amount", "claim_status", "source_file")
    )
    return df

medical_claims = build_medical_claims(60000)
save(medical_claims, "medical_claims", "Synthetic medical claim lines; ~1.5% orphan claims planted")

# COMMAND ----------

N_RX = 49000
rx_base = (
    spark.range(N_RX)
    .withColumn("m_idx", (F.abs(F.hash(F.col("id") + 99)) % N_MEMBERS).cast("long"))
    .withColumn("p_idx", (F.abs(F.hash(F.col("id") * 29)) % 400).cast("long"))
    .withColumn("c_idx", (F.abs(F.hash(F.col("id") * 3)) % 8).cast("long"))
    .withColumn("d_idx", (F.abs(F.hash(F.col("id") * 41)) % len(drug_rows)).cast("long"))
    .withColumn("is_orphan", F.rand(21) < 0.015)
)
pharmacy_claims = (
    rx_base.join(F.broadcast(member_ids), "m_idx", "left")
    .join(F.broadcast(provider_ids), "p_idx", "left")
    .join(F.broadcast(client_ids), "c_idx", "left")
    .join(F.broadcast(drug_ids), "d_idx", "left")
    .withColumn("member_id", F.when(F.col("is_orphan"),
                F.concat(F.lit("M"), F.substring(F.sha2(F.concat(F.lit("rxorphan"), F.col("id").cast("string")), 256), 1, 10)))
                .otherwise(F.col("member_id")))
    .withColumn("rx_claim_id", F.format_string("RX%09d", F.col("id")))
    .withColumn("fill_date", F.date_add(F.lit("2024-01-01"), rand_int(0, 640, 22)))
    .withColumn("pharmacy_id", F.format_string("PH%04d", (F.abs(F.hash(F.col("id") * 13)) % 250).cast("int")))
    .withColumn("days_supply", pick([30, 30, 30, 90, 90, 28], F.col("id") * 17))
    .withColumn("quantity", (F.col("days_supply") * pick([1, 1, 2], F.col("id") * 19)).cast("int"))
    .withColumn("ingredient_cost", F.round(F.col("unit_cost") * F.col("days_supply") / 30 * (0.9 + F.rand(23) * 0.2), 2))
    .withColumn("paid_amount", F.round(F.col("ingredient_cost") * (0.80 + F.rand(24) * 0.15), 2))
    .withColumn("claim_status", F.when(F.rand(25) < 0.04, "Reversed").otherwise("Paid"))
    .withColumn("prescriber_id", F.col("provider_id"))
    .withColumn("source_file", F.concat(F.lit("rx_"), F.col("client_id"), F.lit("_"), F.date_format("fill_date", "yyyyMM"), F.lit(".csv")))
    .select("rx_claim_id", "member_id", "client_id", "ndc", "prescriber_id", "pharmacy_id", "fill_date",
            "days_supply", "quantity", "ingredient_cost", "paid_amount", "claim_status", "source_file")
)

# Plant ~2% duplicates: same member/drug/pharmacy/fill date, new claim ID
dupes = (
    pharmacy_claims.where(F.abs(F.hash("rx_claim_id")) % 50 == 0)
    .withColumn("rx_claim_id", F.concat(F.col("rx_claim_id"), F.lit("D")))
)
save(pharmacy_claims.unionByName(dupes), "pharmacy_claims", "Synthetic pharmacy fills; ~2% duplicates and ~1.5% orphans planted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## File delivery history (for the historical file deliveries / scorecard examples)

# COMMAND ----------

months = spark.sql("SELECT explode(sequence(DATE'2024-01-01', DATE'2025-09-01', INTERVAL 1 MONTH)) AS delivery_month")
file_deliveries = (
    months.crossJoin(clients.select("client_id", "delivery_frequency"))
    .where((F.col("delivery_frequency") != "Quarterly") | (F.month("delivery_month") % 3 == 1))
    .withColumn("delivery_id", F.concat(F.col("client_id"), F.lit("-"), F.date_format("delivery_month", "yyyyMM")))
    .withColumn("file_name", F.concat(F.lit("claims_extract_"), F.col("client_id"), F.lit("_"), F.date_format("delivery_month", "yyyyMM"), F.lit(".parquet")))
    .withColumn("due_date", F.date_add(F.col("delivery_month"), 10))
    .withColumn("delivered_date", F.date_add(F.col("due_date"), rand_int(-3, 6, 31)))
    .withColumn("row_count", rand_int(80000, 400000, 32))
    .withColumn("status", F.when(F.rand(33) < 0.05, "Failed - Resent").otherwise("Delivered"))
    .withColumn("on_time", F.col("delivered_date") <= F.col("due_date"))
    .drop("delivery_frequency")
)
save(file_deliveries, "file_deliveries", "Synthetic history of client file deliveries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Large table for the query-optimization lab (Session 2)
# MAGIC 2M rows, about 60 MB. Skip it with the widget if you are short on time.

# COMMAND ----------

if BUILD_LARGE:
    save(build_medical_claims(2_000_000, seed_offset=1_000_000), "medical_claims_large",
         "2M-row synthetic medical claims for the query optimization lab")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add column descriptions
# MAGIC Good descriptions make data easier to find in Catalog Explorer and help **Genie** and the **Databricks Assistant** give better answers.

# COMMAND ----------

column_comments = {
    "members": {"member_id": "De-identified member key (hashed)", "birth_year": "Year of birth only (no full DOB)",
                "plan_type": "Commercial, Medicare Advantage, or Medicaid", "enroll_end": "NULL means still enrolled"},
    "medical_claims": {"claim_id": "Unique medical claim line ID", "member_id": "Joins to members.member_id",
                       "service_date": "Date the service was provided", "paid_date": "Date the claim was paid",
                       "dx_code": "Primary ICD-10 diagnosis code", "cpt_code": "CPT procedure code",
                       "allowed_amount": "Contracted allowed amount in USD", "paid_amount": "Amount paid by the plan in USD; 0 when Denied"},
    "pharmacy_claims": {"rx_claim_id": "Unique pharmacy claim ID; IDs ending in D are planted duplicates",
                        "ndc": "Synthetic National Drug Code; joins to drugs.ndc", "fill_date": "Date the prescription was filled",
                        "days_supply": "Days of medication dispensed", "paid_amount": "Amount paid by the plan in USD"},
}
for table, cols in column_comments.items():
    for col, text in cols.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{text}'")
print("✔ Column comments added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done. Quick check

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
