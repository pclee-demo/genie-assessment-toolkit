# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Assessment
# MAGIC
# MAGIC Run all cells to generate a scored assessment of your Genie space.
# MAGIC
# MAGIC **What this checks:**
# MAGIC | Area | What it looks for |
# MAGIC |------|-------------------|
# MAGIC | 0. Data & UC Readiness | Genie joins, PK/FK constraints, table ownership, serverless warehouse, Knowledge Store |
# MAGIC | 1. Table & Space Curation | Table count, layer quality, space description |
# MAGIC | 2. Metadata Quality | Column coverage, table comments, value dictionaries, type safety |
# MAGIC | 3. Example SQL | Count, query-type coverage, hardcoded values, parameterisation |
# MAGIC | 4. Instructions | Business rules vs. schema dumps, inline SQL, emphatic overrides |
# MAGIC | 5. Sample Questions | Count and diversity |
# MAGIC | 6. Benchmarks | Count, SQL completeness, complexity coverage |
# MAGIC | 7. Semantic Layer | Metric Views in space, SQL Expressions (Measures/Synonyms/Filters) |
# MAGIC
# MAGIC **Output:** a `.md` assessment file saved to your workspace (or a custom path).
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC | Requirement | Notes |
# MAGIC |---|---|
# MAGIC | Databricks cluster (DBR 10+) | Any cluster type; no extra libraries needed |
# MAGIC | CAN\_MANAGE on the Genie space | Required to read instructions, SQL examples, questions, and joins via API |
# MAGIC | USE CATALOG + USE SCHEMA + SELECT on the space's tables | Required for metadata, tag, and grant checks |
# MAGIC | Foundation Model API access _(optional)_ | Required for the Domain Curation and Sample Question sections. Needs Pay-Per-Token Foundation Models enabled on the workspace (`databricks-meta-llama-3-3-70b-instruct`). If unavailable, those sections are omitted and the rest of the assessment runs normally. |
# MAGIC | System tables access _(optional)_ | Required only when **Use system tables** is set to `true`. Enables lineage-enriched LLM recommendations. Needs `system.lineage` and `system.access.audit` access (typically workspace admin). |
# MAGIC | UC admin or table owner _(optional)_ | Required for `SHOW GRANTS ON TABLE` to return full results. If not available, grant checks are flagged as unverified rather than failing. |

# COMMAND ----------

# Widget setup — fill in SPACE_ID, then optionally adjust the others
dbutils.widgets.text("space_id",          "",    "Genie Space ID")
dbutils.widgets.dropdown("use_system_tables", "false", ["true", "false"], "Use system tables (lineage/audit)")
dbutils.widgets.text("output_path",       "",    "Output path (blank = auto /Workspace/Users/<you>/genie-assessments/)")
dbutils.widgets.combobox(
    "llm_model",
    "databricks-meta-llama-3-3-70b-instruct",
    [
        "databricks-meta-llama-3-3-70b-instruct",
        "databricks-meta-llama-3-1-405b-instruct",
        "databricks-claude-3-7-sonnet",
        "databricks-dbrx-instruct",
        "databricks-mixtral-8x7b-instruct",
    ],
    "LLM model (domain curation & sample questions)"
)

SPACE_ID          = dbutils.widgets.get("space_id").strip()
USE_SYSTEM_TABLES = dbutils.widgets.get("use_system_tables").strip().lower() == "true"
OUTPUT_PATH       = dbutils.widgets.get("output_path").strip()
LLM_MODEL         = dbutils.widgets.get("llm_model").strip() or "databricks-meta-llama-3-3-70b-instruct"

if not SPACE_ID:
    raise ValueError("Please enter your Genie Space ID in the 'space_id' widget above.")

# COMMAND ----------

# MAGIC %run ../src/genie_assessment/fetch

# COMMAND ----------

# MAGIC %run ../src/genie_assessment/score

# COMMAND ----------

# MAGIC %run ../src/genie_assessment/recommend

# COMMAND ----------

# MAGIC %run ../src/genie_assessment/llm

# COMMAND ----------

# MAGIC %run ../src/genie_assessment/report
