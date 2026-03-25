# Databricks notebook source
# ── fetch.py: API setup + space metadata ─────────────────────────────────────
# Variables produced (available to all subsequent %run cells):
#   token, host, headers, api_get, space, text_instructions, sql_instructions,
#   sample_questions, benchmarks, table_identifiers, table_metadata, DIVIDER

import requests, json, re, os
from datetime import datetime

DIVIDER = "─" * 60

token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def api_get(path):
    r = requests.get(f"{host}{path}", headers=headers)
    return r.json() if r.status_code == 200 else {"error": f"{r.status_code}: {r.text[:200]}"}

# ── Space config ──────────────────────────────────────────────────────────────
space = api_get(f"/api/2.0/data-rooms/{SPACE_ID}")
if "error" in space:
    raise Exception(f"Could not fetch space: {space['error']}\nCheck your Space ID is correct.")

# ── Instructions + example SQLs ───────────────────────────────────────────────
instructions_resp = api_get(f"/api/2.0/data-rooms/{SPACE_ID}/instructions")
all_instructions  = instructions_resp.get("instructions", [])
text_instructions = [i for i in all_instructions if i.get("instruction_type") == "TEXT_INSTRUCTION"]
sql_instructions  = [i for i in all_instructions if i.get("instruction_type") == "SQL_INSTRUCTION"]

# SQL Expressions (Measures, Dimensions, Filters, Synonyms) — stored as SQL_SNIPPET items
# Each item's content is a JSON string; parse into a normalised list for score.py.
# Synonyms are embedded inside each item (not separate items) — collect them flat
# into sql_expression_synonyms so score.py can check coverage without inflating the count.
sql_expressions = []
sql_expression_synonyms = []  # flat list of all synonym strings across all expressions
for _i in all_instructions:
    if _i.get("instruction_type") != "SQL_SNIPPET":
        continue
    try:
        _c = json.loads(_i.get("content", "{}"))
    except Exception:
        _c = {}
    _raw_type = _c.get("type", "")
    _name = _c.get("display_name") or _c.get("alias", "")
    _code = _c.get("code", "")
    _syns = _c.get("synonyms", [])
    if "MEASURE" in _raw_type:
        _expr_type = "MEASURE"
    elif "DIMENSION" in _raw_type:
        _expr_type = "DIMENSION"
    elif "FILTER" in _raw_type:
        _expr_type = "FILTER"
    else:
        _expr_type = _raw_type
    sql_expressions.append({"expression_type": _expr_type, "name": _name, "expression": _code})
    sql_expression_synonyms.extend(_syns)


# ── Sample questions + benchmarks ─────────────────────────────────────────────
questions_resp   = api_get(f"/api/2.0/data-rooms/{SPACE_ID}/curated-questions")
all_questions    = questions_resp.get("curated_questions", [])
sample_questions      = [q for q in all_questions if q.get("question_type") == "SAMPLE_QUESTION"]
benchmarks            = [q for q in all_questions if q.get("question_type") == "BENCHMARK"]
benchmark_suggestions = [q for q in all_questions if q.get("question_type") == "BENCHMARK_SUGGESTION"]

# ── Table + column metadata from UC ──────────────────────────────────────────
table_identifiers = space.get("table_identifiers", [])
table_metadata    = {}

for table_id in table_identifiers:
    parts = table_id.split(".")
    if len(parts) != 3:
        table_metadata[table_id] = {"error": f"Cannot parse: {table_id}"}
        continue
    catalog, schema, table = parts
    try:
        tbl_info      = spark.sql(f"DESCRIBE TABLE EXTENDED `{catalog}`.`{schema}`.`{table}`").collect()
        table_comment = next((r["data_type"] for r in tbl_info if r["col_name"] == "Comment"), "")
        cols_df = spark.sql(f"""
            SELECT column_name, data_type, comment
            FROM `{catalog}`.information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
        """).collect()
        type_rows = spark.sql(f"""
            SELECT table_type FROM `{catalog}`.information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            LIMIT 1
        """).collect()
        col_names = [r["col_name"].lower() for r in tbl_info]
        table_metadata[table_id] = {
            "table_comment": table_comment,
            "owner": next((r["data_type"] for r in tbl_info if r["col_name"] == "Owner"), None),
            "table_type": type_rows[0]["table_type"] if type_rows else "BASE TABLE",
            "has_row_filter": any("row filter" in c for c in col_names),
            "has_col_mask":   any("column mask" in c or "masking policy" in c for c in col_names),
            "columns": [{"name": r["column_name"], "type": r["data_type"], "comment": r["comment"] or ""} for r in cols_df],
        }
    except Exception as e:
        table_metadata[table_id] = {"error": str(e)}

# ── Area 0 additional data ────────────────────────────────────────────────────

# UC tags per table
table_tags = {}
for table_id in table_identifiers:
    parts = table_id.split(".")
    if len(parts) != 3:
        table_tags[table_id] = {}
        continue
    catalog, schema, table = parts
    try:
        rows = spark.sql(f"SHOW TAGS ON TABLE `{catalog}`.`{schema}`.`{table}`").collect()
        table_tags[table_id] = {r["tag_name"]: r["tag_value"] for r in rows}
    except Exception:
        table_tags[table_id] = {}

# PK/FK constraints via information_schema
pk_fk_tables = set()
for table_id in table_identifiers:
    parts = table_id.split(".")
    if len(parts) != 3:
        continue
    catalog, schema, table = parts
    try:
        rows = spark.sql(f"""
            SELECT 1 FROM `{catalog}`.information_schema.table_constraints
            WHERE table_schema = '{schema}' AND table_name = '{table}'
              AND constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
            LIMIT 1
        """).collect()
        if rows:
            pk_fk_tables.add(table_id)
    except Exception:
        pass

# Genie join definitions — FROM_SNIPPET items whose parsed content has a "join" key.
# FROM_SNIPPET also covers table registrations (content has "comment" key) — exclude those.
genie_joins = [
    i for i in all_instructions
    if i.get("instruction_type") == "FROM_SNIPPET"
    and "join" in json.loads(i.get("content", "{}") or "{}")
]

# Warehouse type (serverless vs. classic)
warehouse_id   = space.get("warehouse_id", "")
warehouse_info = api_get(f"/api/2.0/sql/warehouses/{warehouse_id}") if warehouse_id else {}
is_serverless  = warehouse_info.get("enable_serverless_compute") if "error" not in warehouse_info else None

# Permissions: space ACL, warehouse ACL, table grants
space_acl_resp    = api_get(f"/api/2.0/permissions/data-rooms/{SPACE_ID}")
space_acl         = space_acl_resp.get("access_control_list", []) if "error" not in space_acl_resp else []

warehouse_acl_resp = api_get(f"/api/2.0/permissions/warehouses/{warehouse_id}") if warehouse_id else {}
warehouse_acl      = warehouse_acl_resp.get("access_control_list", []) if "error" not in warehouse_acl_resp else []

table_grants = {}
for table_id in table_identifiers:
    parts = table_id.split(".")
    if len(parts) != 3:
        table_grants[table_id] = None
        continue
    catalog, schema, table = parts
    try:
        rows = spark.sql(f"SHOW GRANTS ON TABLE `{catalog}`.`{schema}`.`{table}`").collect()
        table_grants[table_id] = [r.asDict() for r in rows]
    except Exception:
        table_grants[table_id] = None  # None = insufficient privilege to check

# Trusted Answers
trusted_answers_resp = api_get(f"/api/2.0/data-rooms/{SPACE_ID}/trusted-answers")
if "error" in trusted_answers_resp:
    trusted_answers_resp = api_get(f"/api/2.0/data-rooms/{SPACE_ID}/trusted_answers")
trusted_answers = (
    trusted_answers_resp.get("trusted_answers")
    or trusted_answers_resp.get("answers")
    or []
) if "error" not in trusted_answers_resp else []

print(f"✓ Space:         {space.get('display_name', space.get('title', ''))}")
print(f"  Tables:        {len(table_identifiers)}")
print(f"  Instructions:  {len(text_instructions)} text, {len(sql_instructions)} SQL queries, {len(sql_expressions)} SQL expressions, {len(genie_joins)} joins")
print(f"  Questions:     {len(sample_questions)} sample, {len(benchmarks)} benchmarks, {len(benchmark_suggestions)} benchmark suggestions (unconfirmed)")
print(f"  Trusted Ans:   {len(trusted_answers)}")
print(f"  PK/FK tables:  {len(pk_fk_tables)}/{len(table_identifiers)}")
print(f"  Space ACL:     {len(space_acl)} entries  |  Warehouse ACL: {len(warehouse_acl)} entries")
