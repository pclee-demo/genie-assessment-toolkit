# Databricks notebook source
# ── llm.py: Domain curation + sample question generation (LLM-assisted) ─────
# Requires: all variables from fetch.py + score.py + recommend.py
# Produces: domain_output (str), question_output (str), lineage_block (str)

# Initialise output variables so report.py can always reference them
domain_output   = ""
question_output = ""
lineage_block   = ""

# ── Helper: call Databricks Foundation Model API ──────────────────────────────
def llm_query(prompt, model=None, max_tokens=1500):
    model = model or LLM_MODEL
    r = requests.post(
        f"{host}/serving-endpoints/{model}/invocations",
        headers=headers,
        json={"messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens}
    )
    if r.status_code == 200:
        return r.json()["choices"][0]["message"]["content"]
    return f"[LLM unavailable: {r.status_code} — {r.text[:200]}]"

# ── Build shared metadata summary ─────────────────────────────────────────────
meta_lines = []
for table_id, meta in table_metadata.items():
    if "error" in meta:
        continue
    tname   = table_id.split(".")[-1]
    comment = (meta.get("table_comment") or "No description").strip()
    cols    = meta.get("columns", [])
    described = [c for c in cols if c.get("comment")]
    col_summary = "; ".join(
        f"{c['name']}: {c['comment'][:60]}" for c in described[:15]
    ) or ", ".join(f"{c['name']} ({c['type']})" for c in cols[:15])
    meta_lines.append(f"  Table: {tname}\n  Description: {comment}\n  Columns: {col_summary}")
metadata_block = "\n\n".join(meta_lines)

# ── Optionally enrich with UC lineage + audit (gated on USE_SYSTEM_TABLES) ────
if USE_SYSTEM_TABLES:
    print("Querying system tables for lineage and co-access patterns...")
    full_names_quoted = ", ".join(f"'{t}'" for t in table_identifiers)

    # Co-access via system.lineage.table_lineage (last 90 days)
    try:
        coaccess_df = spark.sql(f"""
            SELECT
                source_table_full_name AS table_a,
                target_table_full_name AS table_b,
                COUNT(*)               AS co_access_count
            FROM system.lineage.table_lineage
            WHERE event_time >= CURRENT_DATE - INTERVAL 90 DAYS
              AND (source_table_full_name IN ({full_names_quoted})
                   OR target_table_full_name IN ({full_names_quoted}))
            GROUP BY 1, 2
            ORDER BY co_access_count DESC
            LIMIT 30
        """).collect()

        if coaccess_df:
            lineage_lines = ["Co-access / lineage patterns (last 90 days):"]
            for row in coaccess_df:
                a = row["table_a"].split(".")[-1] if row["table_a"] else "?"
                b = row["table_b"].split(".")[-1] if row["table_b"] else "?"
                lineage_lines.append(f"  {a} → {b}  ({row['co_access_count']} times)")
            lineage_block = "\n".join(lineage_lines)
        else:
            lineage_block = "No lineage data found for these tables in the last 90 days."
    except Exception as e:
        lineage_block = f"system.lineage.table_lineage not accessible ({str(e)[:100]}) — proceeding on metadata only."

    # Co-access via system.access.audit (last 90 days)
    try:
        audit_df = spark.sql(f"""
            SELECT
                request_params.full_name_arg AS table_name,
                user_identity.email          AS user,
                DATE(event_time)             AS access_date
            FROM system.access.audit
            WHERE action_name = 'getTable'
              AND event_time  >= CURRENT_DATE - INTERVAL 90 DAYS
              AND request_params.full_name_arg IN ({full_names_quoted})
            ORDER BY user, access_date, table_name
            LIMIT 500
        """).collect()

        if audit_df:
            from collections import defaultdict, Counter
            session_tables = defaultdict(set)
            for row in audit_df:
                key = (row["user"], str(row["access_date"]))
                session_tables[key].add(row["table_name"].split(".")[-1] if row["table_name"] else "?")

            pair_counts = Counter()
            for tables in session_tables.values():
                tables = sorted(tables)
                for i in range(len(tables)):
                    for j in range(i + 1, len(tables)):
                        pair_counts[(tables[i], tables[j])] += 1

            if pair_counts:
                audit_lines = ["Tables co-accessed in same user session (last 90 days):"]
                for (a, b), cnt in pair_counts.most_common(10):
                    audit_lines.append(f"  {a} + {b}: {cnt} sessions")
                lineage_block += "\n\n" + "\n".join(audit_lines)
    except Exception:
        pass  # audit table not accessible — silently skip

    print(f"Lineage context: {len(lineage_block)} chars\n")

# ── Domain Curation (only when Table & Space Curation is Poor or OK) ──────────
space_name_str = space.get("display_name", space.get("title", "this Genie space"))

if a1 < 3 or len(schemas) > 1:
    lineage_section = (
        f"\n\nLineage and co-access data:\n{lineage_block}"
        if lineage_block else
        "\n\n(No lineage data available — recommendations based on metadata only. "
        "Enable 'Use system tables' widget for richer analysis.)"
    )

    domain_prompt = f"""You are a Databricks data domain expert helping a customer improve their Genie space.

The Genie space "{space_name_str}" currently has {table_count} tables. This is {'too many' if table_count > 10 else 'borderline'} — best practice is ≤10 tightly scoped tables per space.

Table metadata:
{metadata_block}
{lineage_section}

Please:
1. Identify 2–4 distinct business domains represented in this table set
2. Recommend which tables belong in each domain
3. Highlight any cross-domain join dependencies that need to be resolved before splitting
4. Suggest a descriptive name for each domain-specific Genie space
5. Flag any tables that appear redundant or out of scope entirely

Be concise and practical. Use plain business language, not SQL or technical jargon."""

    print(DIVIDER)
    print("DOMAIN CURATION RECOMMENDATIONS (LLM-assisted)")
    print(f"Model: {LLM_MODEL}" +
          (" + UC lineage" if USE_SYSTEM_TABLES and lineage_block else " — metadata only"))
    print(DIVIDER)
    domain_output = llm_query(domain_prompt)
    print(domain_output)
    print(DIVIDER)
else:
    print("✓ Table curation looks good — domain split analysis not required.")

# ── Sample Question & KPI Generator ──────────────────────────────────────────
lineage_for_q = (f"\n\nAdditional context from UC lineage:\n{lineage_block[:1000]}"
                 if USE_SYSTEM_TABLES and lineage_block else "")

question_prompt = f"""You are a business analyst helping a team get value from a Databricks Genie space called "{space_name_str}".

Here is the metadata for the tables in this space:

{metadata_block}{lineage_for_q}

Please generate:
1. 15 realistic questions a business user would ask about this data, in natural language. For each question note whether it is: aggregation, filter, trend over time, comparison, or top-N.
2. The 5 most important of those questions to use as benchmark questions (mark these clearly).
3. 5 KPIs a business user would want to monitor regularly from this data. For each KPI:
   - Give it a short business name (e.g. "Monthly Active Customers")
   - Describe what it measures in one sentence
   - Provide a plain-English formula hint that could be turned into a SQL aggregate expression (e.g. "COUNT of distinct customers with at least one transaction in the last 30 days")
   - These will be used as SQL Expression Measures in Databricks Genie (Configuration > SQL Expressions)

Use plain business language. Do not write SQL. Do not reference column names directly — use business-friendly terms."""

print(DIVIDER)
print("SAMPLE QUESTIONS & KPIs  (LLM-generated from metadata)")
print(f"Model: {LLM_MODEL}" +
      (" + UC lineage" if USE_SYSTEM_TABLES and lineage_for_q else " — metadata only"))
print(DIVIDER)
question_output = llm_query(question_prompt, max_tokens=2000)
print(question_output)
print(DIVIDER)
print("\n💡 Use the output above to populate:")
if sq_count < 10:
    print(f"   • Sample Questions ({sq_count}/10 configured) — Configuration > Sample Questions")
if bm_count < 15:
    print(f"   • Benchmarks ({bm_count}/15 configured) — Configuration > Benchmarks")
if not measures_ex:
    print(f"   • SQL Expression Measures (0 configured) — Configuration > SQL Expressions — use the KPIs above as a starting point")
