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
    if domain_output.startswith("[LLM unavailable"):
        print(f"⚠ LLM unavailable — domain curation skipped. {domain_output}")
        domain_output = ""
    else:
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
if question_output.startswith("[LLM unavailable"):
    print(f"⚠ LLM unavailable — sample questions skipped. {question_output}")
    question_output = ""
else:
    print(question_output)
print(DIVIDER)
print("\n💡 Use the output above to populate:")
if sq_count < 10:
    print(f"   • Sample Questions ({sq_count}/10 configured) — Configuration > Sample Questions")
if bm_count < 15:
    print(f"   • Benchmarks ({bm_count}/15 configured) — Configuration > Benchmarks")
if not measures_ex:
    print(f"   • SQL Expression Measures (0 configured) — Configuration > SQL Expressions — use the KPIs above as a starting point")

# ── Instructions Generator (LLM-drafted, best-practice-guided) ───────────────
instructions_output = ""
if a4 < 3 or not text_instructions:
    # Extract existing text instructions content for context (rewrite mode)
    existing_instr_text = ""
    if text_instructions:
        parts = []
        for instr in text_instructions:
            content = (instr.get("content") or instr.get("text") or "").strip()
            if content:
                parts.append(content)
        existing_instr_text = "\n\n---\n\n".join(parts)

    # Key guidance distilled from Databricks Genie Space Playbook (Step 3.1)
    INSTR_BEST_PRACTICES = """\
BEST PRACTICES (Databricks Genie Space Playbook — Step 3.1):
Target length: 50–100 lines. Use this 8-section structure:

## Role  (5–10 lines)
"You are a [role] helping [users] answer questions about [domain]. Your goal is to [objective]."
Behavioural rules: be concise; ask for clarification if unclear; do not choose filter values without instruction.

## Instructions  (general behavioural rules, 3–5 lines)

## Critical Rules  (10–20 lines — table-type-specific, REQUIRED)
• Fact table:    "This table contains one row per [event]. Default filters always apply: [list]."
• Metrics mart:  "This table has ONE row per metric per dimension per time period.
                  To query a metric you MUST filter by metric_alias (e.g. 'Revenue').
                  Volume metrics: SUM(value_denom). Ratio metrics: try_divide(SUM(value_num), SUM(value_denom))."
• Wide table:    describe grain + any required filters.

## Default Filters  (filters that always apply, e.g. is_deleted = false)

## Business Terms  (10–15 lines — domain-specific definitions, synonyms, abbreviations only)

## Date Handling  (5–10 lines — real date column name(s), fiscal year offset if any, default period)

## Dimension Hierarchies  (5–10 lines — e.g. Region > Country > City)

## Data Quality Notes  (known NULLs, stale data, caveats)

WHAT NOT TO INCLUDE:
• SQL examples (belong in SQL Queries tab)
• Column lists or schema descriptions (Genie reads UC metadata directly)
• Inline SQL in text
• Verbose explanations or step-by-step guides
• Emphatic overrides ("NEVER do X") — fix the data model instead
"""

    # Build existing-instructions context block (rewrite mode)
    existing_section = ""
    if existing_instr_text:
        issues = []
        if schema_hits > 3:
            issues.append("- Contains schema/data dictionary content — remove it (Genie reads UC metadata directly)")
        if inline_sql_blocks:
            issues.append(f"- Contains inline SQL ({', '.join(inline_sql_blocks[:2])}) — move to SQL Queries tab")
        if emphatic_blocks:
            issues.append(f"- Contains emphatic overrides ({', '.join(emphatic_blocks[:2])}) — replace with structural rules")
        if total_instr_lines > 100:
            issues.append(f"- Too long ({total_instr_lines} lines) — trim to 50–100 lines")
        issues_str = "\n".join(issues) if issues else "- No specific issues flagged; improve domain specificity."
        existing_section = (
            f"\nEXISTING INSTRUCTIONS (rewrite and improve — do not just copy):\n"
            f"{existing_instr_text[:3000]}\n\n"
            f"Issues to fix:\n{issues_str}\n"
        )

    lineage_hint = (
        f"\nLINEAGE CONTEXT (use to infer business relationships):\n{lineage_block[:600]}\n"
        if lineage_block else ""
    )

    instructions_prompt = f"""You are a Databricks Genie configuration expert. Generate a complete, ready-to-paste Genie space instructions block for a space called "{space_name_str}".

{INSTR_BEST_PRACTICES}
TABLE METADATA (primary source — use real column names and business context from this):
{metadata_block}{lineage_hint}{existing_section}
TASK:
Write a complete instructions block using the 8-section template above. Requirements:
1. Fill every section with real, domain-specific content inferred from the table metadata — no generic placeholders like "[define here]" unless you genuinely cannot infer the value (then use [placeholder: description]).
2. Infer the table type (fact / metrics mart / wide) and use the matching Critical Rules pattern.
3. Use the actual date column name(s) from the metadata in Date Handling.
4. Identify coded or status columns (e.g. type flags, status codes) and define their values in Business Terms.
5. Keep total length 50–100 lines.
6. Output ONLY the instructions block — no preamble, no explanation, no markdown code fences."""

    mode_str = (
        f"rewrite of existing instructions (assessed: {a4l})"
        if existing_instr_text else "generate from scratch (no existing instructions)"
    )
    print(DIVIDER)
    print("INSTRUCTIONS DRAFT  (LLM-generated · best-practice-guided)")
    print(f"Model: {LLM_MODEL}  |  Mode: {mode_str}")
    print(DIVIDER)
    instructions_output = llm_query(instructions_prompt, max_tokens=2000)
    if instructions_output.startswith("[LLM unavailable"):
        print(f"⚠ LLM unavailable — instructions generation skipped. {instructions_output}")
        instructions_output = ""
    else:
        print(instructions_output)
    print(DIVIDER)
    print("\n💡 Review this draft before deploying:")
    print("   • Verify all inferred values (dates, filters, business terms) against your actual data")
    print("   • Remove any [placeholder] items you cannot fill in yet")
    print("   • Copy into: Configuration > Instructions > Text tab")

# ── SQL Query Generator (LLM-drafted, coverage-matrix-guided) ─────────────────
sql_output = ""
if a3 < 3:
    # Build list of patterns to generate — target gaps only when examples exist,
    # generate the full coverage matrix when starting from scratch
    if sql_count == 0:
        patterns_needed = [
            "aggregation (SUM/COUNT + GROUP BY)",
            "date/time filtering with :param_name syntax for date values",
            "top-N ranking (ORDER BY ... LIMIT)",
            "period-over-period comparison (current vs previous period)",
            "multi-table JOIN" if table_count > 1 else "filtered query with WHERE clause",
        ]
    else:
        patterns_needed = []
        if not has_agg_example:     patterns_needed.append("aggregation (SUM/COUNT + GROUP BY)")
        if not has_date_example:    patterns_needed.append("date/time filtering with :param_name syntax for date values")
        if not has_topn_example:    patterns_needed.append("top-N ranking (ORDER BY ... LIMIT)")
        if not has_compare_example: patterns_needed.append("period-over-period comparison (current vs previous period)")
        if not has_join_example and table_count > 1:
            patterns_needed.append("multi-table JOIN")
        if not parameterised_sqls:  patterns_needed.append("parameterised query using :param_name syntax")

    if patterns_needed:
        # Distilled from Databricks Genie Space Playbook Step 3.2
        SQL_BEST_PRACTICES = """\
BEST PRACTICES (Databricks Genie Space Playbook — Step 3.2):
- Every query must be complete and self-contained — no fragments
- Use :param_name syntax for variable values (dates, filter values, IDs)
  e.g.  WHERE order_date >= :start_date AND order_date < :end_date
        AND region = :region
- Include ALL required default filters on every query
- Title should be the natural-language question a business user would type
- Do NOT hardcode specific date literals or filter values
- Use actual table and column names from the metadata provided
"""

        tables_sql = "\n".join(
            f"  {tid.split('.')[-1]} ({', '.join(c['name'] for c in meta.get('columns', [])[:20])})"
            for tid, meta in table_metadata.items()
            if "error" not in meta
        )

        joins_hint = ""
        if genie_joins:
            join_lines = []
            for j in genie_joins[:5]:
                left  = j.get("left_table",  {})
                right = j.get("right_table", {})
                lt = left.get("table_name",  left.get("name",  "?")).split(".")[-1]
                rt = right.get("table_name", right.get("name", "?")).split(".")[-1]
                lc = left.get("column_name",  left.get("join_column",  "?"))
                rc = right.get("column_name", right.get("join_column", "?"))
                join_lines.append(f"  {lt}.{lc} = {rt}.{rc}")
            joins_hint = "\nCONFIGURED JOINS (use these for multi-table queries):\n" + "\n".join(join_lines) + "\n"

        patterns_str = "\n".join(f"  {i+1}. {p}" for i, p in enumerate(patterns_needed))

        sql_prompt = f"""You are a Databricks SQL expert. Generate example SQL queries for a Genie space called "{space_name_str}".

{SQL_BEST_PRACTICES}
TABLE METADATA (use these exact table and column names):
{tables_sql}{joins_hint}
FULL TABLE METADATA (for column descriptions and types):
{metadata_block}

TASK:
Generate one complete SQL query for each of the following missing patterns:
{patterns_str}

For each query output exactly this format:
Title: <natural-language question the query answers>
SQL:
<complete SQL query>
---

Requirements:
1. Use real table and column names from the metadata above — no placeholders like [column_name]
2. Use :param_name syntax for ALL variable values (dates, filter values)
3. Include any default filters that are evident from the metadata (e.g. is_deleted, status columns)
4. For date columns, infer the correct column name from metadata
5. Each query must be complete and runnable after substituting :param values
6. If you cannot determine a required column name, use a comment: /* TODO: replace with actual column */"""

        print(DIVIDER)
        print("SQL QUERY EXAMPLES  (LLM-generated · coverage-matrix-guided)")
        print(f"Model: {LLM_MODEL}  |  Gaps to fill: {len(patterns_needed)} pattern(s)")
        print(f"Patterns: {', '.join(patterns_needed)}")
        print(DIVIDER)
        sql_output = llm_query(sql_prompt, max_tokens=2500)
        if sql_output.startswith("[LLM unavailable"):
            print(f"⚠ LLM unavailable — SQL generation skipped. {sql_output}")
            sql_output = ""
        else:
            print(sql_output)
        print(DIVIDER)
        print("\n💡 Before adding these to your space:")
        print("   • Test every query in a SQL editor — wrong examples teach Genie bad patterns")
        print("   • Replace any /* TODO */ comments with actual column names")
        print("   • Add via: Configuration > Instructions > SQL Queries tab > Add SQL Query")
