# Databricks notebook source
# ── report.py: Starter templates + markdown report builder + file writer ─────
# Requires: all variables from fetch.py + score.py + recommend.py + llm.py
# Produces: assessment .md file written to output_path

space_name    = space.get("display_name", space.get("title", "this space"))
table_names   = [t.split(".")[-1] for t in table_identifiers]
tables_str    = ", ".join(table_names) if table_names else "[your tables]"
primary_table = table_names[0] if table_names else "your_table"

# ── Starter Templates (printed for copy-paste into Genie UI) ─────────────────

# Instructions Template (shown when instructions are missing or Poor)
instructions_template = None
if a4 < 3 or not text_instructions:
    instructions_template = f"""\
## Role
You are a data assistant helping business users answer questions about {space_name}.
Your goal is to provide accurate, concise answers using the available data.

## Instructions
- Be straightforward and concise
- If a question is unclear, ask for clarification before running a query
- Do not choose filter values without specific instruction from the user
- If a question is out of scope, say so clearly

## Critical Rules
[Describe any must-know data structure rules, e.g.:]
- This table contains one row per [transaction / customer / day]
- Always filter by [required_column] = '[required_value]' unless the user specifies otherwise
- [metric_column] = 'volume' for absolute values, 'ratio' for percentages

## Default Filters
[List filters that should always apply, e.g.:]
- is_deleted = false
- status != 'cancelled'

## Business Terms
[Define domain-specific terms, synonyms, and abbreviations, e.g.:]
- Revenue: the SUM of [amount_column] where status = 'completed'
- ARR / Annual Recurring Revenue: [definition]
- [Abbreviation]: [full term and meaning]

## Date Handling
[Describe date columns and any calendar quirks, e.g.:]
- Use [date_column] for all time-based filtering
- Fiscal year runs [month] to [month] (offset from calendar year by [N] months)
- Default to the current calendar year if no time period is specified

## Dimension Hierarchies
[Describe rollup relationships, e.g.:]
- Region > Country > City
- Business Unit > Team > Individual

## Data Quality Notes
[Flag known issues users should be aware of, e.g.:]
- Data is refreshed daily at [time]; queries reflect data as of the previous day
- [column] may be NULL for records created before [date] — treat NULL as [meaning]\
"""

    print(DIVIDER)
    print("INSTRUCTIONS TEMPLATE")
    print("Go to: Configuration > Instructions > Text tab")
    print(DIVIDER)
    print(instructions_template)
    print(DIVIDER)
    print()

# SQL Query Templates (shown when Example SQL is Poor or OK)
sql_templates = []
if a3 < 3:
    if not has_agg_example:
        sql_templates.append(("Aggregation — total by dimension", f"""\
SELECT
    [dimension_column],
    SUM([metric_column]) AS total_metric
FROM {primary_table}
WHERE [date_column] >= '[start_date]'
  AND [date_column] <  '[end_date]'
GROUP BY [dimension_column]
ORDER BY total_metric DESC"""))

    if not has_join_example:
        tbl2 = table_names[1] if len(table_names) > 1 else "dimension_table"
        sql_templates.append(("Multi-table JOIN", f"""\
SELECT
    d.[label_column],
    SUM(f.[metric_column]) AS total_metric
FROM {primary_table} f
JOIN {tbl2} d ON f.[foreign_key] = d.[primary_key]
WHERE f.[date_column] >= '[start_date]'
  AND f.[date_column] <  '[end_date]'
GROUP BY d.[label_column]
ORDER BY total_metric DESC"""))

    if not has_date_example:
        sql_templates.append(("Time trend — monthly", f"""\
SELECT
    DATE_TRUNC('month', [date_column]) AS month,
    SUM([metric_column])               AS total_metric
FROM {primary_table}
WHERE [date_column] >= '[start_date]'
  AND [date_column] <  '[end_date]'
GROUP BY 1
ORDER BY 1"""))

    if not has_topn_example:
        sql_templates.append(("Top-N ranking", f"""\
SELECT
    [dimension_column],
    SUM([metric_column]) AS total_metric
FROM {primary_table}
WHERE [date_column] >= '[start_date]'
  AND [date_column] <  '[end_date]'
GROUP BY [dimension_column]
ORDER BY total_metric DESC
LIMIT 10"""))

    if not has_compare_example:
        sql_templates.append(("Period-over-period comparison", f"""\
SELECT
    [dimension_column],
    SUM(CASE WHEN [date_column] >= '[current_start]'  AND [date_column] < '[current_end]'  THEN [metric_column] ELSE 0 END) AS current_period,
    SUM(CASE WHEN [date_column] >= '[previous_start]' AND [date_column] < '[previous_end]' THEN [metric_column] ELSE 0 END) AS previous_period,
    try_divide(
        SUM(CASE WHEN [date_column] >= '[current_start]'  AND [date_column] < '[current_end]'  THEN [metric_column] ELSE 0 END)
        - SUM(CASE WHEN [date_column] >= '[previous_start]' AND [date_column] < '[previous_end]' THEN [metric_column] ELSE 0 END),
        SUM(CASE WHEN [date_column] >= '[previous_start]' AND [date_column] < '[previous_end]' THEN [metric_column] ELSE 0 END)
    ) AS pct_change
FROM {primary_table}
GROUP BY [dimension_column]
ORDER BY current_period DESC"""))

    if not parameterised_sqls:
        sql_templates.append(("Parameterised query (use :param_name syntax)", f"""\
SELECT
    [dimension_column],
    SUM([metric_column]) AS total_metric
FROM {primary_table}
WHERE [date_column] >= :start_date
  AND [date_column] <  :end_date
  AND [filter_column] = :filter_value
GROUP BY [dimension_column]
ORDER BY total_metric DESC"""))

    if sql_templates:
        print(DIVIDER)
        print("SQL QUERY TEMPLATES  (missing patterns)")
        print("Go to: Configuration > Instructions > SQL Queries tab > Add SQL Query")
        print(DIVIDER)
        for title, sql in sql_templates:
            print(f"\nTitle: {title}")
            print("SQL:")
            print(sql)
            print()
        print(DIVIDER)

# ── Build Markdown Report ─────────────────────────────────────────────────────
today_str    = datetime.utcnow().strftime("%Y-%m-%d")
BAR_MD = {3: "🟢 Good", 2: "🟡 OK  ", 1: "🔴 Poor"}

md_lines = [
    f"# Genie Assessment: {space_name}",
    f"**Date:** {today_str}  |  **Space ID:** {SPACE_ID}  |  **Score:** {total_str}",
    "",
    f"> {verdict_note}",
    "",
    "---",
    "",
    "## Scorecard",
    "",
    "| Area | Score | Rating |",
    "|------|-------|--------|",
]
for area, score, label, area_flags in zip(areas, scores, labels, flags):
    rating    = "⬜ N/A" if label == "N/A" else BAR_MD[score]
    score_str = "N/A"   if label == "N/A" else f"{score}/3"
    md_lines.append(f"| {area} | {score_str} | {rating} |")

md_lines += [
    f"| **TOTAL** | **{total_str}** | |",
    "",
    "---",
    "",
    "## Assessment by Area",
    "",
]

# Build rec lookup: short area name → (severity, items)
_recs_lookup = {rec_area: (sev, items) for rec_area, sev, items in recs}

for area, score, label, area_flags in zip(areas, scores, labels, flags):
    rating    = "⬜ N/A" if label == "N/A" else BAR_MD[score]
    score_str = "N/A"   if label == "N/A" else f"{score}/3"
    md_lines.append(f"### {area} — {rating} ({score_str})")

    if label == "N/A":
        md_lines.append("_Not scored (N/A)_")
    elif not area_flags:
        md_lines.append("✅ No issues found.")
    else:
        for flag in area_flags:
            if flag.startswith("✅"):
                md_lines.append(f"- {flag}")
            else:
                md_lines.append(f"- ⚠ {flag}")

    # Append matching recommendation inline (rec area is the short name after "N. ")
    _short = area.split(". ", 1)[1] if ". " in area else area
    if _short in _recs_lookup:
        _sev, _items = _recs_lookup[_short]
        md_lines.append("")
        md_lines.append("**Next steps:**")
        for item in _items:
            md_lines.append(f"- {item}")

    md_lines += ["", "---", ""]

# Domain curation section (only if LLM ran)
if domain_output:
    # Downgrade any top-level headings in LLM output so they sit below the section heading
    _domain_md = re.sub(r'^#{1,3} ', '#### ', domain_output, flags=re.MULTILINE)
    md_lines += [
        "---",
        "",
        "## Domain Curation Guide (LLM-generated)",
        "",
        "_Too many tables in one space degrades Genie accuracy. "
        "Use the guide below to split into focused spaces before onboarding business users._",
        "",
        _domain_md,
        "",
    ]

# Sample questions section
if question_output:
    _question_md = re.sub(r'^#{1,3} ', '#### ', question_output, flags=re.MULTILINE)
    md_lines += [
        "---",
        "",
        "## Sample Questions & KPIs (LLM-generated)",
        "",
        "_Add top questions as **Sample Questions** and **Benchmarks**; turn KPIs into **SQL Expression Measures**._",
        "",
        _question_md,
        "",
    ]

# Instructions section — prefer LLM-generated draft, fall back to static template
_instr_content = instructions_output or instructions_template
if _instr_content:
    if instructions_output:
        _instr_title = "Instructions Draft (LLM-generated)"
        _instr_desc  = (
            "These instructions were drafted by the LLM from your table metadata using Databricks best practices. "
            "They are a starting point — not final copy. "
            "Before deploying: verify all inferred values (date columns, status codes, filters), "
            "remove any `[placeholder]` items you cannot fill in yet, and keep total length under 100 lines."
        )
        _instr_note  = "> Review, edit, then copy into: **Configuration > Instructions > Text tab**"
    else:
        _instr_title = "Starter: Instructions Template"
        _instr_desc  = (
            "Genie instructions should contain business rules that cannot be inferred from Unity Catalog metadata — "
            "things like fiscal year definitions, default filters, KPI formulas, and NULL semantics. "
            "The template below provides a recommended 8-section structure. "
            "Fill in the bracketed placeholders, remove any sections that don't apply, and keep the total under 100 lines. "
            "Do not copy in schema descriptions or column lists — Genie reads those directly from UC."
        )
        _instr_note  = "> Copy into: **Configuration > Instructions > Text tab**"
    md_lines += [
        "---",
        "",
        f"## {_instr_title}",
        "",
        _instr_desc,
        "",
        _instr_note,
        "",
        "```",
        _instr_content,
        "```",
        "",
    ]

# SQL section — prefer LLM-generated queries, fall back to static placeholder templates
if sql_output:
    # Parse "Title: ...\nSQL:\n...\n---" blocks into formatted markdown
    _sql_md = []
    for _block in re.split(r'\n?---+\n?', sql_output.strip()):
        _block = _block.strip()
        if not _block:
            continue
        _title_m = re.match(r'^Title:\s*(.+?)(?:\n|$)', _block, re.IGNORECASE)
        _sql_m   = re.search(r'\bSQL:\s*\n([\s\S]+)', _block, re.IGNORECASE)
        if _title_m and _sql_m:
            _sql_md += [f"#### {_title_m.group(1).strip()}", "", "```sql", _sql_m.group(1).strip(), "```", ""]
        elif _block:
            _sql_md += [_block, ""]

    md_lines += [
        "---",
        "",
        "## SQL Query Examples (LLM-generated)",
        "",
        "_Test every query before adding — incorrect examples actively teach Genie wrong patterns. "
        "Aim for 10–15 total covering all pattern types._",
        "",
        "> **Configuration > Instructions > SQL Queries tab > Add SQL Query**",
        "",
        *_sql_md,
    ]
elif sql_templates:
    md_lines += [
        "---",
        "",
        "## Starter: SQL Query Templates",
        "",
        ("Example SQL queries teach Genie the query patterns your business uses — aggregations, joins, "
         "time filters, top-N rankings, and period-over-period comparisons. "
         "The templates below cover the patterns currently missing from your space. "
         "Replace the bracketed placeholders with your actual table and column names, "
         "test each query, then add it in Configuration > Instructions > SQL Queries tab. "
         "Aim for 10–15 examples in total, each covering a distinct query shape."),
        "",
        "> Copy into: **Configuration > Instructions > SQL Queries tab > Add SQL Query**",
        "",
    ]
    for title, sql in sql_templates:
        md_lines += [
            f"### {title}",
            "",
            "```sql",
            sql,
            "```",
            "",
        ]

md_content = "\n".join(md_lines)

# ── Resolve output path ───────────────────────────────────────────────────────
if OUTPUT_PATH.strip():
    output_path = OUTPUT_PATH.strip()
    # UC Volumes path: pass through as-is; Workspace path: pass through as-is
else:
    try:
        current_user = (
            spark.sql("SELECT current_user()").collect()[0][0]
        )
    except Exception:
        current_user = "unknown"
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", space_name)
    output_path = f"/Workspace/Users/{current_user}/genie-assessments/{safe_name}_{today_str}.md"

# ── Write the file ────────────────────────────────────────────────────────────
import os

if output_path.startswith("/Volumes/"):
    # UC Volumes: use dbutils.fs (dbfs:/Volumes/ prefix)
    dbfs_path = "dbfs:" + output_path
    try:
        dbutils.fs.mkdirs(dbfs_path.rsplit("/", 1)[0])
        dbutils.fs.put(dbfs_path, md_content, overwrite=True)
        print(f"\n✅ Assessment report saved to: {output_path}")
    except Exception as e:
        print(f"\n⚠ Could not write to {output_path}: {e}")
        print(md_content)
else:
    # /Workspace/ paths: plain Python open() — works directly in Databricks
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            f.write(md_content)
        print(f"\n✅ Assessment report saved to: {output_path}")
    except Exception as e:
        print(f"\n⚠ Could not write to {output_path}: {e}")
        print(md_content)
