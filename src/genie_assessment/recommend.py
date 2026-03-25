# Databricks notebook source
# ── recommend.py: Tiered recommendations for all 7 areas ────────────────────
# Requires: all variables from fetch.py + score.py
# Produces: recs (list of (area, severity_label, [items]))

SEVERITY = {1: "🔴 CRITICAL", 2: "🟡 IMPROVE ", 3: "✅ GOOD    "}
recs = []  # list of (area, severity_label, [items])

# ── Area 0: Data & UC Readiness ───────────────────────────────────────────────
if a0 == 1:
    recs.append(("Data & UC Readiness", SEVERITY[1], [
        *(["BLOCKING: No Genie joins and no PK/FK constraints — with " + str(table_count) + " tables, "
           "Genie cannot reliably answer multi-table questions. "
           "Add joins in Configuration > Joins, or add PK/FK constraints via "
           "ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY / FOREIGN KEY"] if not genie_joins and not pk_fk_tables else []),
        *(["No PK/FK constraints in UC — add PRIMARY KEY / FOREIGN KEY constraints so Genie can "
           "automatically infer join paths without relying solely on configured joins"] if genie_joins and not pk_fk_tables else []),
        *(["BLOCKING: Space not shared with any users — add CAN_USE via Space Settings > Permissions"] if space_not_shared else []),
        *(["BLOCKING: Warehouse not accessible to non-admins — grant CAN_USE in SQL > SQL Warehouses > Permissions"] if warehouse_locked else []),
        *(["Tables with no SELECT grants: " + ", ".join(tables_no_grant[:4])
           + " — grant SELECT to the relevant users or groups"] if tables_no_grant else []),
    ]))
elif a0 == 2:
    items_a0 = []
    if table_count > 1 and join_count < tables_needing_joins:
        items_a0.append(
            f"Only {join_count} of ~{tables_needing_joins} needed join(s) configured — "
            "add missing joins in Configuration > Joins"
        )
    if pk_fk_count < table_count and pk_fk_count == 0:
        items_a0.append(
            "No PK/FK constraints in UC — add PRIMARY KEY / FOREIGN KEY to help Genie auto-resolve joins"
        )
    elif 0 < pk_fk_count < table_count:
        missing_pkfk = [t.split(".")[-1] for t in table_identifiers if t not in pk_fk_tables]
        items_a0.append(
            f"PK/FK constraints still missing on: {', '.join(missing_pkfk[:4])}"
            + (f" (+{len(missing_pkfk)-4} more)" if len(missing_pkfk) > 4 else "")
        )
    if space_not_shared:
        items_a0.append(
            "Space not shared with any users — add CAN_USE grants via Space Settings > Permissions"
        )
    if warehouse_locked:
        items_a0.append(
            "Warehouse not accessible to non-admins — grant CAN_USE in SQL > SQL Warehouses > Permissions"
        )
    if tables_no_grant:
        items_a0.append(
            "Tables with no SELECT grants beyond the owner: " + ", ".join(tables_no_grant[:4])
            + (f" (+{len(tables_no_grant)-4} more)" if len(tables_no_grant) > 4 else "")
            + " — grant SELECT to the relevant users, groups, or service principal"
        )
    if tables_grant_unknown:
        items_a0.append(
            f"Could not verify grants on {len(tables_grant_unknown)} table(s) — "
            "run SHOW GRANTS ON TABLE as a UC admin to confirm access is in place"
        )
    items_a0.append(
        "Review advisory flags above for ownership, serverless warehouse, and tagging opportunities"
    )
    recs.append(("Data & UC Readiness", SEVERITY[2], items_a0))

# ── Area 1: Table & Space Curation ───────────────────────────────────────────
if a1 == 1:
    recs.append(("Table & Space Curation", SEVERITY[1], [
        f"REBUILD SCOPE: {table_count} tables is too many — Genie accuracy degrades significantly above 10; recommend rebuilding around a single business domain",
        "Run the Domain Curation Guide below — it will suggest how to split these tables into 2–4 focused spaces",
        "Do not onboard business users until table count is reduced to ≤10 focused tables",
        "Replace any raw/bronze/silver tables with gold or semantic-layer equivalents",
        *(["Apply UC tags to clarify layer for all tables before rebuilding: "
           "ALTER TABLE <name> SET TAGS ('layer' = 'gold'|'silver'|'bronze')"
           + (" — confirmed non-gold: " + ", ".join(non_gold[:3]) if non_gold else "")] if non_gold or gold_untagged else []),
        "For wide tables: hide irrelevant columns in Configuration > Data, or create purpose-built views exposing only the columns users need",
        "Add a space description: purpose statement + Key Metrics + Dimensions + Data grain + Target Users",
    ]))
elif a1 == 2:
    recs.append(("Table & Space Curation", SEVERITY[2], [
        "Prune to the most essential tables for the target use case — check the Domain Curation Guide below for specific grouping suggestions",
        *(["Add a space description: purpose statement + Key Metrics + Dimensions + Data grain + Target Users"] if not space_desc else []),
        *(["Replace non-gold tables with gold/semantic-layer equivalents: " + ", ".join(non_gold[:5])] if non_gold else []),
        *(["Wide tables detected: " + ", ".join(wide_tables[:3])
           + " — quickest fix: hide irrelevant columns in Configuration > Data; "
           "for a permanent fix, create a purpose-built view exposing only the columns users need"] if wide_tables else []),
        *(["Layer unclear for " + str(len(gold_untagged)) + " table(s) — apply UC tags to make data tier explicit: "
           "ALTER TABLE <name> SET TAGS ('layer' = 'gold') — also consider silver/bronze if these aren't curated yet"] if gold_untagged else []),
        "Use Configuration > Data to hide internal IDs, ETL metadata, and deprecated columns",
    ]))

# ── Area 2: Metadata Quality ──────────────────────────────────────────────────
if a2 == 1:
    undescribed = total_cols - described_cols
    items_a2 = []
    if coverage_pct < 80:
        items_a2.append(f"BLOCKING: Only {coverage_pct:.0f}% of columns are described — Genie cannot reliably interpret your data until this is above 80%")
        items_a2.append(f"Prioritise the {undescribed} undescribed columns, starting with those used in filters, aggregations, and joins")
        items_a2.append("Use ALTER TABLE ... ALTER COLUMN ... COMMENT or the Databricks UI to add descriptions in bulk")
    else:
        items_a2.append(
            f"Description coverage is {coverage_pct:.0f}% but data quality issues are preventing Genie from resolving business queries correctly — "
            "see flagged items above: coded columns and date/string type issues are the primary blockers"
        )
    if len(tables_no_comment) == table_count:
        items_a2.append("No table-level comments at all — add a comment to every table describing its grain, scope, and key metrics")
    items_a2.append(
        "For coded/status columns: add plain-English descriptions to the UC column comment (e.g. 'A=Active, I=Inactive') "
        "AND enable Entity Matching in Configuration > Data so Genie can resolve user-typed values to actual codes"
    )
    if date_as_string:
        items_a2.append("Cast date/time columns stored as STRING to DATE or TIMESTAMP — Genie cannot do date arithmetic on strings")
    recs.append(("Metadata Quality", SEVERITY[1], items_a2))
elif a2 == 2:
    recs.append(("Metadata Quality", SEVERITY[2], [
        f"Description coverage is {coverage_pct:.0f}% — fill the {total_cols - described_cols} remaining gaps, focusing on columns flagged above",
        *(["Thin table comments (<50 chars) on: " + ", ".join(thin_comments[:3]) + " — expand to describe grain, scope, and key metrics"] if thin_comments else []),
        *(["Tables with no comment: " + ", ".join(tables_no_comment[:3]) + " — add a plain-English table-level description"] if tables_no_comment else []),
        "Replace FK/PK technical shorthand with plain-English explanations (e.g. 'Links to the customer record')",
        *(["Coded columns missing value descriptions: " + ", ".join(enum_missing_dict[:4])
           + (f" (+{len(enum_missing_dict)-4} more)" if len(enum_missing_dict) > 4 else "")
           + " — add plain-English descriptions to UC column comments and enable Entity Matching (Configuration > Data)"] if enum_missing_dict else []),
        *(["Cast date/time STRING columns to DATE/TIMESTAMP: " + ", ".join(date_as_string[:3])] if date_as_string else []),
    ]))

# ── Area 3: Example SQL ───────────────────────────────────────────────────────
if a3 == 1:
    recs.append(("Example SQL", SEVERITY[1], [
        f"CRITICAL GAP: Only {sql_count} SQL example(s) — Genie has almost nothing to learn query patterns from; target is 10–15",
        "Use the SQL Query Templates and Sample Question Generator below to bootstrap examples quickly",
        "Start with one example per common question type: aggregation, time filter, multi-table join, top-N, period-over-period",
        "Every example must be a complete, tested query — not fragments or comments",
        "Use :param_name syntax for any values that change (dates, filters, IDs)",
    ]))
elif a3 == 2:
    missing_str = (", ".join(missing_types) + " — use the SQL Templates below") if missing_types else "review coverage against the full query matrix"
    recs.append(("Example SQL", SEVERITY[2], [
        f"You have {sql_count} examples — add {max(0, 10 - sql_count)} more to reach 10 (target: 10–15)" if sql_count < 10
        else f"You have {sql_count} SQL examples (count is fine) — quality issues are pulling the score down: fix table references, add parameterisation, and address alignment gaps flagged above",
        f"Fill coverage gaps: {missing_str}",
        *(["Replace hardcoded values in: " + "; ".join(hardcoded_sqls[:3]) + " — use :param_name syntax"] if hardcoded_sqls else []),
        *(["No parameterised queries yet — add at least one :param_name or {{param}} example"] if not parameterised_sqls and sql_count > 0 else []),
        *(["SQL examples reference tables not in this space: " + "; ".join(sql_unknown_tables[:3])
           + " — update these to reference the tables actually in this space, "
           "otherwise Genie learns patterns against tables it cannot query"] if sql_unknown_tables else []),
        *(["SQL/question alignment gaps: " + "; ".join(alignment_gaps)
           + " — prioritise adding SQL examples that match your featured sample questions"] if alignment_gaps else []),
        "Include complete queries with all required default filters — not fragments",
    ]))

# ── Area 4: Instructions ──────────────────────────────────────────────────────
if a4 == 1:
    recs.append(("Instructions", SEVERITY[1], [
        "REPLACE: Current instructions are not helping Genie — delete and rewrite from scratch using the Instructions Template below",
        *(["Delete all schema/data dictionary content — Genie reads Unity Catalog metadata directly; this content is noise"] if schema_hits > 3 else []),
        *(["Remove inline SQL from text instructions (" + ", ".join(inline_sql_blocks[:2]) + ") — move to the SQL Queries tab"] if inline_sql_blocks else []),
        "Use the 7-section template: Role → Instructions → Critical Rules → Default Filters → Business Terms → Date Handling → Data Quality Notes",
        "Keep total length under 100 lines — every line should be a rule Genie can't infer from metadata alone",
        "See the Instructions Draft (LLM-generated) section below for a ready-to-use rewrite based on your table metadata",
    ]))
elif a4 == 2:
    recs.append(("Instructions", SEVERITY[2], [
        *(["Remove schema/data dictionary content (" + str(schema_hits) + " pattern(s) detected) — replace with business rules; Genie reads UC metadata directly"] if schema_hits > 0 else []),
        *(["Move inline SQL to the SQL Queries tab: " + ", ".join(inline_sql_blocks[:2])] if inline_sql_blocks else []),
        *(["Replace emphatic overrides (" + ", ".join(emphatic_blocks[:2]) + ") — fix the underlying data model or use the Joins tab instead"] if emphatic_blocks else []),
        *(["Split long instruction blocks: " + "; ".join(long_blocks[:2])] if long_blocks else []),
        *(["Trim total instructions from " + str(total_instr_lines) + " lines to ≤100 lines"] if total_instr_lines > 100 else []),
        "Add any missing business rules: fiscal year, default date range, NULL semantics, KPI definitions",
        "See the Instructions Draft (LLM-generated) section below for a ready-to-use rewrite based on your table metadata",
    ]))

# ── Area 5: Sample Questions ──────────────────────────────────────────────────
if a5 == 1:
    recs.append(("Sample Questions", SEVERITY[1], [
        f"URGENT: {'No sample questions configured' if sq_count == 0 else f'Only {sq_count} sample question(s)'} — users see a blank or near-blank chat interface with no guidance",
        "Use the Sample Question & KPI Generator below to create 10+ questions immediately — no business user input required",
        "Even 5 good questions dramatically improves first-time user experience and reduces 'I don't know what to ask' drop-off",
        "Cover at least one question per major business metric and one per target user persona",
    ]))
elif a5 == 2:
    recs.append(("Sample Questions", SEVERITY[2], [
        f"Add {max(0, 10 - sq_count)} more sample questions to reach 10",
        "Diversify across user personas (manager, analyst, ops, compliance) — don't cluster around one question type",
        "Include at least one question per major business metric or KPI",
        "Phrase as natural-language questions business users would actually type, not SQL-like queries",
        *(["Tables with no question coverage: " + ", ".join(uncovered_tables)
           + " — add sample questions that feature these tables so users discover what they can answer; "
           "if no good questions exist, consider whether these tables belong in this space"] if uncovered_tables else []),
    ]))

# ── Area 6: Benchmarks ────────────────────────────────────────────────────────
if a6 == 1:
    recs.append(("Benchmarks", SEVERITY[1], [
        f"BLOCKING: {'No benchmarks configured' if bm_count == 0 else f'Only {bm_count} benchmark(s)'} — you cannot measure or prove Genie's accuracy without them",
        "Do not present this space to business users or stakeholders without at least 5 benchmarks passing",
        "Use the top 5 questions from the Sample Question & KPI Generator above as your first benchmarks",
        "Each benchmark requires a manually verified, tested SQL query as the gold standard",
        "Run benchmarks after every change to instructions, tables, or SQL examples",
    ]))
elif a6 == 2:
    bm_needed = max(0, 15 - bm_count)
    recs.append(("Benchmarks", SEVERITY[2], [
        f"You have {bm_count} benchmarks — add {bm_needed} more to reach 15 (target: 20–30 for production confidence)",
        "Run benchmarks now and record current pass rate as your baseline before making further changes",
        "Expand coverage: 2–3 questions per key metric, per dimension, and per complexity tier (40% simple / 40% medium / 20% complex)",
        "Mix time-period types: specific dates, relative periods (last 30 days), and YoY comparisons",
        *(["Add gold-standard SQL to the " + str(len(bm_missing_sql)) + " benchmark(s) that are missing it"] if bm_missing_sql else []),
        "Target 80%+ pass rate before onboarding business users",
    ]))

# ── Area 7: Semantic Layer ────────────────────────────────────────────────────
if a7 < 3:
    if metric_views_in_catalog and not metric_views_in_space:
        recs.append(("Semantic Layer — Metric Views", SEVERITY[2], [
            f"Metric View(s) found in your catalog but not added to this space: {', '.join(t.split('.')[-1] for t in metric_views_in_catalog[:5])}",
            "Add them to the space — Metric Views define measures, dimensions, and filters at the UC layer and are read directly by Genie",
            "This is the preferred approach: defining your semantic layer in UC rather than duplicating it in Genie SQL Expressions",
        ]))
    elif a7 == 1:
        recs.append(("Semantic Layer — SQL Expressions", SEVERITY[1], [
            "No semantic layer detected — Genie has no reusable definitions for your KPIs, dimensions, or business terminology",
            "PREFERRED: Create Metric Views in Unity Catalog (measures, dimensions, filters defined at the data layer)",
            "ALTERNATIVE: Define SQL Expressions in Genie — Configuration > SQL Expressions:",
            "  • Measures: KPI formulas (e.g. Total Revenue = SUM(sales_fact.amount))",
            "  • Dimensions: column aliases (e.g. Sales Region = sales_fact.region)",
            "  • Filters: metric-mart identifiers (e.g. metric_alias = 'Revenue')",
            "  • Synonyms: abbreviations (e.g. ASP, Average Selling Price, Avg Price)",
        ]))
    elif a7 == 2:
        recs.append(("Semantic Layer — SQL Expressions", SEVERITY[2], [
            f"Partial semantic layer — {expr_count} SQL Expression(s) defined but gaps remain (see flags above)",
            *(["Add Measure expressions for key KPIs — currently none defined"] if not measures_ex else []),
            *(["Add Synonyms for common abbreviations to reduce clarification prompts"] if not synonyms_ex else []),
            *(["Business abbreviations in column names with no synonym mapping: " + ", ".join(unmapped_abbrevs)
               + " — add a Synonym SQL Expression for each so Genie resolves user queries using full terms "
               "(Configuration > SQL Expressions > Synonym)"] if unmapped_abbrevs else []),
            "Aim for at least 5 expressions covering your most-used business terms before launch",
        ]))

# ── Print recommendations ─────────────────────────────────────────────────────
if recs:
    print("RECOMMENDED FIXES (priority order):\n")
    for i, (area, severity, items) in enumerate(recs, 1):
        print(f"{i}. {severity}  {area}")
        for item in items:
            print(f"   • {item}")
        print()
else:
    print("✅ No issues found — space looks production ready.")
