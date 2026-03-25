# Genie Space Assessment Toolkit

A structured diagnostic for Databricks Genie spaces. Run it against any space to get a scored assessment, prioritised recommendations, and ready-to-use starter templates — all in a single `.md` report.

---

## What it checks

| Area | What it looks for |
|------|-------------------|
| **0. Data & UC Readiness** | Genie joins, PK/FK constraints, UC Metric Views, table ownership, serverless warehouse, space & warehouse permissions, UC table grants |
| **1. Table & Space Curation** | Base table count (metric views excluded), data layer quality (gold vs. bronze/silver), UC tags, wide tables, space description |
| **2. Metadata Quality** | Column description coverage, table comments, coded column value descriptions, entity matching candidates, date type safety, business abbreviation audit (unmapped RM/KPI/MFA etc.) |
| **3. Genie Instructions Configuration** | SQL Queries tab: count, query-type coverage (JOIN, aggregation, time filter, top-N, period-over-period), hardcoded values, parameterisation (Trusted Assets), table reference validity, SQL/sample question alignment; Text tab: structural sections, inline SQL, prose metrics/joins, instruction length; Joins tab: completeness; SQL Expressions tab: Measures, Dimensions, Synonyms (skipped if UC Metric Views present) |
| **4. Sample Questions** | Count, table coverage (flags tables with no question coverage), diversity prompt |
| **5. Benchmarks** | Count, gold-standard SQL completeness |

**Scoring:** each area is scored 1–3 (Poor / OK / Good). Maximum score is **18/18**.

**Verdict thresholds:**
- ≥ 15/18 → **PRODUCTION READY**
- ≥ 11/18 → **NEEDS IMPROVEMENT**
- < 11/18 → **RECOMMEND REBUILD**

**Per-area minimums:** if Areas 0 (Data & UC Readiness), 2 (Metadata Quality), or 5 (Benchmarks) score Poor, the verdict is capped at **NEEDS IMPROVEMENT** regardless of total score.

---

## Output

A `.md` assessment report containing:
- Scored scorecard across all 6 areas
- Detailed findings per area with specific flags
- Prioritised next steps with exact fix steps and UI paths
- *(If LLM available)* Domain Curation Guide — suggested table groupings for spaces with too many tables
- *(If LLM available)* Sample Questions & KPIs — 15 business questions, top 5 benchmark candidates, 5 KPI definitions
- *(If LLM available)* Instructions Draft — ready-to-paste rewrite using Databricks best-practice structure
- *(If LLM available)* SQL Query Examples — parameterised queries covering missing pattern gaps
- Starter: Instructions Template — fallback template when LLM is unavailable
- Starter: SQL Query Templates — fallback templates when LLM is unavailable

Reports are saved to `/Workspace/Users/<you>/genie-assessments/<space>_<date>.md` by default, or a custom path.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Databricks cluster (DBR 10+) | Any cluster type; no extra libraries needed |
| CAN_MANAGE on the Genie space | Required to read instructions, SQL examples, questions, and joins via API |
| USE CATALOG + USE SCHEMA + SELECT on the space's tables | Required for metadata, tag, and grant checks |
| **Foundation Model API** *(optional)* | Required for Domain Curation and Sample Questions sections. Needs Pay-Per-Token Foundation Models enabled (`databricks-meta-llama-3-3-70b-instruct` or equivalent). If unavailable, those sections are omitted and the rest of the assessment runs normally. |
| **System tables access** *(optional)* | Required only when **Use system tables** is set to `true`. Enables lineage-enriched LLM recommendations. Needs access to `system.lineage` and `system.access.audit` (typically workspace admin). |
| **UC admin or table owner** *(optional)* | Required for `SHOW GRANTS ON TABLE` to return full results. If unavailable, grant checks are flagged as unverified rather than failing. |

---

## Usage

### 1. Add to your Databricks workspace

In the Databricks UI: **Workspace > Git folders > Add Git folder**

Paste the repo URL: `https://github.com/pclee-demo/genie-assessment-toolkit`

The full folder structure will be cloned directly into your workspace — no CLI required.

### 2. Open the notebook

Open `notebooks/01_assess_genie_space` in your Databricks workspace.

### 3. Fill in the widgets

| Widget | Required | Description |
|--------|----------|-------------|
| **Genie Space ID** | ✅ | The ID from your Genie space URL |
| **Use system tables** | No | Set to `true` to enrich LLM recommendations with UC lineage data (requires system table access) |
| **Output path** | No | Custom path for the report (e.g. `/Volumes/catalog/schema/vol/report.md`). Leave blank to auto-save to `/Workspace/Users/<you>/genie-assessments/` |
| **LLM model** | No | Foundation Model endpoint to use for LLM sections. Defaults to `databricks-meta-llama-3-3-70b-instruct`. Select from the dropdown or type any valid serving endpoint name. |

### 4. Run all cells

The report is printed to the notebook output and saved to the output path.

---

## Project structure

```
genie-assessment-toolkit/
├── notebooks/
│   └── 01_assess_genie_space.py   # Orchestrator — widgets + %run chain
└── src/
    └── genie_assessment/
        ├── fetch.py                # API setup, space config, table metadata,
        │                           # tags, PK/FK, joins, permissions
        ├── score.py                # Scores Areas 0–5, computes verdict
        ├── recommend.py            # Tiered recommendations per area
        ├── llm.py                  # Domain curation + sample question generation
        └── report.py               # Markdown report builder + file writer
```

Each source file is a Databricks notebook run via `%run`. Variables flow between files through the shared notebook scope — `fetch.py` produces variables consumed by `score.py`, which produces variables consumed by `recommend.py`, and so on.

---

## Finding your Genie Space ID

1. Open your Genie space in the Databricks UI
2. The Space ID is in the URL: `.../genie/spaces/<SPACE_ID>`

---

## Interpreting results

**Work through findings in area order** — blockers in Area 0 (joins, permissions) will compound problems in every other area. Fix the foundation before tuning the content.

See [`examples/Credit_Card_Fraud_Detection_Risk_2026-03-25.md`](examples/Credit_Card_Fraud_Detection_Risk_2026-03-25.md) for a full sample report — credit card fraud detection space, 4 base tables + 4 UC Metric Views, score 10/18.

**Priority order for a typical remediation:**
1. Area 0 — permissions and joins (space won't work at all without these)
2. Area 2 — metadata quality (biggest driver of answer accuracy)
3. Area 3 — instructions configuration (SQL examples, instructions text, joins, SQL expressions)
4. Areas 1, 4, 5 — curation, sample questions, benchmarks

Some findings don't affect the score but surface governance and best-practice gaps worth addressing before a broader rollout.
