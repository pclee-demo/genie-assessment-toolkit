# Genie Space Assessment Toolkit

A structured diagnostic for Databricks Genie spaces. Run it against any space to get a scored assessment, prioritised recommendations, and ready-to-use starter templates — all in a single `.md` report.

---

## What it checks

| Area | What it looks for |
|------|-------------------|
| **0. Data & UC Readiness** | Genie joins, PK/FK constraints, table ownership, serverless warehouse, space & warehouse permissions, UC table grants |
| **1. Table & Space Curation** | Table count, data layer quality (gold vs. bronze/silver), UC tags, wide tables, space description |
| **2. Metadata Quality** | Column description coverage, table comments, coded column descriptions, entity matching candidates, date type safety, row filters / column masks |
| **3. Example SQL** | Count, query-type coverage (JOIN, aggregation, time filter, top-N, period-over-period), hardcoded values, parameterisation, table reference validity |
| **4. Instructions** | Business rules vs. schema dumps, inline SQL, emphatic overrides, instruction length |
| **5. Sample Questions** | Count and diversity (manual review prompt) |
| **6. Benchmarks** | Count, gold-standard SQL completeness, complexity coverage |
| **7. Semantic Layer** | Metric Views in space, SQL Expressions (Measures, Dimensions, Filters, Synonyms) |

**Scoring:** each area is scored 1–3 (Poor / OK / Good). Maximum score is **24/24**.

**Verdict thresholds:**
- ≥ 21/24 → **PRODUCTION READY**
- ≥ 15/24 → **NEEDS IMPROVEMENT**
- < 15/24 → **RECOMMEND REBUILD**

**Per-area minimums:** if Areas 0 (Data & UC Readiness), 2 (Metadata Quality), or 6 (Benchmarks) score Poor, the verdict is capped at **NEEDS IMPROVEMENT** regardless of total score.

---

## Output

A `.md` assessment report containing:
- Scored scorecard across all 8 areas
- Detailed findings per area with specific flags
- Prioritised recommendations with exact fix steps and UI paths
- *(If LLM available)* Domain Curation Guide — suggested table groupings for spaces with too many tables
- *(If LLM available)* Sample Questions & KPIs — 15 business questions, top 5 benchmark candidates, 5 KPI SQL Expression Measure candidates
- Starter: Instructions Template — 7-section template to copy into Configuration > Instructions
- Starter: SQL Query Templates — missing query patterns to copy into Configuration > SQL Queries

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

### 1. Import to your Databricks workspace

Import the full folder to your workspace, preserving the directory structure:

```bash
# From the repo root
databricks workspace import-dir . /Workspace/Users/<you>/genie-assessment-toolkit --overwrite
```

Or import the notebook and source files individually:

```bash
databricks workspace mkdirs /Workspace/Users/<you>/genie-assessment-toolkit/notebooks
databricks workspace mkdirs /Workspace/Users/<you>/genie-assessment-toolkit/src/genie_assessment

databricks workspace import /Workspace/Users/<you>/genie-assessment-toolkit/notebooks/01_assess_genie_space \
  --file notebooks/01_assess_genie_space.py --format SOURCE --language PYTHON --overwrite

for f in fetch score recommend llm report; do
  databricks workspace import /Workspace/Users/<you>/genie-assessment-toolkit/src/genie_assessment/$f \
    --file src/genie_assessment/$f.py --format SOURCE --language PYTHON --overwrite
done
```

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
        ├── score.py                # Scores Areas 0–7, computes verdict
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

**Priority order for a typical remediation:**
1. Area 0 — permissions and joins (space won't work at all without these)
2. Area 2 — metadata quality (biggest driver of answer accuracy)
3. Area 3 — example SQL (teaches Genie query patterns)
4. Area 7 — semantic layer (consistency of KPI calculations)
5. Areas 1, 4, 5, 6 — curation, instructions, questions, benchmarks

**Advisory flags** (prefixed in findings) don't affect the score but surface governance and best-practice gaps worth addressing before a broader rollout.
