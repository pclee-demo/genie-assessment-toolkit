# Genie Assessment: FinanceFirst Bank — Retail & Wealth
**Date:** 2026-03-19  |  **Space ID:** 01f1233c935c1cbb972daee59daa8031  |  **Score:** 13/24  |  **Verdict:** RECOMMEND REBUILD

> Significant rework needed — consider starting with a focused domain

---

## Scorecard

| Area | Score | Rating |
|------|-------|--------|
| 0. Data & UC Readiness | 1/3 | 🔴 Poor |
| 1. Table & Space Curation | 2/3 | 🟡 OK   |
| 2. Metadata Quality | 1/3 | 🔴 Poor |
| 3. Example SQL | 2/3 | 🟡 OK   |
| 4. Instructions | 2/3 | 🟡 OK   |
| 5. Sample Questions | 2/3 | 🟡 OK   |
| 6. Benchmarks | 2/3 | 🟡 OK   |
| 7. Semantic Layer | 1/3 | 🔴 Poor |
| **TOTAL** | **13/24** | | |

---

## Findings by Area

### 0. Data & UC Readiness
- ⚠ No Genie joins configured — with 7 tables, add joins via Configuration > Joins to enable accurate multi-table queries
- ⚠ No PK/FK constraints found in UC — add PRIMARY KEY and FOREIGN KEY constraints so Genie can automatically infer join relationships
- ⚠ No UC tags found on any table — consider tagging with domain, data classification, and sensitivity labels for governance
- ⚠ SQL warehouse is not accessible to non-admin users — business users will get permission errors when Genie runs queries; grant CAN_USE in SQL > SQL Warehouses > [warehouse] > Permissions
- ⚠ Tables with no SELECT grants beyond the owner (7): customers, accounts, transactions, loans, credit_cards (+2 more) — grant SELECT to the relevant users, groups, or service principal

### 1. Table & Space Curation
- ⚠ Layer unclear for: customers, accounts, transactions, loans, credit_cards (+2 more) — apply UC tags (e.g. layer=gold) to make data tier explicit; see: ALTER TABLE ... SET TAGS ('layer' = 'gold')
- ⚠ Space description missing: key metrics/KPIs — follow template: purpose + Key Metrics + Dimensions + Data grain + Target Users

### 2. Metadata Quality
- ⚠ Tables with thin comments (<50 chars): loans (48 chars), credit_cards (21 chars), portfolios (22 chars) — describe the grain, scope, key metrics, and relationships in plain English
- ⚠ Generic/technical descriptions on: customers.customer_id, customers.relationship_mgr_id, accounts.customer_id, transactions.txn_id, transactions.account_id (+4 more)
- ⚠ Technical column names with no description (2): transactions.reference_no, holdings.weight_pct — add a UC column comment, or set a display name override in Configuration > Data so business users see plain-English names instead of abbreviations
- ⚠ Coded columns missing description of their values (9): customers.state, customers.postcode, transactions.merchant_category, loans.collateral_type (+5 more) — add plain-English descriptions to the UC column comment (e.g. 'A=Active, I=Inactive') and enable Entity Matching in Configuration > Data so Genie can resolve user-typed values

### 3. Example SQL
- ⚠ Only 6 SQL examples — recommend 10+ to teach query patterns
- ⚠ Hardcoded values in: Total deposits by account type; Monthly transaction volume; Transactions for a specific account — hardcoded — use :param_name syntax
- ⚠ Missing query-type examples for: period-over-period / YoY comparisons

### 4. Instructions
- ⚠ Emphatic override patterns (**CRITICAL**, !!ALWAYS!!) in: "Business Rules & Data Notes" — these signal workarounds; fix the underlying join/data model or use the Joins tab instead
- ⚠ Long instruction blocks (>300 chars): "Business Rules & Data Notes" (1690 chars) — split into single-topic blocks

### 5. Sample Questions
- ⚠ Only 7 sample questions — recommend 10+ to guide business users
- ⚠ Manually verify questions cover a range of metrics, dimensions, time periods, and user personas — 10 questions all asking about the same metric provide less value than 10 questions spread across different business areas and question types

### 6. Benchmarks
- ⚠ Only 8 benchmarks — target is 15-30; add more covering each key metric, dimension, and complexity tier
- ⚠ 1 benchmark(s) have no gold standard SQL — add expected SQL for each
- ⚠ Hardcoded values in benchmark SQL: What is the total credit card balance across all a; How many accounts does each customer have on avera — use representative but generalisable queries

### 7. Semantic Layer
- ⚠ No Metric Views found and SQL Expressions API is not reachable — manually verify Configuration > SQL Expressions in the Genie UI: add Measures (KPI formulas), Dimensions (column aliases), Filters (metric-mart identifiers), and Synonyms (abbreviations)

---

## Recommended Next Steps

### 1. 🔴 CRITICAL  —  Data & UC Readiness
- BLOCKING: No Genie joins and no PK/FK constraints — with 7 tables, Genie cannot reliably answer multi-table questions
- Add joins immediately: Configuration > Joins — define the ON condition for every table pair users are likely to query together
- Add PK/FK constraints in UC: ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY / FOREIGN KEY — Genie uses these to automatically infer join paths
- Without at least one of these, Genie will guess join conditions and produce incorrect results
- BLOCKING: Warehouse not accessible to non-admins — grant CAN_USE in SQL > SQL Warehouses > Permissions
- Tables with no SELECT grants: customers, accounts, transactions, loans — grant SELECT to the relevant users or groups

### 2. 🟡 IMPROVE  —  Table & Space Curation
- Prune to the most essential tables for the target use case — check the Domain Curation Guide below for specific grouping suggestions
- Layer unclear for 7 table(s) — apply UC tags to make data tier explicit: ALTER TABLE <name> SET TAGS ('layer' = 'gold') — also consider silver/bronze if these aren't curated yet
- Use Configuration > Data to hide internal IDs, ETL metadata, and deprecated columns

### 3. 🔴 CRITICAL  —  Metadata Quality
- BLOCKING: Only 53% of columns are described — Genie cannot reliably interpret your data until this is above 80%
- Prioritise the 38 undescribed columns, starting with those used in filters, aggregations, and joins
- Use ALTER TABLE ... ALTER COLUMN ... COMMENT or the Databricks UI to add descriptions in bulk
- For coded/status columns: add plain-English descriptions to the UC column comment (e.g. 'A=Active, I=Inactive') AND enable Entity Matching in Configuration > Data so Genie can resolve user-typed values to actual codes

### 4. 🟡 IMPROVE  —  Example SQL
- You have 6 examples — add 4 more to reach 10 (target: 10–15)
- Fill coverage gaps: period-over-period / YoY comparisons — use the SQL Templates below
- Replace hardcoded values in: Total deposits by account type; Monthly transaction volume; Transactions for a specific account — hardcoded — use :param_name syntax
- Include complete queries with all required default filters — not fragments

### 5. 🟡 IMPROVE  —  Instructions
- REFINE: Instructions exist but contain content that conflicts with or duplicates UC metadata
- Remove schema/data dictionary content (1 pattern(s) detected) — replace with business rules
- Replace emphatic overrides in: "Business Rules & Data Notes" — fix the underlying data model or use Join definitions instead
- Split long instruction blocks: "Business Rules & Data Notes" (1690 chars)
- Add any missing business rules: fiscal year, default date range, NULL semantics, KPI definitions

### 6. 🟡 IMPROVE  —  Sample Questions
- Add 3 more sample questions to reach 10
- Diversify across user personas (manager, analyst, ops, compliance) — don't cluster around one question type
- Include at least one question per major business metric or KPI
- Phrase as natural-language questions business users would actually type, not SQL-like queries

### 7. 🟡 IMPROVE  —  Benchmarks
- You have 8 benchmarks — add 7 more to reach 15 (target: 20–30 for production confidence)
- Run benchmarks now and record current pass rate as your baseline before making further changes
- Expand coverage: 2–3 questions per key metric, per dimension, and per complexity tier (40% simple / 40% medium / 20% complex)
- Mix time-period types: specific dates, relative periods (last 30 days), and YoY comparisons
- Add gold-standard SQL to the 1 benchmark(s) that are missing it
- Target 80%+ pass rate before onboarding business users

### 8. 🔴 CRITICAL  —  Semantic Layer — SQL Expressions
- No semantic layer detected — Genie has no reusable definitions for your KPIs, dimensions, or business terminology
- PREFERRED: Create Metric Views in Unity Catalog (measures, dimensions, filters defined at the data layer)
- ALTERNATIVE: Define SQL Expressions in Genie — Configuration > SQL Expressions:
-   • Measures: KPI formulas (e.g. Total Revenue = SUM(sales_fact.amount))
-   • Dimensions: column aliases (e.g. Sales Region = sales_fact.region)
-   • Filters: metric-mart identifiers (e.g. metric_alias = 'Revenue')
-   • Synonyms: abbreviations (e.g. ASP, Average Selling Price, Avg Price)

---

## Domain Curation Guide (LLM-assisted)

This space contains too many tables across multiple business domains, which degrades Genie's accuracy. The guide below — generated from your table metadata — identifies distinct domains, recommends which tables belong in each, and suggests a focused name for each resulting space. Use it to plan a table reduction before onboarding business users.

Based on the table metadata, I've identified the following:

**Business Domains:**

1. **Customer Management**: Customer information and relationships.
2. **Account and Transaction Management**: Bank account balances, transactions, and related products.
3. **Lending and Credit**: Loan and credit card products.
4. **Investments**: Investment portfolios and holdings.

**Table Assignments:**

1. **Customer Management**: customers, employees (not present, but referenced as a foreign key)
2. **Account and Transaction Management**: accounts, transactions
3. **Lending and Credit**: loans, credit_cards
4. **Investments**: portfolios, holdings

**Cross-Domain Join Dependencies:**

* customers table is referenced by accounts, loans, credit_cards, and portfolios, indicating a need to resolve these dependencies before splitting.

**Genie Space Names:**

1. **Customer Management**: "FinanceFirst Bank — Customer Profiles"
2. **Account and Transaction Management**: "FinanceFirst Bank — Account Transactions"
3. **Lending and Credit**: "FinanceFirst Bank — Lending Products"
4. **Investments**: "FinanceFirst Bank — Investment Portfolios"

**Redundant or Out-of-Scope Tables:**

* None appear redundant, but the employees table (referenced as a foreign key) is missing and should be included in the Customer Management domain.

---

## Sample Questions & KPIs (LLM-generated)

These questions and KPIs were generated from your table metadata. Use them in three ways: (1) add the best ones as **Sample Questions** (Configuration > Sample Questions) to give business users a starting point when they open the space; (2) use the top 5 marked questions as **Benchmarks** (Configuration > Benchmarks) with a manually verified SQL answer to measure and track Genie's accuracy over time; (3) use the **KPIs** as the basis for **SQL Expression Measures** (Configuration > SQL Expressions) — each KPI should become a named, reusable formula so Genie can answer questions about it consistently without recalculating from scratch every time.

### Questions a Business User Would Ask

1. What is the total number of customers we have across all segments? (Aggregation)
2. How many new customers were onboarded in the last quarter? (Filter)
3. What is the trend of savings account balances over the past year? (Trend over time)
4. Which customer segment has the highest average account balance? (Comparison)
5. What are the top 5 most common transaction types for retail customers? (Top-N)
6. How many active credit cards do we have, and what is the total credit limit? (Aggregation)
7. What is the average interest rate for term deposit accounts? (Aggregation)
8. How many customers have a pending Know Your Customer (KYC) status? (Filter)
9. What is the monthly growth rate of new loan applications? (Trend over time)
10. **What are the top 10 customers with the highest total investment portfolio value?** (Top-N) - Benchmark
11. How many transactions are processed through the mobile channel each month? (Aggregation)
12. What is the average age of our premier customers? (Aggregation)
13. **What is the total value of all outstanding loans, and how does it compare to the previous year?** (Comparison) - Benchmark
14. **What percentage of customers have more than one account type?** (Aggregation) - Benchmark
15. **How many customers are at risk of missing a loan payment, based on their current outstanding balance and due date?** (Filter) - Benchmark

### Benchmark Questions

The following five questions are marked as benchmark questions because they cover critical aspects of the business, including customer segmentation, loan and investment performance, and risk management:

1. What are the top 10 customers with the highest total investment portfolio value?
2. What is the total value of all outstanding loans, and how does it compare to the previous year?
3. What percentage of customers have more than one account type?
4. How many customers are at risk of missing a loan payment, based on their current outstanding balance and due date?
5. **What is the average customer lifetime value for our retail segment?** (Aggregation) - Benchmark

### KPIs to Monitor

1. **Monthly Active Customers**
Measures: The number of customers who have performed at least one transaction in the last 30 days.
Formula Hint: COUNT of distinct customers with at least one transaction in the last 30 days.

2. **Total Investment Portfolio Value**
Measures: The total market value of all investment portfolios.
Formula Hint: SUM of total portfolio values for all customers.

3. **Loan Default Risk**
Measures: The percentage of customers with outstanding loans who are at risk of missing a payment.
Formula Hint: COUNT of customers with loans past due divided by the total number of customers with loans.

4. **Average Customer Balance**
Measures: The average balance across all customer accounts.
Formula Hint: SUM of all account balances divided by the total number of accounts.

5. **Credit Utilization Rate**
Measures: The average percentage of available credit used by customers with credit cards.
Formula Hint: SUM of current credit card balances divided by the SUM of credit limits for all credit cards.

---

## Starter: Instructions Template

Genie instructions should contain business rules that cannot be inferred from Unity Catalog metadata — things like fiscal year definitions, default filters, KPI formulas, and NULL semantics. The template below provides a recommended 7-section structure. Fill in the bracketed placeholders, remove any sections that don't apply, and keep the total under 100 lines. Do not copy in schema descriptions or column lists — Genie reads those directly from UC.

> Copy into: **Configuration > Instructions > Text tab**

```
## Role
You are a data assistant helping business users answer questions about FinanceFirst Bank — Retail & Wealth.
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
- [column] may be NULL for records created before [date] — treat NULL as [meaning]
```

---

## Starter: SQL Query Templates

Example SQL queries teach Genie the query patterns your business uses — aggregations, joins, time filters, top-N rankings, and period-over-period comparisons. The templates below cover the patterns currently missing from your space. Replace the bracketed placeholders with your actual table and column names, test each query, then add it in Configuration > Instructions > SQL Queries tab. Aim for 10–15 examples in total, each covering a distinct query shape.

> Copy into: **Configuration > Instructions > SQL Queries tab > Add SQL Query**

### Period-over-period comparison

```sql
SELECT
    [dimension_column],
    SUM(CASE WHEN [date_column] >= '[current_start]'  AND [date_column] < '[current_end]'  THEN [metric_column] ELSE 0 END) AS current_period,
    SUM(CASE WHEN [date_column] >= '[previous_start]' AND [date_column] < '[previous_end]' THEN [metric_column] ELSE 0 END) AS previous_period,
    try_divide(
        SUM(CASE WHEN [date_column] >= '[current_start]'  AND [date_column] < '[current_end]'  THEN [metric_column] ELSE 0 END)
        - SUM(CASE WHEN [date_column] >= '[previous_start]' AND [date_column] < '[previous_end]' THEN [metric_column] ELSE 0 END),
        SUM(CASE WHEN [date_column] >= '[previous_start]' AND [date_column] < '[previous_end]' THEN [metric_column] ELSE 0 END)
    ) AS pct_change
FROM customers
GROUP BY [dimension_column]
ORDER BY current_period DESC
```
