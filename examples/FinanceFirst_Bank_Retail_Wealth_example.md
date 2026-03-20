# Genie Assessment: FinanceFirst Bank — Retail & Wealth
**Date:** 2026-03-20  |  **Space ID:** 01f1233c935c1cbb972daee59daa8031  |  **Score:** 13/24  |  **Verdict:** RECOMMEND REBUILD

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
| **TOTAL** | **13/24** | |

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

## Sample Questions & KPIs (LLM-generated)

These questions and KPIs were generated from your table metadata. Use them in three ways: (1) add the best ones as **Sample Questions** (Configuration > Sample Questions) to give business users a starting point when they open the space; (2) use the top 5 marked questions as **Benchmarks** (Configuration > Benchmarks) with a manually verified SQL answer to measure and track Genie's accuracy over time; (3) use the **KPIs** as the basis for **SQL Expression Measures** (Configuration > SQL Expressions) — each KPI should become a named, reusable formula so Genie can answer questions about it consistently without recalculating from scratch every time.

### 15 Realistic Questions a Business User Would Ask

1. What is the total value of all investment portfolios? (Aggregation)
2. How many customers have an active loan? (Filter)
3. What is the trend of average account balance over the past year? (Trend over time)
4. Which segment of customers (retail or premier) has a higher average account balance? (Comparison)
5. What are the top 10 customers with the highest total value of investment portfolios? (Top-N)
6. How many credit cards are currently issued to customers? (Aggregation)
7. What is the total amount of transactions processed in the last quarter? (Aggregation)
8. How does the average credit limit of credit cards compare between retail and premier customers? (Comparison)
9. What is the monthly growth rate of new customers? (Trend over time)
10. How many customers have a pending Know Your Customer (KYC) verification status? (Filter)
11. What are the top 5 loan products by total principal amount? (Top-N)
12. How has the average interest rate of savings accounts changed over the past two years? (Trend over time)
13. What is the total outstanding balance of all active loans? (Aggregation)
14. How do the average transaction amounts differ between ATM and branch channels? (Comparison)
15. What are the bottom 10 customers with the lowest available credit on their credit cards? (Top-N)

### 5 Most Important Benchmark Questions

1. **What is the trend of average account balance over the past year?** (Trend over time) **(Benchmark)**
2. **What are the top 10 customers with the highest total value of investment portfolios?** (Top-N) **(Benchmark)**
3. **How many customers have an active loan?** (Filter) **(Benchmark)**
4. **What is the total amount of transactions processed in the last quarter?** (Aggregation) **(Benchmark)**
5. **What is the monthly growth rate of new customers?** (Trend over time) **(Benchmark)**

### 5 KPIs to Monitor Regularly

1. **KPI Name:** Monthly Active Customers
   - **Description:** Measures the number of customers who have had at least one transaction in the last 30 days.
   - **Formula Hint:** COUNT of distinct customers with at least one transaction in the last 30 days.

2. **KPI Name:** Average Account Balance
   - **Description:** Tracks the average current balance across all active accounts.
   - **Formula Hint:** SUM of current account balances divided by COUNT of active accounts.

3. **KPI Name:** Loan Portfolio Risk
   - **Description:** Monitors the total outstanding balance of loans that are past due.
   - **Formula Hint:** SUM of outstanding loan balances where the loan is past due.

4. **KPI Name:** Credit Card Utilization Rate
   - **Description:** Measures the average percentage of used credit compared to the total credit limit across all credit cards.
   - **Formula Hint:** AVERAGE of (current balance / credit limit) for all credit cards.

5. **KPI Name:** New Customer Acquisition Rate
   - **Description:** Tracks the number of new customers acquired over a specified period.
   - **Formula Hint:** COUNT of new customers onboarded within the last month.

---

## Instructions Draft (LLM-generated)

These instructions were drafted by the LLM from your table metadata, guided by the Databricks They are a starting point — not final copy. Before deploying: verify all inferred values (date columns, status codes, filters), remove any `[placeholder]` items you cannot fill in yet, and keep total length under 100 lines.

> Review, edit, then copy into: **Configuration > Instructions > Text tab**

```
## Role
You are a data analyst for FinanceFirst Bank — Retail & Wealth, helping users answer questions about customer banking and investment activities. Your goal is to provide accurate and relevant data insights to support business decisions.

## Instructions
Be concise and ask for clarification if a question is unclear. Do not choose filter values without explicit user instruction.

## Critical Rules
• Fact table (transactions): This table contains one row per transaction. Default filters always apply: txn_status = 'C' (completed).
• Metrics mart (accounts): This table has ONE row per account. To query an account metric, you MUST filter by account_type (e.g., 'SAV' for savings).
• Wide table (customers): This table has one row per customer, with grain at the individual customer level. Required filters: kyc_status = 'V' (verified) for product eligibility analysis.

## Default Filters
Always filter accounts by account_status = 'A' (active) unless the user asks for inactive or closed accounts.
Always filter transactions by txn_status = 'C' (completed) for volume and revenue analysis.
Portfolios: default to status = 'A' (active) unless specifically asked for inactive.

## Business Terms
KYC status: V = verified, P = pending.
Account type: SAV = savings, CHQ = transaction/cheque, TD = term deposit.
Transaction type: CR = credit (money in), DR = debit (money out).
Risk rating: internal risk classification assigned by the risk team.
Segment: RETAIL = standard, PREMIER = high net worth.

## Date Handling
Use the actual date column names: date_of_birth, onboarding_date, open_date, txn_date.
Fiscal year runs January to December (calendar year).

## Dimension Hierarchies
Region > Country > City (not directly applicable, as location data is not provided).
Customer segment: RETAIL > PREMIER.

## Data Quality Notes
The accounts table may contain inactive or suspended accounts.
The transactions table may contain pending or failed transactions.
Portfolios and holdings data may be subject to daily refresh delays.
```

---

## SQL Query Examples (LLM-generated)

These queries were generated from your table metadata to fill the coverage gaps identified by the assessment. Each query uses your real column names with `:param_name` syntax for variable values. **Test every query in a SQL editor before adding it to your space** — incorrect examples actively teach Genie wrong patterns. Aim for 10–15 total examples covering all pattern types.

> Test first, then add via: **Configuration > Instructions > SQL Queries tab > Add SQL Query**

Title: What is the period-over-period comparison of total transactions for the current vs previous period for customers in a specific segment?
SQL:
```sql
WITH current_period AS (
  SELECT 
    SUM(amount) AS total_transactions
  FROM 
    transactions
  WHERE 
    txn_date >= :start_date AND 
    txn_date < :end_date AND 
    account_status = 'A' AND 
    txn_status = 'C'
),
previous_period AS (
  SELECT 
    SUM(amount) AS total_transactions
  FROM 
    transactions
  WHERE 
    txn_date >= :previous_start_date AND 
    txn_date < :previous_end_date AND 
    account_status = 'A' AND 
    txn_status = 'C'
),
customer_segment AS (
  SELECT 
    customer_id
  FROM 
    customers
  WHERE 
    segment = :segment AND 
    kyc_status = 'V'
)
SELECT 
  cp.total_transactions AS current_period_transactions,
  pp.total_transactions AS previous_period_transactions,
  (cp.total_transactions - pp.total_transactions) / pp.total_transactions * 100 AS period_over_period_change
FROM 
  current_period cp
  CROSS JOIN previous_period pp
  JOIN customer_segment cs ON cs.customer_id = (SELECT customer_id FROM accounts WHERE account_id = (SELECT account_id FROM transactions LIMIT 1))
WHERE 
  cs.segment = :segment;
```
However, the given query is not fully correct because the join condition between `customer_segment` and the other two CTEs or subqueries is missing and also we cannot directly join `customer_segment` with `current_period` or `previous_period` as there is no common column between them.

Here is a more accurate query:
```sql
WITH current_period AS (
  SELECT 
    SUM(t.amount) AS total_transactions
  FROM 
    transactions t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
  WHERE 
    t.txn_date >= :start_date AND 
    t.txn_date < :end_date AND 
    a.account_status = 'A' AND 
    t.txn_status = 'C' AND 
    c.segment = :segment AND 
    c.kyc_status = 'V'
),
previous_period AS (
  SELECT 
    SUM(t.amount) AS total_transactions
  FROM 
    transactions t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
  WHERE 
    t.txn_date >= :previous_start_date AND 
    t.txn_date < :previous_end_date AND 
    a.account_status = 'A' AND 
    t.txn_status = 'C' AND 
    c.segment = :segment AND 
    c.kyc_status = 'V'
)
SELECT 
  cp.total_transactions AS current_period_transactions,
  pp.total_transactions AS previous_period_transactions,
  (cp.total_transactions - pp.total_transactions) / pp.total_transactions * 100 AS period_over_period_change
FROM 
  current_period cp
  CROSS JOIN previous_period pp;
```
Note: The above query assumes that `:start_date`, `:end_date`, `:previous_start_date`, `:previous_end_date`, and `:segment` are parameters that will be replaced with actual values. 

Also, the query assumes that the `txn_date` column in the `transactions` table represents the date of the transaction, and that the `account_status` column in the `accounts` table represents the status of the account (where 'A' means active). The `txn_status` column in the `transactions` table represents the status of the transaction (where 'C' means completed). The `segment` column in the `customers` table represents the segment of the customer, and the `kyc_status` column represents the Know Your Customer verification status (where 'V' means verified).

The query calculates the total transactions for the current period and the previous period, and then calculates the period-over-period change as a percentage. The result is a single row with three columns: `current_period_transactions`, `previous_period_transactions`, and `period_over_period_change`. 

Please adjust the query according to your actual requirements and data. 

---
Title: What is the total value of all portfolios for customers in a specific segment as of a specific date?
SQL:
```sql
SELECT 
  SUM(p.total_value) AS total_portfolio_value
FROM 
  portfolios p
  JOIN customers c ON p.customer_id = c.customer_id
WHERE 
  p.status = 'ACTIVE' AND 
  c.segment = :segment AND 
  c.kyc_status = 'V' AND 
  p.inception_date <= :as_of_date;
```
Note: This query assumes that the `as_of_date` parameter represents the date as of which the total portfolio value is to be calculated.

---
Title: What is the total outstanding balance of all loans for customers in a specific segment as of a specific date?
SQL:
```sql
SELECT 
  SUM(l.outstanding_balance) AS total_outstanding_balance
FROM 
  loans l
  JOIN customers c ON l.customer_id = c.customer_id
WHERE 
  l.loan_status = 'ACT' AND 
  c.segment = :segment AND 
  c.kyc_status = 'V' AND 
  l.start_date <= :as_of_date;
```
Note: This query assumes that the `as_of_date` parameter represents the date as of which the total outstanding balance is to be calculated.

---
Title: What is the total available credit of all credit cards for customers in a specific segment as of a specific date?
SQL:
```sql
SELECT 
  SUM(cc.available_credit) AS total_available_credit
FROM 
  credit_cards cc
  JOIN customers c ON cc.customer_id = c.customer_id
WHERE 
  cc.card_status = 'ACTIVE' AND 
  c.segment = :segment AND 
  c.kyc_status = 'V' AND 
  cc.issue_date <= :as_of_date;
```
Note: This query assumes that the `as_of_date` parameter represents the date as of which the total available credit is to be calculated.
