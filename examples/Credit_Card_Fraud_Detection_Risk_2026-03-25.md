# Genie Assessment: Credit Card Fraud Detection & Risk
**Date:** 2026-03-25  |  **Space ID:** 01f127def4531f6f953c89015655fee9  |  **Score:** 10/18

> Good foundation — work through the flagged areas below to get this space production-ready

---

## Scorecard

| Area | Score | Rating |
|------|-------|--------|
| 0. Data & UC Readiness | 1/3 | 🔴 Poor |
| 1. Table & Space Curation | 3/3 | 🟢 Good |
| 2. Metadata Quality | 1/3 | 🔴 Poor |
| 3. Genie Instructions Configuration | 2/3 | 🟡 OK   |
| 4. Sample Questions | 2/3 | 🟡 OK   |
| 5. Benchmarks | 1/3 | 🔴 Poor |
| **TOTAL** | **10/18** | |

---

## Assessment by Area

### 0. Data & UC Readiness — 🔴 Poor (1/3)
- ⚠ No PK/FK constraints found in UC — add PRIMARY KEY and FOREIGN KEY constraints so Genie can automatically infer join relationships
- ⚠ No UC tags found on any table — consider tagging with domain, data classification, and sensitivity labels for governance
- ⚠ Tables with no SELECT grants beyond the owner (8): `account_profiles`, `fraud_risk_state`, `login_events`, `transactions`, `metrics_account_profiles` (+3 more) — grant SELECT to the relevant users, groups, or service principal
- ✅ Metric Views in space: `metrics_account_profiles`, `metrics_login_events`, `metrics_fraud_risk_state`, `metrics_transactions` — semantic layer (measures, dimensions, filters) is defined at the UC layer

**Next steps:**
- No PK/FK constraints in UC — add PRIMARY KEY / FOREIGN KEY constraints so Genie can automatically infer join paths without relying solely on configured joins
- Tables with no SELECT grants: `account_profiles`, `fraud_risk_state`, `login_events`, `transactions` — grant SELECT to the relevant users or groups

---

### 1. Table & Space Curation — 🟢 Good (3/3)
- ⚠ Layer unclear for: `account_profiles`, `fraud_risk_state`, `login_events`, `transactions` — apply UC tags (e.g. layer=gold) to make data tier explicit; see: ALTER TABLE ... SET TAGS ('layer' = 'gold')

---

### 2. Metadata Quality — 🔴 Poor (1/3)
- ⚠ Generic/technical descriptions on: `transactions.transaction_id`
- ⚠ Coded columns missing description of their values (40): `account_profiles.account_type`, `fraud_risk_state.risk_category`, `transactions.merchant_type`, `transactions.transaction_type` (+36 more) — add plain-English descriptions to the UC column comment (e.g. 'A=Active, I=Inactive') and enable Entity Matching in Configuration > Data so Genie can resolve user-typed values
- ⚠ Date/time columns stored as `STRING` (9): `login_events.timestamp`, `transactions.timestamp`, `metrics_account_profiles.transaction_date` — cast to `DATE`/`TIMESTAMP` or Genie cannot do date arithmetic
- ⚠ Business abbreviations in column names with no synonym or instruction mapping (1): MFA (Multi-Factor Authentication) — add a SQL Expression Synonym for each so Genie resolves full terms, or define them in Instructions > Business Terms

**Next steps:**
- Description coverage is 100% but data quality issues are preventing Genie from resolving business queries correctly — see flagged items above: coded columns and date/string type issues are the primary blockers
- For coded/status columns: add plain-English descriptions to the UC column comment (e.g. 'A=Active, I=Inactive') AND enable Entity Matching in Configuration > Data so Genie can resolve user-typed values to actual codes
- Cast date/time columns stored as STRING to DATE or TIMESTAMP — Genie cannot do date arithmetic on strings
- Business abbreviations with no synonym or instruction mapping: MFA (Multi-Factor Authentication) — add a SQL Expression Synonym for each (Configuration > SQL Expressions > Synonym) so Genie resolves user queries using the full term; or define them under Business Terms in the Instructions tab

---

### 3. Genie Instructions Configuration — 🟡 OK (2/3)
- ⚠ SQL Queries tab: hardcoded values in: What are the most common warning signals appearing; Which accounts currently have a critical risk scor; How many fraud risk cases are still awaiting analy — use `:param_name` syntax
- ⚠ SQL Queries tab: no parameterised queries (`:param_name` syntax) — parameterised queries become Trusted Assets: when Genie matches a user question to one, the response is automatically labelled Trusted, signalling a domain expert has verified the answer path
- ⚠ SQL Queries tab: missing pattern examples for: multi-table JOINs, date/time filtering, period-over-period / YoY comparisons
- ⚠ SQL Queries tab: queries reference tables not in this space (7): "What are the most common warning signals" → `assessments`; "What is the spread of risk scores for bu" → `assessments`; "Which accounts currently have a critical" → `accounts` (+4 more) — update to reference tables actually in this space
- ⚠ Text tab: missing sections — role / behavior, critical rules, business terms, date handling; structure instructions so Genie can parse rules reliably
- ⚠ SQL examples are misaligned with sample questions — sample questions ask for comparisons but no period-over-period SQL examples exist. Genie learns patterns from SQL examples; if questions and examples don't match, users will get poor results on the questions you've featured

**Next steps:**
- SQL Queries tab: 15 queries (count OK) — fix quality issues flagged above
- Fill pattern gaps: multi-table JOINs, date/time filtering, period-over-period / YoY comparisons — see the SQL Query Examples section below
- Replace hardcoded values in: What are the most common warning signals appearing; Which accounts currently have a critical risk scor; How many fraud risk cases are still awaiting analy — use `:param_name` syntax
- SQL/question alignment gaps: sample questions ask for comparisons but no period-over-period SQL examples exist — add examples that match your featured sample questions
- Text tab: add missing sections — role / behavior, critical rules, business terms, date handling
- See the Instructions Draft (LLM-generated) section below for a ready-to-use rewrite

---

### 4. Sample Questions — 🟡 OK (2/3)
- ⚠ Only 7 sample questions — recommend 10+ to guide business users
- ⚠ Manually verify questions cover a range of metrics, dimensions, time periods, and user personas — 10 questions all asking about the same metric provide less value than 10 questions spread across different business areas and question types
- ⚠ Tables with no sample question coverage (2): `metrics_fraud_risk_state`, `metrics_login_events` — users will never discover what these tables can answer; add at least one sample question per table that showcases its key metrics or use case

**Next steps:**
- Add 3 more sample questions to reach 10
- Diversify across user personas (manager, analyst, ops, compliance) — don't cluster around one question type
- Include at least one question per major business metric or KPI
- Phrase as natural-language questions business users would actually type, not SQL-like queries
- Tables with no question coverage: `metrics_fraud_risk_state`, `metrics_login_events` — add sample questions that feature these tables so users discover what they can answer; if no good questions exist, consider whether these tables belong in this space

---

### 5. Benchmarks — 🔴 Poor (1/3)
- ⚠ No benchmarks — add at least 5 question/SQL pairs to validate Genie's accuracy

**Next steps:**
- BLOCKING: No benchmarks configured — you cannot measure or prove Genie's accuracy without them
- Do not present this space to business users or stakeholders without at least 5 benchmarks passing
- Use the top 5 questions from the Sample Question & KPI Generator above as your first benchmarks
- Each benchmark requires a manually verified, tested SQL query as the gold standard
- Run benchmarks after every change to instructions, tables, or SQL examples

---

## Sample Questions & KPIs (LLM-generated)

_Add top questions as **Sample Questions** and **Benchmarks**; turn KPIs into **SQL Expression Measures**._

#### Sample Questions
1. How many transactions were flagged as high risk in the last 30 days? (aggregation)
2. Which fraud risk categories are most common this quarter? (top-N)
3. How has the number of fraud reviews changed month over month? (trend)
4. What percentage of transactions from high-risk account profiles were recommended for blocking? (aggregation)
5. Which home cities have the highest number of high-risk account profiles? (top-N)
6. How do fraud risk scores compare between domestic and international transactions? (comparison)
7. How many successful logins came from new devices in the past 7 days? (aggregation)
8. Which accounts had both an impossible-travel login and a high-risk transaction on the same day? (filter)
9. How does the average transaction amount compare between high-risk and standard account profiles? (comparison)
10. What are the top fraud reasons associated with blocked transactions? (top-N)
11. How many login attempts failed in the last 24 hours for accounts without multi-factor authentication enabled? (filter)
12. What trend do we see in international transactions labeled as risky over time? (trend)
13. Which analysts reviewed the most fraud cases this month? (top-N)
14. How often does the analyst's action differ from the recommended action? (comparison)
15. Which merchant categories have the highest number of risky transactions? (top-N)

#### Benchmark Questions
1. How many transactions were flagged as high risk in the last 30 days? (aggregation)
2. How has the number of fraud reviews changed month over month? (trend)
3. What percentage of transactions from high-risk account profiles were recommended for blocking? (aggregation)
4. Which accounts had both an impossible-travel login and a high-risk transaction on the same day? (filter)
5. How often does the analyst's action differ from the recommended action? (comparison)

#### KPIs

**KPI name:** High-Risk Transaction Volume
**Description:** Tracks the number of transactions identified as high risk within a selected time period.
**Formula hint:** COUNT of transactions marked as high risk during the reporting period

**KPI name:** Block Recommendation Rate
**Description:** Measures how often risky transactions are recommended for blocking.
**Formula hint:** COUNT of risky transactions with a block recommendation divided by total risky transactions

**KPI name:** Fraud Review Throughput
**Description:** Shows how many fraud cases analysts reviewed over a given period.
**Formula hint:** COUNT of fraud assessments reviewed during the reporting period

**KPI name:** New Device Login Rate
**Description:** Monitors the share of login events coming from devices not previously seen for the account.
**Formula hint:** COUNT of logins from new devices divided by total login events

**KPI name:** Analyst Override Rate
**Description:** Tracks how often the analyst's final action differs from the system's recommended action.
**Formula hint:** COUNT of fraud cases where analyst action does not match recommended action divided by total reviewed fraud cases

---

## Instructions Draft (LLM-generated)

These instructions were drafted by the LLM from your table metadata using Databricks best practices. They are a starting point — not final copy. Before deploying: verify all inferred values (date columns, status codes, filters), remove any `[placeholder]` items you cannot fill in yet, and keep total length under 100 lines.

> Review, edit, then copy into: **Configuration > Instructions > Text tab**

```
## Role
You are a fraud analytics and risk intelligence assistant helping fraud analysts, risk managers,
operations leaders, and investigators answer questions about credit card fraud, suspicious logins,
risky transactions, and account risk patterns.
Your goal is to provide accurate, concise answers grounded in the available account, transaction,
login, fraud assessment, and metrics data.
Help users analyze fraud trends, operational case handling, geographic anomalies, risky account
behavior, and authentication signals.
Use the most appropriate table based on the question: event-level tables for detailed investigation
and metrics tables for aggregated KPI and trend analysis.
Be concise and business-oriented in summaries.
Ask for clarification when the request is ambiguous, especially for time period, metric, risk
segment, or action status.
Do not choose specific filter values such as account_type, risk_category, fraud_type,
analyst_action, or city unless the user asks for a default summary across all values.

## Instructions
Prefer metrics tables for trend, KPI, and grouped summary questions.
Prefer fact tables for record-level investigation, top suspicious events, and drill-downs by
account_id or transaction_id.
If the user asks for "rate," "share," "conversion," or "percentage," calculate the numerator and
denominator explicitly from the relevant records.
Round percentages to two decimals and currency amounts to whole units unless the user requests
higher precision.

## Critical Rules
transactions is a fact table. One row per transaction event.
Use for transaction counts, amounts, merchant patterns, channel patterns, international activity,
and fraud labels tied to transactions. Use timestamp as the event time.

login_events is a fact table. One row per login event.
Use for authentication trends, failed logins, MFA behavior, new device activity, and impossible
travel signals. Use timestamp as the event time.

fraud_risk_state is a fact table. One row per fraud risk assessment for a transaction.
Use for analyst workflow, risk score analysis, recommended_action, reviewed_by, and
key_signals/risk_reason analysis. Use timestamp for creation time and updated_at for latest
review timing.

account_profiles is a wide dimension-style table. One row per account_id.
Use for account segmentation, home city analysis, typical_txn_amount benchmarking, and high-risk
profile counts. Do not sum typical_txn_amount across accounts.

metrics_account_profiles, metrics_fraud_risk_state, metrics_login_events, and metrics_transactions
are metrics marts. Each has ONE row per metric per dimension per time period.
To query a metric, you MUST filter by metric_alias.
Volume metrics: SUM(value_denom). Ratio metrics: try_divide(SUM(value_num), SUM(value_denom)).
Prefer metrics marts for KPI trends and grouped summaries; fact tables for event-level detail.

## Default Filters
No always-on row exclusion filter is defined. Do not apply hidden filters on risk category,
account type, analyst action, country, merchant type, or channel unless requested.
If no time range is provided, ask for the intended period rather than assuming one.

## Business Terms
ATO = account takeover.
MFA = multi-factor authentication.
Fraud assessment = a record in fraud_risk_state evaluating a transaction's fraud risk.
High-risk profile = an account where is_high_risk_profile is true.
Approval rate = approved/allowed events divided by total assessed events.
Recommendation alignment rate = cases where analyst_action matches recommended_action divided by
assessed cases with both values present.
Login failure rate = failed login events divided by total login events.

## Date Handling
Use transactions.timestamp for transaction event time.
Use login_events.timestamp for login event time.
Use fraud_risk_state.timestamp for assessment creation time.
Use fraud_risk_state.updated_at when the user asks about latest review or analyst update timing.
Use transaction_date, login_date, fraud_assessment_date in metrics tables for aggregated queries.
Use transaction_month and fraud_assessment_month for monthly trends in metrics tables.
No fiscal calendar offset — use standard calendar periods.
If no time period is given, default to the latest 30 days and state that assumption.

## Dimension Hierarchies
Geography: account_home_city / home_city > broader geo grouping.
Risk operations: fraud_risk_category > fraud_risk_reason > key_fraud_signals.
Decision workflow: recommended_action > analyst_action > reviewed_by.
Account risk: account_type > high_risk_profile_flag.

## Data Quality Notes
reviewed_by and analyst_action may be null for unreviewed or pending fraud assessments.
fraud_type and risk_label may not be populated for every transaction.
typical_txn_amount is an account-level benchmark — compare to transaction amount, do not sum.
```

---

## SQL Query Examples (LLM-generated)

_Test every query before adding — incorrect examples actively teach Genie wrong patterns. Aim for 10–15 total covering all pattern types._

> **Configuration > Instructions > SQL Queries tab > Add SQL Query**

#### How many high-risk transactions did we see by day in a selected time range?

```sql
SELECT
  CAST(timestamp AS DATE) AS transaction_date,
  COUNT(*) AS high_risk_transaction_count,
  SUM(amount) AS total_high_risk_amount
FROM transactions
WHERE timestamp >= :start_date
  AND timestamp < :end_date
  AND risk_label = :risk_label
GROUP BY CAST(timestamp AS DATE)
ORDER BY transaction_date
```

#### How does the number of flagged fraud assessments in the current period compare to the previous period?

```sql
WITH current_period AS (
  SELECT COUNT(*) AS current_flagged_assessments
  FROM fraud_risk_state
  WHERE timestamp >= :current_start_date
    AND timestamp < :current_end_date
    AND risk_category = :risk_category
),
previous_period AS (
  SELECT COUNT(*) AS previous_flagged_assessments
  FROM fraud_risk_state
  WHERE timestamp >= :previous_start_date
    AND timestamp < :previous_end_date
    AND risk_category = :risk_category
)
SELECT
  c.current_flagged_assessments,
  p.previous_flagged_assessments,
  c.current_flagged_assessments - p.previous_flagged_assessments AS absolute_change,
  CASE
    WHEN p.previous_flagged_assessments = 0 THEN NULL
    ELSE ROUND(
      100.0 * (c.current_flagged_assessments - p.previous_flagged_assessments)
             / p.previous_flagged_assessments, 2)
  END AS percent_change
FROM current_period c
CROSS JOIN previous_period p
```

#### Which transactions were preceded by suspicious login activity on the same account and device?

```sql
SELECT
  t.transaction_id,
  t.account_id,
  t.timestamp AS transaction_timestamp,
  t.amount,
  t.merchant_type,
  t.risk_label,
  l.login_id,
  l.timestamp AS login_timestamp,
  l.login_success,
  l.mfa_enabled,
  l.is_new_device,
  l.is_impossible_travel,
  l.failed_attempts_prior_24h
FROM transactions t
INNER JOIN login_events l
  ON t.account_id = l.account_id /* verify join */
 AND t.device_id = l.device_id /* verify join */
WHERE t.timestamp >= :start_date
  AND t.timestamp < :end_date
  AND l.timestamp >= :login_start_date
  AND l.timestamp < :login_end_date
  AND t.risk_label = :risk_label
  AND l.is_impossible_travel = :is_impossible_travel
ORDER BY t.timestamp DESC, l.timestamp DESC
```

#### What is the average transaction amount for accounts of a selected type and risk profile?

```sql
SELECT
  ap.account_type,
  ap.is_high_risk_profile,
  COUNT(*) AS transaction_count,
  AVG(t.amount) AS avg_transaction_amount,
  SUM(t.amount) AS total_transaction_amount
FROM account_profiles ap
INNER JOIN transactions t
  ON ap.account_id = t.account_id /* verify join */
WHERE t.timestamp >= :start_date
  AND t.timestamp < :end_date
  AND ap.account_type = :account_type
  AND ap.is_high_risk_profile = :is_high_risk_profile
GROUP BY ap.account_type, ap.is_high_risk_profile
ORDER BY total_transaction_amount DESC
```
