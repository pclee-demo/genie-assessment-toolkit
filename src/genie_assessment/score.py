# Databricks notebook source
# ── score.py: Score Areas 0–7 + verdict ──────────────────────────────────────
# Requires: all variables from fetch.py + SPACE_ID, USE_SYSTEM_TABLES
# Produces: a0–a7, a0l–a7l, a0_flags–a7_flags, scores, labels, areas, flags,
#           total, max_total, total_str, verdict, verdict_note,
#           plus intermediate vars used by recommend.py and report.py

# ── Area 0: Data & UC Readiness ───────────────────────────────────────────────
table_count          = len(table_identifiers)
a0_flags             = []
join_count           = len(genie_joins)
tables_needing_joins = max(0, table_count - 1)   # N tables need at least N-1 joins
pk_fk_count          = len(pk_fk_tables)
unowned_tables       = [t.split(".")[-1] for t in table_identifiers
                        if not table_metadata.get(t, {}).get("owner")]
tagged_count         = sum(1 for t in table_identifiers if table_tags.get(t))

# Score: driven by joins + PK/FK (structural correctness for multi-table queries)
if table_count <= 1:
    # Single-table space — joins N/A; score on PK/FK presence
    if pk_fk_count >= 1: a0, a0l = 3, "Good"
    else:                a0, a0l = 2, "OK"
elif join_count >= tables_needing_joins and pk_fk_count >= 1:
    a0, a0l = 3, "Good"
elif join_count >= 1 or pk_fk_count >= 1:
    a0, a0l = 2, "OK"
else:
    a0, a0l = 1, "Poor"

if table_count > 1 and join_count == 0:
    a0_flags.append(
        f"No Genie joins configured — with {table_count} tables, add joins via "
        "Configuration > Joins to enable accurate multi-table queries"
    )
elif table_count > 1 and join_count < tables_needing_joins:
    a0_flags.append(
        f"Only {join_count} join(s) defined for {table_count} tables — verify all "
        "necessary table relationships are configured in Configuration > Joins"
    )

if pk_fk_count == 0 and table_count > 1:
    a0_flags.append(
        "No PK/FK constraints found in UC — add PRIMARY KEY and FOREIGN KEY constraints "
        "so Genie can automatically infer join relationships"
    )
elif 0 < pk_fk_count < table_count:
    missing_pkfk = [t.split(".")[-1] for t in table_identifiers if t not in pk_fk_tables]
    a0_flags.append(
        f"PK/FK constraints missing on: {', '.join(missing_pkfk[:5])}"
        + (f" (+{len(missing_pkfk)-5} more)" if len(missing_pkfk) > 5 else "")
        + " — add constraints so Genie can automatically resolve join paths"
    )

if unowned_tables:
    a0_flags.append(
        f"Tables without assigned owner: {', '.join(unowned_tables[:5])}"
        + (f" (+{len(unowned_tables)-5} more)" if len(unowned_tables) > 5 else "")
        + " — assign ownership in UC for governance and accountability"
    )
if is_serverless is False:
    a0_flags.append(
        "Space is not using a Serverless SQL warehouse — serverless is "
        "recommended for Genie (lower latency, auto-scaling, no cold-start penalty)"
    )
if not tagged_count and table_count > 0:
    a0_flags.append(
        "No UC tags found on any table — consider tagging with domain, "
        "data classification, and sensitivity labels for governance"
    )

# ── Permissions checks ────────────────────────────────────────────────────────
def _can_use_principals(acl):
    """Return non-admin principals explicitly granted CAN_USE."""
    result = []
    for entry in acl:
        principal = (entry.get("user_name") or
                     entry.get("group_name") or
                     entry.get("service_principal_name", ""))
        levels = [p.get("permission_level", "") for p in entry.get("all_permissions", [])]
        if principal and principal.lower() != "admins" and "CAN_USE" in levels:
            result.append(principal)
    return result

space_not_shared = bool(space_acl) and len(_can_use_principals(space_acl)) == 0
warehouse_locked = bool(warehouse_acl) and bool(warehouse_id) and len(_can_use_principals(warehouse_acl)) == 0

tables_no_grant      = []
tables_grant_unknown = []
for table_id, grants in table_grants.items():
    tname = table_id.split(".")[-1]
    if grants is None:
        tables_grant_unknown.append(tname)
        continue
    owner = (table_metadata.get(table_id, {}).get("owner") or "").lower()
    has_external_grant = any(
        str(r.get("action_type", r.get("ActionType", ""))).upper() in ("SELECT", "ALL_PRIVILEGES")
        and str(r.get("principal", r.get("Principal", ""))).lower() != owner
        for r in grants
    )
    if not has_external_grant:
        tables_no_grant.append(tname)

if space_not_shared:
    a0_flags.append(
        "Space has no CAN_USE grants — not shared with any users or groups; "
        "add permissions via Space Settings > Permissions before onboarding business users"
    )
if warehouse_locked:
    a0_flags.append(
        "SQL warehouse is not accessible to non-admin users — business users will get "
        "permission errors when Genie runs queries; grant CAN_USE in "
        "SQL > SQL Warehouses > [warehouse] > Permissions"
    )
if tables_no_grant:
    a0_flags.append(
        f"Tables with no SELECT grants beyond the owner ({len(tables_no_grant)}): "
        f"{', '.join(tables_no_grant[:5])}"
        + (f" (+{len(tables_no_grant)-5} more)" if len(tables_no_grant) > 5 else "")
        + " — grant SELECT to the relevant users, groups, or service principal"
    )
if tables_grant_unknown:
    a0_flags.append(
        f"Could not verify grants on {len(tables_grant_unknown)} table(s): "
        f"{', '.join(tables_grant_unknown[:3])}"
        + (f" (+{len(tables_grant_unknown)-3} more)" if len(tables_grant_unknown) > 3 else "")
        + " — run SHOW GRANTS ON TABLE as a UC admin to verify access"
    )

# Downgrade Area 0 score for permissions blockers
if space_not_shared or (tables_no_grant and len(tables_no_grant) >= max(1, table_count // 2)):
    if a0 == 3: a0, a0l = 2, "OK"
    elif a0 == 2: a0, a0l = 1, "Poor"
elif warehouse_locked or tables_no_grant:
    if a0 == 3: a0, a0l = 2, "OK"

# ── Area 1: Table & Space Curation ───────────────────────────────────────────
table_count = len(table_identifiers)
schemas     = list(set(".".join(t.split(".")[:2]) for t in table_identifiers))

if table_count <= 5:
    a1, a1l = 3, "Good"
elif table_count <= 9:
    a1, a1l = 2, "OK"
else:
    a1, a1l = 1, "Poor"

a1_flags = []
if len(schemas) > 1:
    a1_flags.append(f"Tables span {len(schemas)} schemas — consider splitting by domain")
if table_count > 9:
    a1_flags.append(f"{table_count} tables is too many; Genie accuracy degrades above ~9")

NON_GOLD_KEYWORDS  = ["raw", "bronze", "silver", "landing", "staging", "ingest", "stg_", "ods_"]
GOLD_TAG_KEYS      = {"layer", "data_tier", "medallion_layer", "data_layer"}
GOLD_TAG_VALUES    = {"gold", "platinum", "curated", "serving", "semantic"}
NON_GOLD_TAG_VALUES= {"bronze", "silver", "raw", "landing", "staging", "ingest"}

non_gold      = []   # confirmed non-gold (tagged or keyword match)
gold_untagged = []   # layer unclear — no tags, no keyword signal

for t in table_identifiers:
    meta  = table_metadata.get(t, {})
    ttype = meta.get("table_type", "").upper()
    tags  = table_tags.get(t, {})
    tname = t.split(".")[-1]

    # Views and Metric Views are gold-equivalent by definition
    if ttype in ("VIEW", "METRIC_VIEW"):
        continue

    # UC tags are authoritative — check them first
    tag_layer = next(
        (v.lower() for k, v in tags.items() if k.lower() in GOLD_TAG_KEYS),
        None
    )
    if tag_layer is not None:
        if tag_layer in NON_GOLD_TAG_VALUES:
            non_gold.append(f"{tname} (tagged: {tag_layer})")
        # if tagged gold/curated/etc — nothing to flag
    elif any(kw in t.lower() for kw in NON_GOLD_KEYWORDS):
        # keyword heuristic when no tag exists
        non_gold.append(tname)
    else:
        # no tag, no keyword signal — layer is unclear
        gold_untagged.append(tname)

if non_gold:
    a1_flags.append(f"Non-gold tables detected: {', '.join(non_gold)} — replace with gold/semantic-layer equivalents")
if gold_untagged:
    a1_flags.append(
        f"Layer unclear for: {', '.join(gold_untagged[:5])}"
        + (f" (+{len(gold_untagged)-5} more)" if len(gold_untagged) > 5 else "")
        + " — apply UC tags (e.g. layer=gold) to make data tier explicit; "
        "see: ALTER TABLE ... SET TAGS ('layer' = 'gold')"
    )

wide_tables = []
for table_id, meta in table_metadata.items():
    if "error" not in meta and len(meta.get("columns", [])) > 50:
        wide_tables.append(f"{table_id.split('.')[-1]} ({len(meta['columns'])} cols)")
if wide_tables:
    a1_flags.append(
        f"Wide tables (>50 cols): {', '.join(wide_tables)} — hide irrelevant columns in "
        "Configuration > Data (quickest fix) or create a purpose-built view exposing only "
        "the columns business users need"
    )

_DESC_LABEL_MAP = {
    "metrics":    "key metrics/KPIs",
    "dimensions": "dimensions/segments",
    "grain":      "data grain/freshness",
    "users":      "target users/audience",
}

def _llm_judge_desc(desc_text):
    """Ask the LLM whether the description covers the four required elements.
    Returns a list of missing element labels, or None on failure."""
    _prompt = f"""Does this Genie space description cover each element below? Be generous — implicit or indirect coverage counts.
Reply with ONLY a JSON object — no other text.

Elements:
1. metrics   — mentions any metrics, KPIs, measures, scores, amounts, counts, or what can be analysed (implicit is fine)
2. dimensions — mentions any dimensions, segments, groupings, categories, types, or breakdowns (implicit is fine)
3. grain     — mentions data grain, event type, object type, or what each record represents (implicit is fine)
4. users     — mentions intended users, audience, teams, roles, or the business function it serves (implicit is fine)

Description: "{desc_text}"

JSON format: {{"metrics": true/false, "dimensions": true/false, "grain": true/false, "users": true/false}}"""
    try:
        _r = requests.post(
            f"{host}/serving-endpoints/{LLM_MODEL}/invocations",
            headers=headers,
            json={"messages": [{"role": "user", "content": _prompt}], "max_tokens": 80},
            timeout=15,
        )
        _raw = _r.json()["choices"][0]["message"]["content"] if _r.status_code == 200 else ""
        _match = re.search(r'\{[^}]+\}', _raw, re.DOTALL)
        if _match:
            _verdict = json.loads(_match.group())
            return [_DESC_LABEL_MAP[k] for k, v in _verdict.items() if not v and k in _DESC_LABEL_MAP]
    except Exception:
        pass
    return None

space_desc = (space.get("description", "") or "").strip()
if not space_desc:
    a1_flags.append("Space has no description — add one: purpose statement + Key Metrics + Dimensions + Data grain/freshness + Target Users")
elif len(space_desc) < 80:
    a1_flags.append(f"Space description too short ({len(space_desc)} chars) — expand to cover key metrics, dimensions, data grain, and target users")
else:
    _missing = _llm_judge_desc(space_desc)
    if _missing and len(_missing) >= 2:
        a1_flags.append(f"Space description missing: {', '.join(_missing)} — follow template: purpose + Key Metrics + Dimensions + Data grain + Target Users")

# ── Area 2: Metadata Quality ──────────────────────────────────────────────────
total_cols, described_cols = 0, 0
generic_flags, tables_no_desc, tables_no_comment, thin_comments = [], [], [], []

GENERIC_PAT   = [r"\bPK\b", r"\bFK\b", r"foreign key", r"primary key",
                 r"stores the", r"references the", r"surrogate key"]
ENUM_NAME_PAT = re.compile(r"(status|type|code|flag|indicator|category|tier|class|mode|state)$", re.IGNORECASE)
VALUE_DICT_PAT = re.compile(r"\w+=\w+|\bY/N\b|\btrue/false\b", re.IGNORECASE)
enum_missing_dict = []

for table_id, meta in table_metadata.items():
    if "error" in meta:
        continue
    tc = (meta.get("table_comment") or "").strip()
    if not tc:
        tables_no_comment.append(table_id.split(".")[-1])
    elif len(tc) < 50:
        thin_comments.append((table_id.split('.')[-1], len(tc)))

    cols = meta.get("columns", [])
    col_described = 0
    for col in cols:
        total_cols += 1
        comment  = col.get("comment", "").strip()
        col_name = col.get("name", "")
        if comment:
            described_cols += 1
            col_described  += 1
            if any(re.search(p, comment, re.IGNORECASE) for p in GENERIC_PAT):
                generic_flags.append(f"{table_id.split('.')[-1]}.{col_name}")
        if ENUM_NAME_PAT.search(col_name) and not VALUE_DICT_PAT.search(comment):
            enum_missing_dict.append(f"{table_id.split('.')[-1]}.{col_name}")
    if col_described == 0:
        tables_no_desc.append(table_id.split(".")[-1])

date_as_string = []
DATE_NAME_PAT  = re.compile(r"date|timestamp|time|_at$|_on$|_dt$", re.IGNORECASE)
for table_id, meta in table_metadata.items():
    if "error" in meta:
        continue
    for col in meta.get("columns", []):
        if DATE_NAME_PAT.search(col.get("name","")) and col.get("type","").upper() in ("STRING","VARCHAR","CHAR"):
            date_as_string.append(f"{table_id.split('.')[-1]}.{col['name']}")

coverage_pct = (described_cols / total_cols * 100) if total_cols else 0

if coverage_pct >= 80:   a2, a2l = 3, "Good"
elif coverage_pct >= 50: a2, a2l = 2, "OK"
else:                    a2, a2l = 1, "Poor"

if a2 == 3 and (len(enum_missing_dict) > 3 or date_as_string or len(thin_comments) > 1):
    a2, a2l = 2, "OK"
if a2 == 2 and (len(enum_missing_dict) > 5 or len(date_as_string) > 2):
    a2, a2l = 1, "Poor"

a2_flags = []
if tables_no_desc:
    a2_flags.append(f"Tables with no column descriptions: {', '.join(f'`{t}`' for t in tables_no_desc)}")
if tables_no_comment:
    a2_flags.append(f"Tables with no table-level comment: {', '.join(f'`{t}`' for t in tables_no_comment)}")
if thin_comments:
    a2_flags.append(f"Tables with thin comments (<50 chars): {', '.join(f'`{t}` ({n} chars)' for t, n in thin_comments)} — describe the grain, scope, key metrics, and relationships in plain English")
if generic_flags:
    a2_flags.append(f"Generic/technical descriptions on: {', '.join(f'`{c}`' for c in generic_flags[:5])}" +
                    (f" (+{len(generic_flags)-5} more)" if len(generic_flags) > 5 else ""))

ABBREV_PAT = re.compile(
    r"(_cd|_flg|_ind|_nr|_nbr|_no|_amt|_qty|_dsc|_tp|_typ|_grp|_ctgry|_cat|_val|_pct|_fg)$",
    re.IGNORECASE
)
technical_no_desc = []
for table_id, meta in table_metadata.items():
    if "error" in meta:
        continue
    for col in meta.get("columns", []):
        if ABBREV_PAT.search(col.get("name", "")) and not col.get("comment", "").strip():
            technical_no_desc.append(f"{table_id.split('.')[-1]}.{col['name']}")
if technical_no_desc:
    a2_flags.append(
        f"Technical column names with no description ({len(technical_no_desc)}): "
        f"{', '.join(f'`{c}`' for c in technical_no_desc[:4])}"
        + (f" (+{len(technical_no_desc)-4} more)" if len(technical_no_desc) > 4 else "")
        + " — add a UC column comment, or set a display name override in Configuration > Data "
        "so business users see plain-English names instead of abbreviations"
    )
if enum_missing_dict:
    a2_flags.append(
        f"Coded columns missing description of their values ({len(enum_missing_dict)}): "
        f"{', '.join(f'`{c}`' for c in enum_missing_dict[:4])}" +
        (f" (+{len(enum_missing_dict)-4} more)" if len(enum_missing_dict) > 4 else "") +
        " — add plain-English descriptions to the UC column comment (e.g. 'A=Active, I=Inactive') "
        "and enable Entity Matching in Configuration > Data so Genie can resolve user-typed values"
    )
if date_as_string:
    a2_flags.append(
        f"Date/time columns stored as `STRING` ({len(date_as_string)}): {', '.join(f'`{c}`' for c in date_as_string[:3])}"
        " — cast to `DATE`/`TIMESTAMP` or Genie cannot do date arithmetic"
    )

tables_with_row_filter = [
    t.split(".")[-1] for t in table_identifiers
    if table_metadata.get(t, {}).get("has_row_filter")
]
tables_with_col_mask = [
    t.split(".")[-1] for t in table_identifiers
    if table_metadata.get(t, {}).get("has_col_mask")
]
restricted_tables = sorted(set(tables_with_row_filter + tables_with_col_mask))
if restricted_tables:
    a2_flags.append(
        f"Tables with row filters or column masks: {', '.join(f'`{t}`' for t in restricted_tables[:5])}"
        + (f" (+{len(restricted_tables)-5} more)" if len(restricted_tables) > 5 else "")
        + " — Entity Matching is automatically disabled on these tables; "
        "users querying filtered columns may get incomplete results without explanation"
    )

# ── Area 3: Example SQL ───────────────────────────────────────────────────────
sql_count     = len(sql_instructions)
HARDCODED_PAT = [r"= '[A-Z0-9]{4,}'", r"WHERE \w+ = '[^']{1,30}'", r"= \d{5,}"]
PARAM_PAT     = [r":\w+", r"\{\{.*?\}\}", r"getArgument\("]

JOIN_PAT    = re.compile(r"\bJOIN\b", re.IGNORECASE)
DATE_FILT_PAT = re.compile(r"\b(date|timestamp|month|year|quarter|period|week)\b", re.IGNORECASE)
AGG_PAT     = re.compile(r"\b(SUM|COUNT|AVG|MAX|MIN|GROUP BY)\b", re.IGNORECASE)
TOPN_PAT    = re.compile(r"\bORDER\s+BY\b.{1,200}\bLIMIT\s+\d+", re.IGNORECASE | re.DOTALL)
COMPARE_PAT = re.compile(
    r"\bLAG\s*\(|prior[_\s]period|year.over.year|yoy\b|previous[_\s]*(month|quarter|year)|"
    r"period.over.period|pop\b|mom\b|qoq\b", re.IGNORECASE
)

hardcoded_sqls, parameterised_sqls = [], []
has_join_example = has_date_example = has_agg_example = False
has_topn_example = has_compare_example = False

for ex in sql_instructions:
    sql   = ex.get("content", "")
    title = ex.get("title", "")[:50]
    if any(re.search(p, sql) for p in HARDCODED_PAT) and not any(re.search(p, sql) for p in PARAM_PAT):
        hardcoded_sqls.append(title)
    if any(re.search(p, sql) for p in PARAM_PAT):
        parameterised_sqls.append(title)
    if JOIN_PAT.search(sql):    has_join_example    = True
    if DATE_FILT_PAT.search(sql): has_date_example  = True
    if AGG_PAT.search(sql):     has_agg_example     = True
    if TOPN_PAT.search(sql):    has_topn_example    = True
    if COMPARE_PAT.search(sql): has_compare_example = True

# ── Table reference validity check ───────────────────────────────────────────
TABLE_FROM_PAT  = re.compile(r'\b(?:FROM|JOIN)\s+([`]?[\w.]+[`]?)', re.IGNORECASE)
CTE_PAT         = re.compile(r'\bWITH\b\s+(\w+)\s+AS\s*\(', re.IGNORECASE)
PLACEHOLDER_PAT = re.compile(r'[\[\{].*?[\]\}]')
space_table_names = set(t.split('.')[-1].lower() for t in table_identifiers)

sql_unknown_tables = []   # list of (title, unknown_ref) tuples
for ex in sql_instructions:
    sql   = ex.get("content", "")
    title = ex.get("title", "")[:40]
    if PLACEHOLDER_PAT.search(sql):
        continue                               # skip template-style examples
    cte_names = set(m.lower() for m in CTE_PAT.findall(sql))
    for ref in TABLE_FROM_PAT.findall(sql):
        ref_name = ref.strip('`"').split('.')[-1].lower()
        if ref_name and ref_name not in cte_names and ref_name not in space_table_names:
            sql_unknown_tables.append(f'"{title}" → {ref_name}')
            break                              # one flag per example is enough

if sql_count >= 10:   a3, a3l = 3, "Good"
elif sql_count >= 5:  a3, a3l = 2, "OK"
else:                 a3, a3l = 1, "Poor"

if sql_unknown_tables and a3 == 3:
    a3, a3l = 2, "OK"

missing_types = []
a3_flags = []
if sql_count < 10:
    a3_flags.append(f"Only {sql_count} SQL examples — recommend 10+ to teach query patterns")
if hardcoded_sqls:
    a3_flags.append(f"Hardcoded values in: {'; '.join(hardcoded_sqls[:3])} — use `:param_name` syntax")
if not parameterised_sqls and sql_count > 0:
    a3_flags.append("No parameterised queries — add `:param_name` or `{{param}}` examples")
if not has_join_example:    missing_types.append("multi-table `JOIN`s")
if not has_date_example:    missing_types.append("date/time filtering")
if not has_agg_example:     missing_types.append("aggregations (`SUM`/`COUNT`/`GROUP BY`)")
if not has_topn_example:    missing_types.append("top-N ranking (`ORDER BY...LIMIT`)")
if not has_compare_example: missing_types.append("period-over-period / YoY comparisons")
if missing_types and sql_count > 0:
    a3_flags.append(f"Missing query-type examples for: {', '.join(missing_types)}")
if sql_unknown_tables:
    a3_flags.append(
        f"SQL examples reference tables not in this space ({len(sql_unknown_tables)}): "
        f"{'; '.join(sql_unknown_tables[:3])}"
        + (f" (+{len(sql_unknown_tables)-3} more)" if len(sql_unknown_tables) > 3 else "")
        + " — these examples teach Genie patterns against the wrong tables; "
        "update to reference the tables actually in this space"
    )

# ── Area 4: Instructions ──────────────────────────────────────────────────────
SCHEMA_DUMP = [
    r"\bFK\b", r"foreign key", r"primary key", r"\bSCHEMA\b",
    r"field uses codes", r"This table", r"stores all", r"references the",
    r"This Genie space contains", r"The following tables",
]
BIZ_RULE = [
    r"\bshould\b", r"\balways\b", r"\bnever\b",
    r"fiscal", r"year.to.date", r"rolling \d+",
    r"default.*filter", r"business rule",
    r"case.sensitive", r"case.insensitive",
    r"\bNULL\b.*mean", r"missing.*value",
    r"wildcard", r"partial match",
    r"exclude\b", r"\binclude only\b",
    r"KPI", r"metric", r"definition",
    r"do not.*join", r"prefer.*join",
]
INLINE_SQL_PAT = re.compile(r"\bSELECT\b.{5,200}\bFROM\b", re.IGNORECASE | re.DOTALL)
EMPHATIC_PAT   = re.compile(
    r"\*\*\s*(CRITICAL|IMPORTANT|ALWAYS|NEVER|MUST|WARNING)\b.*?\*\*|"
    r"!!+\s*\w|CRITICAL\s+(JOIN|NOTE)\b|ALWAYS\s+(JOIN|USE|FILTER)\b", re.IGNORECASE
)

schema_hits, biz_hits  = 0, 0
long_blocks, inline_sql_blocks, emphatic_blocks = [], [], []
for i in text_instructions:
    c     = i.get("content", "")
    title = i.get("title", "(untitled)")[:40]
    schema_hits += sum(1 for p in SCHEMA_DUMP if re.search(p, c, re.IGNORECASE))
    biz_hits    += sum(1 for p in BIZ_RULE    if re.search(p, c, re.IGNORECASE))
    if len(c) > 300:            long_blocks.append(f'"{title}" ({len(c)} chars)')
    if INLINE_SQL_PAT.search(c): inline_sql_blocks.append(f'"{title}"')
    if EMPHATIC_PAT.search(c):  emphatic_blocks.append(f'"{title}"')

total_instr_chars = sum(len(i.get("content","")) for i in text_instructions)
total_instr_lines = sum(len(i.get("content","").splitlines()) for i in text_instructions)

if not text_instructions:          a4, a4l = 1, "Poor"
elif biz_hits > schema_hits:       a4, a4l = 3, "Good"
elif schema_hits > 3:              a4, a4l = 1, "Poor"
else:                              a4, a4l = 2, "OK"

if a4 == 3 and (len(long_blocks) > 2 or inline_sql_blocks or emphatic_blocks):
    a4, a4l = 2, "OK"
if a4 == 2 and inline_sql_blocks and schema_hits > 2:
    a4, a4l = 1, "Poor"

a4_flags = []
if not text_instructions:
    a4_flags.append("No instructions found — add business rules (fiscal year, default filters, KPI definitions)")
elif schema_hits > 3:
    a4_flags.append("Instructions look like a schema/data dictionary — Genie reads UC metadata directly; use instructions for business rules instead")
if inline_sql_blocks:
    a4_flags.append(f"Text instructions contain inline `SELECT...FROM` SQL ({len(inline_sql_blocks)} block(s)): {'; '.join(inline_sql_blocks[:3])} — move SQL examples to the SQL Queries tab")
if emphatic_blocks:
    a4_flags.append(f"Emphatic override patterns (e.g. **NEVER**, !!ALWAYS!!) found in: {'; '.join(emphatic_blocks[:3])} — these signal workarounds; fix the underlying data model or use the Joins tab instead")
if long_blocks:
    a4_flags.append(f"Long instruction blocks (>300 chars): {'; '.join(long_blocks[:3])} — split into single-topic blocks")
if total_instr_lines > 100:
    a4_flags.append(f"Instructions total {total_instr_lines} lines — best practice is ≤100 lines; trim to the most impactful rules")
elif total_instr_chars > 3000:
    a4_flags.append(f"Total instruction length is {total_instr_chars:,} chars — trim to the most impactful rules")

# ── Area 3 addendum: SQL / Sample Question alignment check ───────────────────
# (runs here so sample_questions is available; flags are appended to a3_flags)
sq_count = len(sample_questions)   # needed early for alignment check; redefined below
sq_text_blob = " ".join(
    q.get("question", q.get("question_text", q.get("content", ""))).lower()
    for q in sample_questions
)
alignment_gaps = []
if sq_count >= 3:
    SQ_TIME_PAT    = re.compile(r"\btrend|over time|monthly|quarterly|yearly|growth|change|last \d+\b", re.IGNORECASE)
    SQ_JOIN_PAT    = re.compile(r"\bby customer|across|with their|for each\b|breakdown by\b", re.IGNORECASE)
    SQ_TOPN_PAT    = re.compile(r"\btop \d+|bottom \d+|highest|lowest|best|worst|ranking\b", re.IGNORECASE)
    SQ_COMPARE_PAT = re.compile(r"\bvs\.?|versus|compare|year.over.year|yoy\b|period.over.period\b", re.IGNORECASE)

    if SQ_TIME_PAT.search(sq_text_blob) and not has_date_example:
        alignment_gaps.append("sample questions ask about time trends but no time-based SQL examples exist")
    if SQ_JOIN_PAT.search(sq_text_blob) and not has_join_example and table_count > 1:
        alignment_gaps.append("sample questions imply multi-table queries but no JOIN SQL examples exist")
    if SQ_TOPN_PAT.search(sq_text_blob) and not has_topn_example:
        alignment_gaps.append("sample questions ask for rankings/top-N but no ORDER BY...LIMIT SQL examples exist")
    if SQ_COMPARE_PAT.search(sq_text_blob) and not has_compare_example:
        alignment_gaps.append("sample questions ask for comparisons but no period-over-period SQL examples exist")

if alignment_gaps:
    a3_flags.append(
        "SQL examples are misaligned with sample questions — "
        + "; ".join(alignment_gaps)
        + ". Genie learns patterns from SQL examples; if questions and examples don't match, "
        "users will get poor results on the questions you've featured"
    )
    if a3 == 3:
        a3, a3l = 2, "OK"

# ── Area 5: Sample Questions ──────────────────────────────────────────────────
sq_count = len(sample_questions)
ta_count = len(trusted_answers) if "trusted_answers" in dir() else 0

if sq_count >= 10:   a5, a5l = 3, "Good"
elif sq_count >= 5:  a5, a5l = 2, "OK"
else:                a5, a5l = 1, "Poor"

a5_flags = []
if sq_count == 0:
    a5_flags.append("No sample questions — users will see a blank chat interface")
elif sq_count < 10:
    a5_flags.append(f"Only {sq_count} sample questions — recommend 10+ to guide business users")
if sq_count > 0:
    a5_flags.append(
        "Manually verify questions cover a range of metrics, dimensions, time periods, "
        "and user personas — 10 questions all asking about the same metric provide less value than "
        "10 questions spread across different business areas and question types"
    )

# Trusted Answers check
if ta_count == 0 and sq_count >= 5:
    a5_flags.append(
        "No Trusted Answers configured — for critical or frequently-asked questions "
        "that must always return a specific result, add Trusted Answers via "
        "Configuration > Trusted Answers; these take priority over Genie's LLM response"
    )
elif ta_count > 0:
    a5_flags.append(
        f"Trusted Answers: {ta_count} configured — good practice for pinning responses "
        "to critical business questions"
    )

# Table coverage check — flag tables not referenced by any sample question
uncovered_tables = []
if sq_count >= 3 and table_count >= 2:
    for table_id in table_identifiers:
        tname = table_id.split(".")[-1].lower()
        col_tokens = set()
        for col in table_metadata.get(table_id, {}).get("columns", [])[:15]:
            parts = col.get("name", "").lower().split("_")
            col_tokens.update(p for p in parts if len(p) > 3)
        covered = any(
            tname in q.get("question", q.get("question_text", q.get("content", ""))).lower()
            or any(
                tok in q.get("question", q.get("question_text", q.get("content", ""))).lower()
                for tok in col_tokens
            )
            for q in sample_questions
        )
        if not covered:
            uncovered_tables.append(tname)

    if uncovered_tables:
        a5_flags.append(
            f"Tables with no sample question coverage ({len(uncovered_tables)}): "
            f"{', '.join(f'`{t}`' for t in uncovered_tables)} — users will never discover what these tables can answer; "
            "add at least one sample question per table that showcases its key metrics or use case"
        )
        if len(uncovered_tables) >= max(2, table_count // 2) and a5 == 3:
            a5, a5l = 2, "OK"

# ── Area 6: Benchmarks ───────────────────────────────────────────────────────
bm_count      = len(benchmarks)
bm_missing_sql = [b for b in benchmarks if not (b.get("answer_text") or "").strip()]
BM_HARDCODED_PAT = [r"= '[A-Z0-9]{4,}'", r"WHERE \w+ = '\w+'", r"= \d{5,}"]
bm_hardcoded   = [
    b.get("question_text","")[:50] for b in benchmarks
    if any(re.search(p, b.get("answer_text","")) for p in BM_HARDCODED_PAT)
]

if bm_count >= 15:   a6, a6l = 3, "Good"
elif bm_count >= 5:  a6, a6l = 2, "OK"
else:                a6, a6l = 1, "Poor"

a6_flags = []
if bm_count == 0:
    a6_flags.append("No benchmarks — add at least 5 question/SQL pairs to validate Genie's accuracy")
elif bm_count < 5:
    a6_flags.append(f"Only {bm_count} benchmark{'s' if bm_count > 1 else ''} — add more to reach 15+ (target: 20-30 covering simple/medium/complex)")
elif bm_count < 15:
    a6_flags.append(f"Only {bm_count} benchmarks — target is 15-30; add more covering each key metric, dimension, and complexity tier")
if bm_missing_sql:
    a6_flags.append(f"{len(bm_missing_sql)} benchmark(s) have no gold standard SQL — add expected SQL for each")
if bm_hardcoded:
    a6_flags.append(f"Hardcoded values in benchmark SQL: {'; '.join(bm_hardcoded[:2])} — use representative but generalisable queries")

# ── Area 7: Semantic Layer (Metric Views / SQL Expressions) ───────────────────
expr_count   = 0
measures_ex  = []  # initialise so recommend.py can always reference these
synonyms_ex  = list(sql_expression_synonyms) if "sql_expression_synonyms" in dir() else []
filters_ex   = []
dimensions_ex = []

space_table_set         = set(t.lower() for t in table_identifiers)
metric_views_in_space   = []
metric_views_in_catalog = []

for table_id in table_identifiers:
    parts = table_id.split(".")
    if len(parts) != 3:
        continue
    catalog, schema, _ = parts
    try:
        mv_rows = spark.sql(f"""
            SELECT CONCAT('{catalog}', '.', table_schema, '.', table_name) AS full_name
            FROM `{catalog}`.information_schema.tables
            WHERE table_schema = '{schema}' AND table_type = 'METRIC_VIEW'
        """).collect()
        for row in mv_rows:
            fn = row["full_name"].lower()
            if fn in space_table_set:
                if fn not in metric_views_in_space:   metric_views_in_space.append(fn)
            else:
                if fn not in metric_views_in_catalog: metric_views_in_catalog.append(fn)
    except Exception:
        pass

if metric_views_in_space:
    a7, a7l = 3, "Good"
    a7_flags = [
        f"Metric View(s) in space: {', '.join(t.split('.')[-1] for t in metric_views_in_space)}"
        " — semantic layer (measures, dimensions, filters) is defined at the UC layer ✓"
    ]
    if metric_views_in_catalog:
        a7_flags.append(
            f"Additional Metric Views found in catalog but not added to space: "
            f"{', '.join(t.split('.')[-1] for t in metric_views_in_catalog[:3])}"
            " — add them if they cover relevant metrics"
        )

elif metric_views_in_catalog:
    a7, a7l = 2, "OK"
    a7_flags = [
        f"Metric Views detected in catalog but not added to this space: "
        f"{', '.join(t.split('.')[-1] for t in metric_views_in_catalog[:5])}"
        " — add them to the space to use the UC semantic layer instead of manual SQL Expressions"
    ]

else:
    # sql_expressions is pre-parsed from SQL_SNIPPET items in fetch.py
    expressions = sql_expressions if "sql_expressions" in dir() else []
    AGG_CONTENT_PAT  = re.compile(r"\b(SUM|COUNT|AVG|MAX|MIN|try_divide)\s*\(", re.IGNORECASE)
    COND_CONTENT_PAT = re.compile(r"\bWHERE\b|\bAND\b|\bOR\b|=\s*'", re.IGNORECASE)

    for e in expressions:
        etype   = (e.get("expression_type") or e.get("type") or "").upper()
        content = e.get("expression") or e.get("sql") or e.get("content") or ""
        name    = e.get("name") or e.get("title") or ""
        if etype in ("MEASURE","AGGREGATION") or (not etype and AGG_CONTENT_PAT.search(content)):
            measures_ex.append(name)
        elif etype in ("FILTER","CONDITIONAL") or (not etype and COND_CONTENT_PAT.search(content) and not AGG_CONTENT_PAT.search(content)):
            filters_ex.append(name)
        else:
            dimensions_ex.append(name)

    expr_count = len(expressions)
    if expr_count >= 3 and measures_ex: a7, a7l = 3, "Good"
    elif expr_count >= 1:               a7, a7l = 2, "OK"
    else:                               a7, a7l = 1, "Poor"

    a7_flags = []
    if expr_count == 0:
        a7_flags.append("No Metric Views in space and no SQL Expressions defined — add Measures, Dimensions, Filters, and Synonyms")
    else:
        if not measures_ex:
            a7_flags.append("No Measure expressions — define SUM/ratio formulas for key KPIs")
        if not synonyms_ex:
            a7_flags.append("No Synonyms defined — add abbreviations and alternate names to reduce clarification prompts")
        if expr_count < 3:
            a7_flags.append(f"Only {expr_count} SQL Expression(s) — add more Measures, Dimensions, Filters, and Synonyms")

# ── Business abbreviation audit (Area 7 addendum) ────────────────────────────
# Checks whether business abbreviations found in column names have synonym/instruction mappings
KNOWN_BUSINESS_ABBREVS = {
    "rm": "Relationship Manager", "kpi": "Key Performance Indicator",
    "aum": "Assets Under Management", "nps": "Net Promoter Score",
    "arr": "Annual Recurring Revenue", "mrr": "Monthly Recurring Revenue",
    "ltv": "Lifetime Value", "cac": "Customer Acquisition Cost",
    "ytd": "Year-to-Date", "mtd": "Month-to-Date", "qtd": "Quarter-to-Date",
    "yoy": "Year-over-Year", "mom": "Month-over-Month", "qoq": "Quarter-over-Quarter",
    "ebitda": "EBITDA", "roe": "Return on Equity", "roa": "Return on Assets",
    "nim": "Net Interest Margin", "fum": "Funds Under Management",
    "nav": "Net Asset Value", "glp": "Gross Loan Portfolio", "npl": "Non-Performing Loan",
    "nrr": "Net Revenue Retention", "arr": "Annual Recurring Revenue",
}

ABBREV_TOKEN_PAT = re.compile(r'\b([a-z]{2,6})\b')
found_abbrevs = set()
for table_id, meta in table_metadata.items():
    if "error" in meta:
        continue
    for col in meta.get("columns", []):
        col_lower = col.get("name", "").lower().replace("_", " ")
        for token in ABBREV_TOKEN_PAT.findall(col_lower):
            if token in KNOWN_BUSINESS_ABBREVS:
                found_abbrevs.add(token)

instr_text_blob = " ".join(i.get("content", "") for i in text_instructions).lower()
synonym_blob    = " ".join(synonyms_ex).lower()

unmapped_abbrevs = []
for abbrev in sorted(found_abbrevs):
    full_term_lower = KNOWN_BUSINESS_ABBREVS[abbrev].lower().split("/")[0].strip()
    in_synonyms = abbrev in synonym_blob or full_term_lower in synonym_blob
    in_instrs   = abbrev in instr_text_blob or full_term_lower in instr_text_blob
    if not in_synonyms and not in_instrs:
        unmapped_abbrevs.append(abbrev.upper())

if unmapped_abbrevs:
    a7_flags.append(
        f"Business abbreviations in column names with no synonym or instruction mapping "
        f"({len(unmapped_abbrevs)}): {', '.join(unmapped_abbrevs)} — "
        "add a SQL Expression Synonym for each (Configuration > SQL Expressions > Synonym) "
        "so Genie resolves queries using full terms (e.g. 'Relationship Manager' → rm_id), "
        "or define them in Instructions > Business Terms"
    )
    if a7 == 3 and len(unmapped_abbrevs) >= 2:
        a7, a7l = 2, "OK"

# ── Verdict ───────────────────────────────────────────────────────────────────
scores  = [a0, a1, a2, a3, a4, a5, a6, a7]
labels  = [a0l, a1l, a2l, a3l, a4l, a5l, a6l, a7l]
areas   = ["0. Data & UC Readiness", "1. Table & Space Curation", "2. Metadata Quality",
           "3. Example SQL", "4. Instructions", "5. Sample Questions",
           "6. Benchmarks", "7. Semantic Layer"]
flags   = [a0_flags, a1_flags, a2_flags, a3_flags, a4_flags, a5_flags, a6_flags, a7_flags]

scored_total = sum(s for s, l in zip(scores, labels) if l != "N/A")
max_total    = 3 * sum(1 for l in labels if l != "N/A")
total        = scored_total

if max_total == 24:
    if total >= 21:   verdict, verdict_note = "PRODUCTION READY",   "Minor tuning recommended before onboarding business users"
    elif total >= 15: verdict, verdict_note = "NEEDS IMPROVEMENT",  "Address flagged areas before onboarding business users"
    else:             verdict, verdict_note = "RECOMMEND REBUILD",  "Good foundation — work through the flagged areas below to get this space production-ready"
elif max_total == 21:
    if total >= 18:   verdict, verdict_note = "PRODUCTION READY",   "Minor tuning recommended before onboarding business users"
    elif total >= 13: verdict, verdict_note = "NEEDS IMPROVEMENT",  "Address flagged areas before onboarding business users"
    else:             verdict, verdict_note = "RECOMMEND REBUILD",  "Good foundation — work through the flagged areas below to get this space production-ready"
else:
    pct = total / max_total if max_total else 0
    if pct >= 0.83:   verdict, verdict_note = "PRODUCTION READY",   "Minor tuning recommended before onboarding business users"
    elif pct >= 0.61: verdict, verdict_note = "NEEDS IMPROVEMENT",  "Address flagged areas before onboarding business users"
    else:             verdict, verdict_note = "RECOMMEND REBUILD",  "Good foundation — work through the flagged areas below to get this space production-ready"

# ── Per-area minimums — cap verdict if any critical area is Poor ───────────────
BLOCKER_AREAS = {"0. Data & UC Readiness", "2. Metadata Quality", "6. Benchmarks"}
blocker_fails = [
    area.split(". ", 1)[1]
    for area, score, label in zip(areas, scores, labels)
    if score == 1 and label != "N/A" and area in BLOCKER_AREAS
]
if blocker_fails and verdict == "PRODUCTION READY":
    verdict      = "NEEDS IMPROVEMENT"
    verdict_note = (
        f"Verdict capped — critical area(s) scored Poor: {', '.join(blocker_fails)}. "
        "Resolve these before onboarding business users"
    )

total_str = f"{total}/{max_total}" + (" (Area 7 N/A)" if max_total < 24 and max_total >= 21 else "" if max_total == 24 else " (some areas N/A)")

# ── Print scorecard ───────────────────────────────────────────────────────────
BAR = {3: "███ Good", 2: "██░ OK  ", 1: "█░░ Poor"}

print("=" * 58)
print(f"  GENIE ASSESSMENT: {space.get('display_name', space.get('title',''))[:30]}")
print(f"  {datetime.utcnow().strftime('%Y-%m-%d')}")
print("=" * 58)
for area, score, label, area_flags in zip(areas, scores, labels, flags):
    bar_str   = "░░░ N/A " if label == "N/A" else BAR[score]
    score_str = "N/A"      if label == "N/A" else f"{score}/3"
    print(f"  {area:<26} {bar_str}  ({score_str})")
    for f in area_flags:
        print(f"    ⚠ {f}")
print("-" * 58)
print(f"  TOTAL                          {total_str}")
print(f"  VERDICT: {verdict}")
print(f"           {verdict_note}")
print("=" * 58)
