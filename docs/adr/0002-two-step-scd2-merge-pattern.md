# ADR 0002 — SCD2 pattern: partition current into expired vs. kept

- **Status:** Accepted
- **Date:** 2026-10-03
- **Deciders:** Lead Data Engineer (portfolio)
- **Related:** `src/transforms/silver_scd2.py`, `tests/test_scd2.py`

## Context
The customer dimension needs SCD Type 2 history (risk tier, deposit limit,
status, jurisdiction). A naive implementation found "unchanged" rows with a
`left_anti` join **against the incoming feed**, which keeps only current rows that
are *absent* from the feed. Current rows that are **present and unchanged** then
fall through every branch and are **dropped** — the dimension silently shrinks
each run.

## Decision
Compute the set of **changed keys** (present in current AND CDE hash differs),
then:
- **expired** = current rows whose key is in changed_keys (close `end_ts`,
  `is_current = false`);
- **kept** = current rows whose key is NOT in changed_keys (retains
  present‑unchanged *and* not‑in‑feed rows);
- **new_versions** = incoming rows that are new keys OR changed keys;
- union with untouched inactive history.

Change detection uses the shared `cde_hash` (so it matches reconciliation).

## Consequences
**Positive:** no row loss, no duplicate current versions, exactly one current row
per key (asserted by tests). Pure Spark — runs on Databricks/Glue/EMR unchanged.
**Negative:** deletes/tombstones are out of scope (absent keys stay current); a
later ADR will choose soft‑delete vs. full‑snapshot diff. Single business key for
readability; composite keys need the join generalised.

## Alternatives considered
- **Delta `MERGE` with null‑merge‑key:** elegant on Databricks, but ties the
  logic to Delta and complicates local CI; chosen the pure‑Spark union form so it
  tests without JARs and runs on Glue/EMR too.
- **Truncate‑reload:** destroys history; rejected.
