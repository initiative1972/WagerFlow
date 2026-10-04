# ADR 0003 — Dual-run reconciliation as a circuit breaker

- **Status:** Accepted
- **Date:** 2026-10-03
- **Deciders:** Lead Data Engineer (portfolio)
- **Related:** `src/transforms/reconciliation.py`, `tests/test_reconciliation.py`

## Context
In a wagering platform, a cent of settlement drift across millions of bet slips
is a regulatory and financial problem. Cutover cannot rely on spot checks; we need
automated, evidenced parity between the legacy procedure output and the modern AWS
mart during the dual‑run phase.

## Decision
Implement a two‑level reconciliation that both **computes** and can **enforce**:
- **Macro:** counts, turnover, payout, GGR.
- **Micro:** SHA‑256 row hash over Critical Data Elements, compared by full‑outer
  join (catches changed values, missing rows, extra rows).
- **Gate order:** row‑count mismatch → fail; any micro mismatch → fail; else
  turnover drift vs. tolerance.
`reconcile_settlement` returns a result (never raises); `enforce` is the circuit
breaker that fails the pipeline, blocking promotion to Gold/Redshift/Power BI.

## Rationale
- Separating compute from enforce lets the reconciliation **log** capture a failing
  run's detail before the breaker trips.
- Hashing **CDEs only** (shared with SCD2) means audit/metadata columns and
  processing latency cannot create false breaches.
- Drift is a **true percentage** so the number and the tolerance units agree.

## Consequences
**Positive:** evidenced equivalence a steward can sign; corrupted data cannot reach
consumers. **Negative:** micro hashing is a full‑outer join — on billions of rows,
run it on the **settled T+1 partition** (see cutover runbook), not the live set;
hashing is collision‑safe at SHA‑256 but column order/null handling must be
governed (handled by `cde_hash`).

## Alternatives considered
- **Macro‑only reconciliation:** cheap but misses offsetting row‑level errors that
  net to the same totals; rejected as insufficient for financial parity.
