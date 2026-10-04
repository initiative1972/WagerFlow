# Cutover Runbook — Legacy Settlement → AWS Lakehouse

Operational playbook for the dual‑run and production cutover of the wagering
settlement platform. Written as a lead artefact: gates, decisions, and
communication under pressure.

## 1. Phases
1. **Shadow / dual‑run** — legacy and AWS run in parallel on the same inputs;
   AWS output not consumed. Reconciliation runs each cycle.
2. **Validation (Dev → SIT → UAT)** — mock cutovers; defects logged and resolved;
   finance/stewards review parity reports.
3. **Cutover** — reporting + downstream repoint to Gold/Redshift; legacy frozen.
4. **Decommission** — after an agreed clean dual‑run window.

## 2. Go / No-Go gate (per cycle)
| Signal | Go | No‑Go |
|---|---|---|
| Micro row‑hash parity (CDEs) | 100% match | any mismatch |
| Row count (legacy vs modern) | equal | not equal |
| Turnover / payout / GGR drift | ≤ 0.001% | > 0.001% |
| Late/out‑of‑sequence backlog | drained within T+1 | still draining |

The reconciliation **circuit breaker** (`reconciliation.enforce`) automates the
first three rows and fails the pipeline before Gold/Redshift/Power BI refresh.

## 3. The 8:00 AM scenario (worked example)
*Breaker trips at 08:00; trading desk dashboard expected 08:30.*
1. **Contain** — breaker halts promotion; Redshift marts and Power BI do not refresh.
2. **Assess** — read the drift report: mismatch count, affected CDE, settlement
   window, link to the exception dashboard.
3. **Decide & fall back** — if isolated and understood, repoint the semantic layer
   to the last validated Gold snapshot so 08:30 shows **verified prior‑cycle** data,
   clearly labelled.
4. **Communicate** — lead the message to the trading desk / risk: what they see,
   what's trusted, ETA. No raw "job failed" alerts to the business.
5. **Post‑incident** — root‑cause (often a late settlement feed); add/adjust a test
   or watermark; update an ADR if the design changes.

### Stakeholder message template
> *Subject: Wagering dashboard — showing verified data as of [cycle], refresh delayed*
> The automated parity gate detected a settlement discrepancy this morning and
> paused the refresh to prevent incorrect GGR/turnover figures. The dashboard shows
> the **last fully reconciled** cycle ([time]). Engineering is resolving it; expected
> refresh by [time]. Accuracy over speed — the numbers shown are reliable.

## 4. Rollback
- **Serving:** repoint Redshift/Power BI to the prior validated Gold partition —
  minutes, no data movement.
- **Pipeline:** legacy remains system of record until decommission sign‑off, so
  rollback is "freeze modern, keep legacy."

## 5. Late / out-of-sequence settlement
- Watermark both sides on the DMS commit timestamp / shared batch id.
- Reconcile the **settled T+1 partition**, not a live moving target.
- Exclude audit/metadata columns from the hash (handled by `cde_hash`).

## 6. Roles
| Role | Responsibility at cutover |
|---|---|
| Lead Data Engineer | Go/No‑Go call, stakeholder comms, sign‑off |
| Data Engineers | Pipeline run, defect fixes, fallback activation |
| Finance / Stewards | Parity review, GGR/turnover validation |
| Platform / DevOps | Terraform promotion, monitoring (SNS alerts), rollback |
