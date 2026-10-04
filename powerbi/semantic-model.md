# Power BI Semantic Model

Star schema over the Gold wagering marts. Illustrative/synthetic; evidences
semantic modelling, DAX, performance, and governance for the role.

## 1. Star schema

```
            ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
            │  DimCustomer  │     │   DimMarket   │     │   DimEvent    │
            │ (SCD2→current)│     │               │     │               │
            └───────┬───────┘     └───────┬───────┘     └───────┬───────┘
                    │1                    │1                    │1
                    ▼*                    ▼*                    ▼*
            ┌───────────────────────────────────────────────────────────┐
 ┌────────┐ │                   FactWagerSettled                        │
 │DimDate │*│   grain: one row per settled bet slip                     │
 │(marked)│─│   measures: stake_amount, payout_amount, bonus_cost, tax  │
 └────────┘ └───────────────────────────────────────────────────────────┘

 DimReconciliationLog  — DISCONNECTED (DQ/trust KPIs, not sliced by the fact)
```

| Table | Role | Grain | Key |
|---|---|---|---|
| `FactWagerSettled` | Fact | settled bet slip | `bet_id` |
| `DimCustomer` | Dimension (SCD2, current) | one per account | `account_id` |
| `DimMarket` | Dimension | one per market | `market_id` |
| `DimEvent` | Dimension | one per event | `event_id` |
| `DimDate` | Date (marked) | one per day | `date` |
| `DimReconciliationLog` | DQ metrics (disconnected) | one per run | `run_ts` |

## 2. Relationships
- Fact → each dimension: many‑to‑one, single direction (no bidirectional).
- `FactWagerSettled[event_date]` → `DimDate[date]`; **mark DimDate** for time
  intelligence.
- `DimReconciliationLog` is intentionally **disconnected**.

## 3. Performance
- **Dual storage:** aggregates in **Import** for sub‑second cards; bet‑level
  drill‑through via **DirectQuery** on Redshift.
- **Incremental refresh** on `FactWagerSettled` partitioned by `event_date`.
- Star schema, integer surrogate keys, no snowflaking; pre‑aggregate in Gold.

## 4. Governance
- **RLS** by jurisdiction (VIC/NSW/QLD/WA) on `DimCustomer`; test with "View as role".
- **Certified dataset**; sensitivity label; access via app, not file sharing.
- PII tokenised upstream (`utils/masking.py`) + **Lake Formation** tag policies.
- `Data Freshness` + `Reconciliation Parity Status` on the cover so trading/risk
  see the data passed the gate and when it loaded.

## 5. Status
Specified and designed, **not deployed** — needs a Power BI workspace. The Gold
tables it sits on are produced by the (AWS/Databricks) Gold job.
