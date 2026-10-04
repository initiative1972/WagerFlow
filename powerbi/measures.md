# Power BI DAX Measures

Annotated companion to `measures.dax` (the paste‑ready source for Tabular Editor /
the Power BI model). Star schema over the Gold wagering marts; see
`semantic-model.md`. Synthetic/illustrative.

## 1. Core wagering metrics
- `Total Turnover`, `Total Payout`.
- **`Gross Gaming Revenue (GGR)` = Turnover − Payout** — the *same* definition as
  `gold_wagering_marts.py`, so the dashboard can't disagree with the pipeline.
- `NGR` = GGR − Bonus Cost − Gaming Tax (components kept explicit for audit).
- `Hold %` = GGR / Turnover (safe divide).
- `Active Bettors` counts the **current** SCD2 slice only.

## 2. Time intelligence
`GGR Prior Year`, `GGR YoY %`, `Turnover MTD`, `Turnover YTD` — all via the marked
`DimDate`.

## 3. Reconciliation & trust signals
From the disconnected `DimReconciliationLog` (written by the reconciliation gate):
- `Reconciliation Drift %` (latest run), `Reconciliation Parity Status`
  (PARITY PASS / AUDIT BREACH vs tolerance), `Reconciliation Status Colour`,
  `Data Freshness`.
- These are the DAX counterpart to the pipeline circuit breaker — the equivalence
  story made visible to trading/risk on the report cover.

## Measure index
| Measure | Group |
| --- | --- |
| Total Turnover, Total Payout, GGR, NGR, Hold %, Active Bettors, Avg Bet Slip Value | Core |
| GGR Prior Year, GGR YoY %, Turnover MTD, Turnover YTD | Time |
| Reconciliation Drift %, Parity Status, Status Colour, Data Freshness | Trust |

## Status
Designed, **not deployed** — needs a Power BI workspace to build the `.bim`/`.pbix`.
