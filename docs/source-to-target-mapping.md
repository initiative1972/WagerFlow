# Source-to-Target Mapping (STTM)

Legacy SQL Server → S3 medallion → Redshift/Athena/Power BI. Synthetic/illustrative;
column names mirror `src/data_generator.py`.

## 1. Customer dimension (`DimCustomer`, SCD2)

| # | Source (legacy) | Target (Silver) | Type | Rule | CDE? |
|---|---|---|---|---|---|
| 1 | `dbo.Account.AcctNo` | `account_id` | STRING | Trim; business key | Key |
| 2 | `dbo.Account.StatusCd` | `account_status` | STRING | Map code→label | **Yes** |
| 3 | `dbo.Account.RiskTier` | `risk_tier` | STRING | Standardise upper | **Yes** |
| 4 | `dbo.Limits.DailyDep` | `daily_deposit_limit` | DECIMAL | Latest effective | **Yes** |
| 5 | `dbo.Account.Juris` | `jurisdiction` | STRING | Validate against ref | **Yes** |
| 6 | `dbo.Account.UpdatedAt` | `updated_at` | TIMESTAMP | SCD2 effective time | — |
| 7 | *(derived)* | `row_hash` | STRING | `sha2(concat_ws('||', CDEs),256)` | — |
| 8 | *(derived)* | `start_ts`/`end_ts`/`is_current` | TS/TS/BOOL | SCD2 control (ADR‑0002) | — |

PII (`email`, `phone`, `card_number`) tokenised at Bronze→Silver (`utils/masking.py`).

## 2. Wager fact (`FactWagerSettled`)

| # | Source | Target | Type | Rule | CDE? |
|---|---|---|---|---|---|
| 1 | `dbo.Bet.BetId` | `bet_id` | STRING | Business key | Key |
| 2 | `dbo.Bet.AcctNo` | `account_id` | STRING | FK → DimCustomer (current) | — |
| 3 | `dbo.Bet.Stake` | `stake_amount` | DECIMAL(14,2) | Replaces cursor sum | **Yes** |
| 4 | `dbo.Settlement.Payout` | `payout_amount` | DECIMAL(14,2) | Join on BetId; replaces temp table | **Yes** |
| 5 | `dbo.Bet.EventDt` | `event_date` | DATE | Cast; partition key | — |

## 3. Gold daily mart (`FactBettingDaily`) → Redshift / Athena / Power BI

| Field | Rule |
|---|---|
| `turnover` | `SUM(stake_amount)` by `event_date` |
| `payout` | `SUM(payout_amount)` by `event_date` |
| `ggr` | `turnover − payout` (single definition, reused by DAX) |
| `hold_pct` | `ggr / turnover` (safe divide) |

## Governance
- CDEs drive both the SCD2 change hash and the reconciliation hash — defined once
  in `config/cde_catalog.json`.
- Every target column traces to a legacy column or is marked derived/pipeline.
- Code→label maps live in reference data, lifted out of the legacy procedure.
- Access via **Lake Formation** tag‑based policies; PII tokenised before Silver.
