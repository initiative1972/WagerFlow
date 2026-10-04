# WagerFlow
An AWS + Databricks lakehouse migration accelerator: reverse‑engineering legacy SQL settlement procedures into a governed medallion on S3, with batch *and* streaming ingestion, automated dual‑run financial reconciliation, and a Power BI semantic layer.
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?logo=githubactions)](../../actions)
[![AWS](https://img.shields.io/badge/Cloud-AWS-232F3E?logo=amazonaws)](https://aws.amazon.com/)
[![Databricks](https://img.shields.io/badge/Compute-Databricks-FF3621?logo=databricks)](https://databricks.com/)
[![Spark](https://img.shields.io/badge/Engine-PySpark-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## What this repository is — and is not

A **portfolio reference implementation** for an enterprise AWS data‑migration role (ETL + Power BI, Databricks + AWS services across batch and streaming). It is deliberately honest about what executes and what is design evidence — because on a migration, judgement about *"how do we know it's correct?"* matters more than any one tool.

| Layer / component | Status | How to verify |
| --- | --- | --- |
| PySpark transforms — Silver SCD2, Gold wagering marts | **Runs & tested in CI** | `pytest tests/test_scd2.py tests/test_settlement_rules.py` |
| Reconciliation engine — macro aggregates **+ micro row hash** | **Runs & tested in CI** | `pytest tests/test_reconciliation.py` |
| Streaming transform — odds/bet micro‑batch logic | **Runs & tested in CI** (static micro‑batch) | `pytest tests/test_streaming.py` |
| Data‑quality assertions (pytest; Great Expectations planned) | **Runs in CI** | `pytest tests/test_settlement_rules.py` |
| Power BI semantic model + DAX | **Design artefact** | inspect `powerbi/` |
| AWS infra — Glue, EMR, Redshift, Athena, Kinesis, MSK, Lake Formation; Terraform | **Design artefact** (needs an AWS account) | `terraform validate` (not run in CI) |
| Cutover runbook, STTM, ADRs | **Design artefact** | inspect `docs/` |

All data is **synthetic**. No proprietary or customer data is used. In‑repo figures are illustrative; measured numbers come from `make demo` and regenerate on each run.

> **Scenario:** a fictional wagering operator ("WagerFlow", invented) migrating an 800‑line SQL Server settlement procedure and a live odds feed to an AWS lakehouse ahead of a Redshift/Power BI serving refresh. The scenario is illustrative; the engineering patterns are real and transferable to any enterprise AWS migration.

---

## 1. The problem

A monolithic SQL Server stored procedure settles wagers nightly — cursors, temp tables, bonus/tax logic, and reporting all tangled together. The business wants it on AWS (Databricks + Glue/EMR compute, S3 storage, Redshift/Athena serving) with **batch** history and **streaming** odds, but cannot cut over until the new platform is **provably equivalent on the money** — turnover, payout, and Gross Gaming Revenue — within a tiny tolerance.

**Objectives**
1. **Reverse‑engineer** procedural SQL into set‑based, idempotent PySpark across Bronze/Silver/Gold.
2. **Ingest both ways:** batch/CDC (AWS DMS → S3) and streaming (Kinesis/MSK → Structured Streaming).
3. **Model** SCD2 customer dimensions + a dimensional wagering mart.
4. **Prove equivalence** with a dual‑run reconciliation gate (macro aggregates **and** micro row hashing) wired as a circuit breaker.
5. **Serve** Redshift/Athena + a governed Power BI model, with reconciliation trust signals on the report.
6. **Automate** with CI (genuinely runnable), Terraform IaC, and Lake Formation governance.

---

## 2. Architecture

```
 BATCH  Legacy SQL Server ──(AWS DMS CDC)──┐
                                           ▼
 STREAM Live odds/bets ──(Kinesis / MSK)──▶  S3 BRONZE (raw Delta/Parquet)
                                           │
                        (Databricks on AWS / AWS Glue / EMR — PySpark)
                                           ▼
                               S3 SILVER (cleansed, SCD2 dims, fact)
                                           │
                        ┌──────────────────▼───────────────────┐
                        │  RECONCILIATION GATE (circuit breaker)│
                        │  macro aggregates + SHA‑256 row hash   │
                        └──────────────────┬───────────────────┘
                                           ▼
                 S3 GOLD + Amazon Redshift marts ── Amazon Athena (ad‑hoc SQL)
                                           │
                                           ▼
                     Power BI semantic model (RLS + wagering DAX + trust KPIs)

 Governance: AWS Lake Formation (tag‑based access) · PII tokenisation · jurisdictional filtering
 DataOps: GitHub Actions CI (local Spark) · Terraform IaC · Dev/SIT/UAT/Prod
```

**AWS service map (brief → where it appears)**
| Brief service | Role here |
| --- | --- |
| **Glue** | Serverless PySpark jobs + Glue Data Catalog over S3 |
| **EMR** | Alternative managed Spark for heavy batch backfills |
| **Redshift** | Low‑latency dimensional serving marts |
| **Athena** | Ad‑hoc SQL over S3 Gold (Catalog‑registered) |
| **Kinesis / MSK** | Streaming ingestion of odds/bet events |
| **Lake Formation** | Tag‑based access control, row/column governance |
| **DMS** | CDC capture from the legacy transactional DB |

---

## 3. Repository structure

```
wagerflow-modernize/
├── .github/workflows/
│   ├── ci.yml                     # flake8 + local-Spark pytest (RUNS)
│   └── cd.yml                     # Terraform plan/apply + Databricks/Glue deploy (design)
├── config/
│   ├── pipeline_config.json       # metadata-driven source→target configs
│   ├── reconciliation_rules.json  # tolerances for Turnover/Payout/GGR
│   └── cde_catalog.json           # Critical Data Element definitions + ownership
├── data/
│   ├── legacy_samples/legacy_settlement_sp.sql   # trimmed synthetic T-SQL procedure
│   └── synthetic/                 # generated bets, customers, odds, expected outputs
├── docs/
│   ├── adr/
│   │   ├── 0001-cdc-dms-vs-batch-extract.md
│   │   ├── 0002-two-step-scd2-merge-pattern.md
│   │   └── 0003-dual-run-reconciliation-circuit-breaker.md
│   ├── source-to-target-mapping.md
│   └── cutover-runbook.md
├── infra/terraform/               # S3, Glue, Kinesis, Redshift, Lake Formation (design)
├── powerbi/
│   ├── semantic-model.md
│   ├── measures.dax
│   └── measures.md
├── src/
│   ├── transforms/                # PURE functions (no cloud deps) — tested in CI
│   │   ├── silver_scd2.py
│   │   ├── gold_wagering_marts.py
│   │   ├── streaming_odds.py      # micro-batch transform (CI) + readStream wrapper (design)
│   │   └── reconciliation.py
│   └── utils/
│       ├── hashing.py             # deterministic SHA-256 row hashing
│       └── masking.py             # PII tokenisation (email, phone, card)
├── tests/
│   ├── test_scd2.py
│   ├── test_settlement_rules.py
│   ├── test_streaming.py
│   └── test_reconciliation.py
├── Makefile
├── requirements.txt
└── README.md
```

**Design principle that makes CI real:** business logic lives in `src/transforms/` as **pure functions (DataFrame → DataFrame)** with no cloud dependency. Glue/EMR/Databricks jobs and the streaming driver are thin wrappers that call these functions. So the logic is unit‑tested on a local Spark session in GitHub Actions, and lifts onto Glue, EMR, or Databricks unchanged.

---

## 4. Core implementation

### 4.1 Silver — SCD Type 2 (correct, no row loss)

> **Fixes a real bug.** A naive version keeps "unchanged" rows via `left_anti` on the incoming feed, which drops current rows that are **present and unchanged** — the dimension silently loses rows every run. The pattern below partitions current rows into *expired* vs *kept*, and never loses an unchanged row.

```python
from pyspark.sql import DataFrame, functions as F

CDE_COLS = ["account_status", "risk_tier", "daily_deposit_limit", "jurisdiction"]
_END = "9999-12-31 23:59:59"


def _with_hash(df: DataFrame, cde_cols: list[str]) -> DataFrame:
    h = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("\u2205"))
                                   for c in cde_cols]), 256)
    return df.withColumn("row_hash", h)


def process_customer_scd2(incoming_df: DataFrame, current_dim_df: DataFrame | None,
                          key: str = "account_id", cde_cols: list[str] = CDE_COLS) -> DataFrame:
    incoming = _with_hash(incoming_df, cde_cols)

    def as_current(df):
        return (df.withColumn("start_ts", F.col("updated_at"))
                  .withColumn("end_ts", F.to_timestamp(F.lit(_END)))
                  .withColumn("is_current", F.lit(True)))

    if current_dim_df is None or len(current_dim_df.take(1)) == 0:
        return as_current(incoming)                                   # initial load

    active = current_dim_df.filter("is_current")
    inactive = current_dim_df.filter("is_current = false")           # untouched history

    changed_keys = (incoming.alias("i")
        .join(active.select(key, F.col("row_hash").alias("_h")).alias("c"), key, "inner")
        .where(F.col("i.row_hash") != F.col("_h")).select(key))

    # Current rows that must be expired (key present in feed AND changed)
    expired = (active.join(changed_keys, key, "left_semi")
        .withColumn("end_ts", F.lit(None).cast("timestamp"))         # closed by the new version's start
        .withColumn("is_current", F.lit(False)))

    # Current rows that are retained unchanged (NOT in the changed set) — the bug fix
    kept = active.join(changed_keys, key, "left_anti")

    # New current versions: brand-new keys OR changed keys
    new_versions = as_current(
        incoming.join(active.select(key, F.col("row_hash").alias("_h")), key, "left")
                .where(F.col("_h").isNull() | (F.col("row_hash") != F.col("_h")))
                .drop("_h"))

    return inactive.unionByName(expired).unionByName(kept).unionByName(new_versions)
```

### 4.2 Reconciliation — macro gate **and** micro hash (as advertised)

```python
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, functions as F


@dataclass
class ReconciliationResult:
    passed: bool
    drift_pct: float          # a true percentage (0.5 == 0.5%)
    mismatches: int
    metrics: dict = field(default_factory=dict)
    breach_reason: str = ""


def _row_hash(df, keys, cde_cols):
    h = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("\u2205"))
                                   for c in cde_cols]), 256)
    return df.select(*keys, h.alias("row_hash"))


def reconcile_settlement(legacy: DataFrame, modern: DataFrame, keys: list[str],
                         cde_cols: list[str], tolerance_pct: float = 0.001) -> ReconciliationResult:
    # Macro: counts + financial sums → GGR
    def agg(df):
        r = df.select(F.count("*").alias("rows"),
                      F.coalesce(F.sum("stake_amount"), F.lit(0.0)).alias("turnover"),
                      F.coalesce(F.sum("payout_amount"), F.lit(0.0)).alias("payout")).first().asDict()
        r["ggr"] = float(r["turnover"]) - float(r["payout"])
        return r
    leg, mod = agg(legacy), agg(modern)

    # Micro: SHA-256 row hash parity across CDEs
    cmp = _row_hash(legacy, keys, cde_cols).alias("l").join(
        _row_hash(modern, keys, cde_cols).alias("m"), on=keys, how="full_outer")
    mismatches = cmp.where(F.col("l.row_hash").isNull() | F.col("m.row_hash").isNull()
                           | (F.col("l.row_hash") != F.col("m.row_hash"))).count()

    turnover_drift = (abs(leg["turnover"] - mod["turnover"]) / leg["turnover"] * 100) \
        if leg["turnover"] else 0.0
    metrics = {"legacy": leg, "modern": mod, "turnover_drift_pct": round(turnover_drift, 6)}

    if leg["rows"] != mod["rows"]:
        return ReconciliationResult(False, 100.0, mismatches, metrics,
                                    f"Row count mismatch: {leg['rows']} vs {mod['rows']}")
    if mismatches > 0:
        return ReconciliationResult(False, turnover_drift, mismatches, metrics,
                                    f"{mismatches} row-hash mismatches across CDEs")
    if turnover_drift > tolerance_pct:
        return ReconciliationResult(False, turnover_drift, mismatches, metrics,
                                    f"Turnover drift {turnover_drift:.6f}% > {tolerance_pct}%")
    return ReconciliationResult(True, turnover_drift, 0, metrics, "Parity within tolerance")
```

### 4.3 Streaming — batch *and* streaming from one tested transform

```python
from pyspark.sql import DataFrame, functions as F


def transform_odds_events(df: DataFrame) -> DataFrame:
    """Pure micro-batch transform: parse, dedupe latest price per market, flag staleness.
    Unit-tested on a static DataFrame; reused verbatim by batch and streaming."""
    w = F.window  # noqa: F841 (illustrative)
    deduped = (df.withColumn("event_ts", F.col("event_ts").cast("timestamp"))
                 .dropDuplicates(["market_id", "event_ts"]))
    return deduped.withColumn("is_stale",
                              F.col("event_ts") < F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"))


def start_odds_stream(spark, kinesis_opts: dict, sink_path: str):
    """Design artefact (needs Kinesis/MSK). Calls the SAME transform as batch."""
    stream = spark.readStream.format("kinesis").options(**kinesis_opts).load()
    out = transform_odds_events(stream)
    return (out.writeStream.format("delta")
               .option("checkpointLocation", f"{sink_path}/_chk")
               .outputMode("append").start(sink_path))
```

The pure `transform_odds_events` is tested with a static DataFrame in CI (`test_streaming.py`); `start_odds_stream` is the design wrapper that needs Kinesis/MSK. Same logic, batch or stream — exactly the Spark Structured Streaming "same code" promise.

---

## 5. Power BI serving

- **Star schema** over Gold: `FactWagerSettled` (grain: bet slip) + `DimCustomer` (SCD2), `DimEvent`, `DimMarket`, `DimDate`, plus a disconnected `DimReconciliationLog`.
- **DAX** (`powerbi/measures.dax`): Turnover, Payout, **GGR**, **NGR**, **Hold/Margin %**, Active Bettors, time intelligence, and **trust KPIs** (`Reconciliation Drift %`, `Parity Status`, `Data Freshness`).
- **Performance:** dual storage — aggregates in Import, bet‑level drill‑through via DirectQuery on Redshift; incremental refresh by `event_date`.
- **Governance:** RLS by jurisdiction, certified dataset, sensitivity labels; reconciliation status on the report cover so stewards see the data passed the gate.

---

## 6. CI/CD, governance, environments

- **`ci.yml` (runs on every PR):** `flake8` → `pytest` on a **local Spark session** (no cloud, no cost) covering SCD2, settlement rules, streaming transform, and reconciliation. Data‑quality is asserted in `test_settlement_rules.py`; Great Expectations is a documented next step.
- **`cd.yml` (design):** `terraform plan/apply` + Glue/Databricks job deploy, promoted **Dev → SIT → UAT → Prod**, each gated by the reconciliation result.
- **Governance:** Lake Formation tag‑based access, PII tokenisation (`utils/masking.py`), jurisdictional filtering.

---

## 7. Quick start

```bash
pip install -r requirements.txt      # pyspark, pytest, flake8, great_expectations
make test                            # local-Spark unit tests (same as CI)
make demo                            # synthetic bets → settle → reconcile → reports/last_run.json
```

`make demo` prints a parity summary and writes `reports/last_run.json`; the figures quoted anywhere in this repo come from there.

---

## 8. How this maps to the role

| Brief requirement | Evidence |
| --- | --- |
| ETL + proven enterprise migration experience | Reverse‑engineered SQL → medallion; STTM; cutover runbook |
| Power BI background | Semantic model, wagering DAX, RLS, trust KPIs (`powerbi/`) |
| Databricks + PySpark | Pure transforms run on Databricks/Glue/EMR unchanged |
| AWS services (Glue, Redshift, Athena, EMR, Kinesis, MSK, Lake Formation) | Service map §2; Terraform in `infra/`; Athena over Gold; Lake Formation governance |
| Batch **and** streaming | DMS/CDC batch + Kinesis/MSK streaming; one tested transform for both |
| Enterprise assurance | Dual‑run reconciliation (macro + micro), circuit breaker, ADRs |

---

## 9. Honest limitations

- AWS services (Glue, EMR, Redshift, Athena, Kinesis, MSK, Lake Formation) and all Terraform need an AWS account; **not executed in CI**.
- Streaming is proven by a **unit‑tested micro‑batch transform**, not a live Kinesis stream.
- Scale is shown by **pattern** (set‑based, partition‑pruned, broadcast joins) on synthetic data, not a billion‑row benchmark — no throughput multiplier is claimed.
- The Power BI model and `.bim`/DAX are **designed, not deployed** (needs a Power BI workspace).

License: MIT.
