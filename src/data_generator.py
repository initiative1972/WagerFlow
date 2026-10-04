"""Synthetic data generator and local demo entrypoint for WagerFlow-Modernize.

All data is SYNTHETIC. No proprietary or customer data is used.

Pure factories return plain dicts (used by the tests). ``main`` builds a local
Spark session, runs the SCD2 upsert (initial + change feed), builds the Gold
mart, runs the reconciliation gate, and writes ``reports/last_run.json``.
"""
from __future__ import annotations

import json
from pathlib import Path


# --------------------------------------------------------------------------- #
# Customer dimension (SCD2)                                                    #
# --------------------------------------------------------------------------- #
def customers_v1() -> list[dict]:
    return [
        {"account_id": "A001", "account_status": "ACTIVE", "risk_tier": "LOW",
         "daily_deposit_limit": 100, "jurisdiction": "VIC", "updated_at": "2026-09-01 00:00:00"},
        {"account_id": "A002", "account_status": "ACTIVE", "risk_tier": "MED",
         "daily_deposit_limit": 200, "jurisdiction": "NSW", "updated_at": "2026-09-01 00:00:00"},
        {"account_id": "A003", "account_status": "ACTIVE", "risk_tier": "HIGH",
         "daily_deposit_limit": 500, "jurisdiction": "QLD", "updated_at": "2026-09-01 00:00:00"},
    ]


def customers_v2() -> list[dict]:
    """Change feed: A001 risk tier changes (new version); A002 unchanged (must be
    retained, not lost); A004 brand new; A003 absent (stays current)."""
    return [
        {"account_id": "A001", "account_status": "ACTIVE", "risk_tier": "HIGH",
         "daily_deposit_limit": 100, "jurisdiction": "VIC", "updated_at": "2026-09-02 00:00:00"},
        {"account_id": "A002", "account_status": "ACTIVE", "risk_tier": "MED",
         "daily_deposit_limit": 200, "jurisdiction": "NSW", "updated_at": "2026-09-02 00:00:00"},
        {"account_id": "A004", "account_status": "ACTIVE", "risk_tier": "LOW",
         "daily_deposit_limit": 300, "jurisdiction": "WA", "updated_at": "2026-09-02 00:00:00"},
    ]


# --------------------------------------------------------------------------- #
# Wager fact (settlement + reconciliation)                                     #
# --------------------------------------------------------------------------- #
def legacy_wagers() -> list[dict]:
    return [
        {"bet_id": "B1", "account_id": "A001", "stake_amount": 100.0,
         "payout_amount": 80.0, "event_date": "2026-09-01"},
        {"bet_id": "B2", "account_id": "A002", "stake_amount": 50.0,
         "payout_amount": 0.0, "event_date": "2026-09-01"},
        {"bet_id": "B3", "account_id": "A003", "stake_amount": 200.0,
         "payout_amount": 250.0, "event_date": "2026-09-01"},
    ]


def modern_wagers(introduce_drift: bool = False) -> list[dict]:
    rows = [dict(r) for r in legacy_wagers()]
    if introduce_drift:
        rows[0]["payout_amount"] = 90.0  # a settlement bug the gate must catch
    return rows


# --------------------------------------------------------------------------- #
# Odds stream events                                                           #
# --------------------------------------------------------------------------- #
def odds_events() -> list[dict]:
    return [
        {"market_id": "M1", "event_ts": "2099-01-01 00:00:00", "price": 1.5},
        {"market_id": "M1", "event_ts": "2099-01-01 00:00:00", "price": 1.5},  # exact dup
        {"market_id": "M2", "event_ts": "2020-01-01 00:00:00", "price": 2.0},  # stale
    ]


# --------------------------------------------------------------------------- #
# Demo                                                                         #
# --------------------------------------------------------------------------- #
def _build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("wagerflow-demo")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def main() -> dict:
    from src.transforms.silver_scd2 import process_customer_scd2
    from src.transforms.gold_wagering_marts import build_daily_mart
    from src.transforms.reconciliation import reconcile_settlement

    spark = _build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    dim = process_customer_scd2(spark.createDataFrame(customers_v1()), None)
    dim = process_customer_scd2(spark.createDataFrame(customers_v2()), dim)
    current_customers = dim.filter("is_current").count()

    mart = build_daily_mart(spark.createDataFrame(legacy_wagers()))
    mart_row = mart.first().asDict()

    recon = reconcile_settlement(
        spark.createDataFrame(legacy_wagers()),
        spark.createDataFrame(modern_wagers(introduce_drift=False)),
        keys=["bet_id"],
        cde_cols=["stake_amount", "payout_amount"],
    )

    report = {
        "current_customers": current_customers,
        "daily_mart": {k: (float(v) if isinstance(v, (int, float)) else v)
                       for k, v in mart_row.items()},
        "reconciliation": {"passed": recon.passed, "drift_pct": recon.drift_pct,
                           "mismatches": recon.mismatches, "reason": recon.breach_reason},
    }
    out = Path("reports")
    out.mkdir(exist_ok=True)
    (out / "last_run.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))

    spark.stop()
    return report


if __name__ == "__main__":
    main()
