"""Dual-run reconciliation gate for wagering settlement.

Macro aggregates (counts, turnover, payout, GGR) AND a micro SHA-256 row-hash
parity check across Critical Data Elements. ``reconcile_settlement`` computes a
result; ``enforce`` is the circuit breaker that raises when parity fails.

Drift is expressed as a true percentage (0.5 == 0.5%).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, functions as F

from src.utils.hashing import cde_hash


@dataclass
class ReconciliationResult:
    passed: bool
    drift_pct: float
    mismatches: int
    metrics: dict = field(default_factory=dict)
    breach_reason: str = ""


def _agg(df: DataFrame) -> dict:
    row = df.select(
        F.count("*").alias("rows"),
        F.coalesce(F.sum("stake_amount"), F.lit(0.0)).alias("turnover"),
        F.coalesce(F.sum("payout_amount"), F.lit(0.0)).alias("payout"),
    ).first().asDict()
    row["ggr"] = float(row["turnover"]) - float(row["payout"])
    return row


def reconcile_settlement(
    legacy: DataFrame,
    modern: DataFrame,
    keys: list[str],
    cde_cols: list[str],
    tolerance_pct: float = 0.001,
) -> ReconciliationResult:
    leg, mod = _agg(legacy), _agg(modern)

    # Micro: SHA-256 row-hash parity across CDEs
    leg_h = legacy.select(*keys, cde_hash(cde_cols).alias("row_hash")).alias("l")
    mod_h = modern.select(*keys, cde_hash(cde_cols).alias("row_hash")).alias("m")
    cmp = leg_h.join(mod_h, on=keys, how="full_outer")
    mismatches = cmp.where(
        F.col("l.row_hash").isNull()
        | F.col("m.row_hash").isNull()
        | (F.col("l.row_hash") != F.col("m.row_hash"))
    ).count()

    turnover_drift = (
        abs(leg["turnover"] - mod["turnover"]) / leg["turnover"] * 100
        if leg["turnover"] else 0.0
    )
    metrics = {"legacy": leg, "modern": mod, "turnover_drift_pct": round(turnover_drift, 6)}

    if leg["rows"] != mod["rows"]:
        return ReconciliationResult(
            False, 100.0, mismatches, metrics,
            f"Row count mismatch: legacy {leg['rows']} vs modern {mod['rows']}",
        )
    if mismatches > 0:
        return ReconciliationResult(
            False, turnover_drift, mismatches, metrics,
            f"{mismatches} row-hash mismatches across CDEs",
        )
    if turnover_drift > tolerance_pct:
        return ReconciliationResult(
            False, turnover_drift, mismatches, metrics,
            f"Turnover drift {turnover_drift:.6f}% exceeds tolerance {tolerance_pct}%",
        )
    return ReconciliationResult(True, turnover_drift, 0, metrics, "Parity within tolerance")


def enforce(result: ReconciliationResult) -> ReconciliationResult:
    """Circuit breaker: raise if reconciliation did not pass."""
    if not result.passed:
        raise ValueError(f"Reconciliation breaker: {result.breach_reason}")
    return result
