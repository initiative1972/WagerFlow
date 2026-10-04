"""Gold layer: wagering settlement marts.

Replaces the legacy procedure's cursor-based rollups with set-based aggregation.
Pure Spark; the GGR/Hold logic is what the reconciliation gate validates.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_daily_mart(settled: DataFrame) -> DataFrame:
    """Aggregate settled bet slips to a daily financial mart.

    turnover = SUM(stake), payout = SUM(payout), GGR = turnover - payout,
    hold_pct = GGR / turnover (safe divide).
    """
    return (
        settled.groupBy("event_date")
        .agg(
            F.count("*").alias("bet_count"),
            F.sum("stake_amount").alias("turnover"),
            F.sum("payout_amount").alias("payout"),
        )
        .withColumn("ggr", F.col("turnover") - F.col("payout"))
        .withColumn("hold_pct", F.when(F.col("turnover") != 0,
                                       F.col("ggr") / F.col("turnover")).otherwise(F.lit(0.0)))
    )
