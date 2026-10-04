"""Silver layer: SCD Type 2 for wagering customer dimensions.

Pure Spark (no Delta / cloud dependency) so it unit-tests on a local Spark
session and lifts onto Databricks, Glue, or EMR unchanged.

Fixes a real data-loss bug found in a naive implementation: using ``left_anti``
on the incoming feed to find "unchanged" rows drops current rows that are
*present and unchanged*, silently shrinking the dimension each run. Here, current
rows are partitioned into *expired* (changed keys) vs *kept* (everything else),
so no unchanged row is ever lost and no duplicate current version is created.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from src.utils.hashing import cde_hash

CDE_COLS = ["account_status", "risk_tier", "daily_deposit_limit", "jurisdiction"]
_END = "9999-12-31 23:59:59"


def process_customer_scd2(
    incoming_df: DataFrame,
    current_dim_df: DataFrame | None,
    key: str = "account_id",
    cde_cols: list[str] = CDE_COLS,
) -> DataFrame:
    incoming = (
        incoming_df.withColumn("row_hash", cde_hash(cde_cols))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )

    def as_current(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("start_ts", F.col("updated_at"))
            .withColumn("end_ts", F.to_timestamp(F.lit(_END)))
            .withColumn("is_current", F.lit(True))
        )

    # Initial historical load
    if current_dim_df is None or len(current_dim_df.take(1)) == 0:
        return as_current(incoming)

    active = current_dim_df.filter("is_current")
    inactive = current_dim_df.filter("is_current = false")

    active_keys = active.select(key, F.col("row_hash").alias("_h"))
    cmp = incoming.join(active_keys, key, "left")

    # Keys that already exist and whose CDE hash changed
    changed_keys = (
        cmp.where(F.col("_h").isNotNull() & (F.col("row_hash") != F.col("_h")))
        .select(key)
    )

    # New current versions: brand-new keys OR changed keys
    new_versions = as_current(
        cmp.where(F.col("_h").isNull() | (F.col("row_hash") != F.col("_h")))
        .select(*incoming.columns)
    )

    # Expire the old current version for changed keys, closing end_ts at the new start
    new_start = incoming.select(key, F.col("updated_at").alias("_new_start"))
    expired = (
        active.join(changed_keys, key, "left_semi")
        .join(new_start, key, "left")
        .withColumn("end_ts", F.col("_new_start"))
        .withColumn("is_current", F.lit(False))
        .drop("_new_start")
    )

    # Retain all current rows that did NOT change (present-unchanged AND not-in-feed)
    kept = active.join(changed_keys, key, "left_anti")

    return inactive.unionByName(expired).unionByName(kept).unionByName(new_versions)
