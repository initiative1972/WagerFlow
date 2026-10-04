"""PII tokenisation helpers for Responsible Wagering / privacy compliance.

Deterministic tokenisation (not reversible) so masked values still join and
deduplicate consistently across runs, without exposing the raw PII.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def tokenize(column_name: str) -> "F.Column":
    """Deterministic, non-reversible token for a PII column."""
    return F.sha2(F.lower(F.trim(F.col(column_name).cast("string"))), 256)


def mask_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """Replace each named PII column with its deterministic token in place."""
    out = df
    for col in columns:
        out = out.withColumn(col, tokenize(col))
    return out
