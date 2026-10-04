"""Deterministic hashing over Critical Data Elements (CDEs).

A single source of truth for the CDE hash used by both the Silver SCD2 change
detection and the reconciliation gate, so the two can never drift apart.
"""
from __future__ import annotations

from pyspark.sql import Column, functions as F

NULL_TOKEN = "\u2205"  # sentinel so NULLs hash distinctly from empty strings


def cde_hash(cde_cols: list[str]) -> Column:
    """Return a SHA-256 Column over the given columns.

    NULLs are coalesced to a sentinel so ``(A, NULL)`` and ``(A, '')`` do not
    collide, and so a dropped value is detectable.
    """
    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in cde_cols],
        ),
        256,
    )
