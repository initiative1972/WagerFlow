"""Reconciliation gate tests (local Spark)."""
from __future__ import annotations

import pytest

from src.transforms.reconciliation import reconcile_settlement, enforce
from src.data_generator import legacy_wagers, modern_wagers

KEYS = ["bet_id"]
CDE = ["stake_amount", "payout_amount"]


def test_identical_passes(spark):
    legacy = spark.createDataFrame(legacy_wagers())
    modern = spark.createDataFrame(modern_wagers(introduce_drift=False))
    result = reconcile_settlement(legacy, modern, KEYS, CDE)
    assert result.passed is True
    assert result.mismatches == 0
    enforce(result)  # must not raise


def test_changed_payout_trips_micro_breaker(spark):
    legacy = spark.createDataFrame(legacy_wagers())
    modern = spark.createDataFrame(modern_wagers(introduce_drift=True))
    result = reconcile_settlement(legacy, modern, KEYS, CDE)
    assert result.passed is False
    assert result.mismatches == 1
    with pytest.raises(ValueError):
        enforce(result)


def test_missing_row_is_count_breach(spark):
    legacy = spark.createDataFrame(legacy_wagers())
    modern = spark.createDataFrame(legacy_wagers()[:-1])  # drop one bet
    result = reconcile_settlement(legacy, modern, KEYS, CDE)
    assert result.passed is False
    assert "Row count mismatch" in result.breach_reason
