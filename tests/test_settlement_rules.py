"""Settlement / Gold mart data-quality tests (local Spark)."""
from __future__ import annotations

import pytest

from src.transforms.gold_wagering_marts import build_daily_mart
from src.data_generator import legacy_wagers


def test_daily_mart_financials(spark):
    mart = build_daily_mart(spark.createDataFrame(legacy_wagers()))
    row = mart.first().asDict()
    assert row["bet_count"] == 3
    assert row["turnover"] == pytest.approx(350.0)
    assert row["payout"] == pytest.approx(330.0)
    assert row["ggr"] == pytest.approx(20.0)          # turnover - payout
    assert row["hold_pct"] == pytest.approx(20.0 / 350.0)


def test_hold_pct_safe_divide(spark):
    zero = [{"bet_id": "Z", "account_id": "A", "stake_amount": 0.0,
             "payout_amount": 0.0, "event_date": "2026-09-01"}]
    row = build_daily_mart(spark.createDataFrame(zero)).first().asDict()
    assert row["hold_pct"] == 0.0                      # no divide-by-zero
