"""Streaming odds transform tests (static micro-batch, local Spark)."""
from __future__ import annotations

from src.transforms.streaming_odds import transform_odds_events
from src.data_generator import odds_events


def test_dedupes_exact_duplicate_events(spark):
    out = transform_odds_events(spark.createDataFrame(odds_events()))
    assert out.count() == 2  # one exact M1 duplicate removed


def test_flags_stale_and_fresh(spark):
    out = transform_odds_events(spark.createDataFrame(odds_events()))
    rows = {r["market_id"]: r["is_stale"] for r in out.collect()}
    assert rows["M2"] is True   # 2020 timestamp is stale
    assert rows["M1"] is False  # 2099 timestamp is not stale
