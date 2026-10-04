"""Streaming ingestion of odds/bet events — batch AND streaming from one transform.

The pure ``transform_odds_events`` is a micro-batch transform (DataFrame ->
DataFrame) unit-tested on a static DataFrame and reused verbatim by batch and by
the streaming driver. ``start_odds_stream`` is the design wrapper that reads from
Kinesis/MSK; it needs AWS and is not run in CI.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

STALE_AFTER = "INTERVAL 5 MINUTES"


def transform_odds_events(df: DataFrame) -> DataFrame:
    """Parse event time, drop exact duplicate events per market+timestamp, and
    flag stale prices. Streaming-safe (no non-time windowing)."""
    deduped = (
        df.withColumn("event_ts", F.col("event_ts").cast("timestamp"))
        .dropDuplicates(["market_id", "event_ts"])
    )
    return deduped.withColumn(
        "is_stale", F.col("event_ts") < (F.current_timestamp() - F.expr(STALE_AFTER))
    )


def start_odds_stream(spark, kinesis_options: dict, sink_path: str):
    """Design artefact (needs Kinesis/MSK). Calls the SAME transform as batch."""
    stream = spark.readStream.format("kinesis").options(**kinesis_options).load()
    out = transform_odds_events(stream)
    return (
        out.writeStream.format("delta")
        .option("checkpointLocation", f"{sink_path}/_chk")
        .outputMode("append")
        .start(sink_path)
    )
