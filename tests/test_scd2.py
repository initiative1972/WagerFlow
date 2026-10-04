"""SCD2 behaviour tests (local Spark)."""
from __future__ import annotations

from src.transforms.silver_scd2 import process_customer_scd2
from src.data_generator import customers_v1, customers_v2


def _current(df):
    return df.filter("is_current")


def test_initial_load_all_current(spark):
    dim = process_customer_scd2(spark.createDataFrame(customers_v1()), None)
    assert _current(dim).count() == 3
    assert dim.count() == 3


def test_rerun_unchanged_no_loss_no_dupes(spark):
    v1 = spark.createDataFrame(customers_v1())
    dim = process_customer_scd2(v1, None)
    dim = process_customer_scd2(v1, dim)  # identical feed again
    assert dim.count() == 3                       # the row-loss bug would drop rows
    assert _current(dim).count() == 3
    assert _current(dim).select("account_id").distinct().count() == 3


def test_change_expires_old_and_adds_new(spark):
    dim = process_customer_scd2(spark.createDataFrame(customers_v1()), None)
    dim = process_customer_scd2(spark.createDataFrame(customers_v2()), dim)

    a001 = dim.filter("account_id = 'A001'")      # risk tier changed
    assert a001.count() == 2
    assert a001.filter("is_current").count() == 1
    assert a001.filter("is_current = false").count() == 1


def test_absent_key_is_retained_not_lost(spark):
    dim = process_customer_scd2(spark.createDataFrame(customers_v1()), None)
    dim = process_customer_scd2(spark.createDataFrame(customers_v2()), dim)
    # A003 is absent from v2 but must remain current (the fixed bug)
    assert dim.filter("account_id = 'A003' and is_current").count() == 1
    # Exactly one current row per key overall; A004 (new) present
    cur = _current(dim)
    assert cur.count() == cur.select("account_id").distinct().count()
    assert dim.filter("account_id = 'A004' and is_current").count() == 1
