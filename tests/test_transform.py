"""Unit test untuk logika transformasi PySpark (tanpa database).

Jalankan:
    pip install -r requirements-dev.txt
    pytest -v
"""
import datetime
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from pyspark.sql import SparkSession  # noqa: E402

from spark_fraud_etl import transform_fraud_data  # noqa: E402

COLUMNS = ["transaction_id", "transaction_date", "merchant_category",
           "transaction_amount", "is_fraud", "fraud_type"]
SAMPLE_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "sample_bank_fraud.csv")


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("test-fraud-etl")
               .config("spark.ui.enabled", "false").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _summary(spark, rows):
    df = spark.createDataFrame(rows, COLUMNS)
    out = transform_fraud_data(df).collect()
    return {(r.transaction_date, r.merchant_category, r.fraud_type): r for r in out}


def test_none_fraud_type_becomes_legitimate(spark):
    rows = [
        ("T1", "2024-01-01", "Grocery", 10.0, 0, "None"),
        ("T2", "2024-01-01", "Grocery", 20.0, 0, None),
    ]
    out = _summary(spark, rows)
    key = (datetime.date(2024, 1, 1), "Grocery", "Legitimate")
    assert list(out) == [key]
    assert out[key].total_transactions == 2
    assert out[key].total_amount == pytest.approx(30.0)


def test_fraud_loss_only_counts_fraudulent_transactions(spark):
    rows = [
        ("T1", "2024-01-02", "Jewelry", 500.0, 1, "Card Cloning"),
        ("T2", "2024-01-02", "Jewelry", 300.0, 1, "Card Cloning"),
        ("T3", "2024-01-02", "Jewelry", 100.0, 0, "None"),
    ]
    out = _summary(spark, rows)
    fraud = out[(datetime.date(2024, 1, 2), "Jewelry", "Card Cloning")]
    legit = out[(datetime.date(2024, 1, 2), "Jewelry", "Legitimate")]
    assert fraud.fraud_cases_count == 2
    assert fraud.fraud_loss_amount == pytest.approx(800.0)
    assert legit.fraud_cases_count == 0
    assert legit.fraud_loss_amount == pytest.approx(0.0)


def test_grouping_keys_are_unique(spark):
    rows = [
        ("T1", "2024-01-03", "Travel", 50.0, 0, "None"),
        ("T2", "2024-01-03", "Travel", 70.0, 0, "None"),
        ("T3", "2024-01-04", "Travel", 90.0, 0, "None"),
        ("T4", "2024-01-03", "Fuel", 40.0, 1, "Phishing"),
    ]
    df = spark.createDataFrame(rows, COLUMNS)
    summary = transform_fraud_data(df)
    keys = ["transaction_date", "merchant_category", "fraud_type"]
    assert summary.count() == summary.select(keys).distinct().count() == 3


def test_sample_csv_totals_are_preserved(spark):
    df = spark.read.csv(SAMPLE_CSV, header=True, inferSchema=True)
    summary = transform_fraud_data(df)
    totals = summary.groupBy().sum("total_transactions", "fraud_cases_count").collect()[0]
    assert totals[0] == df.count()
    assert totals[1] == df.filter("is_fraud = 1").count()
