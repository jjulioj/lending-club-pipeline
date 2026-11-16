"""Preparação da carga para a silver (sem escrita): quarentena, deduplicação e colunas."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import functions as F
from tests.factory import footer, loan, raw_df

from lending_pipeline.silver import SILVER_COLUMNS, prepare_batch


def _bronze(spark, rows):
    return (
        raw_df(spark, rows)
        .withColumn("_batch_id", F.lit("batch_001_2015"))
        .withColumn("_batch_seq", F.lit(1))
        .withColumn("_source_file", F.lit("file:///landing/batch_001_2015/part-0.csv"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit("run-1"))
    )


def test_prepare_batch_splits_and_deduplicates(spark):
    rows = [
        loan(1),
        loan(1),  # duplicata exata
        loan(2, status="Current", last_pymnt_d=None, last_credit_pull_d="Jan-2015", total_pymnt="0.0"),
        loan(2, status="Charged Off"),  # versão mais recente na mesma carga
        loan(3, grade="Z"),  # erro de qualidade
        footer(),
    ]
    prepared = prepare_batch(_bronze(spark, rows), datetime(2026, 11, 16, tzinfo=UTC))

    assert prepared.checked.count() == 6
    assert prepared.quarantine.count() == 2
    merged = {r.loan_id: r.loan_status for r in prepared.valid_deduped.collect()}
    assert merged == {1: "Fully Paid", 2: "Charged Off"}


def test_quarantine_keeps_original_values_for_investigation(spark):
    prepared = prepare_batch(_bronze(spark, [loan(3, grade="Z", int_rate="abc")]), datetime.now(UTC))
    row = prepared.quarantine.first()
    assert row._raw.grade == "Z"
    assert row._raw.int_rate == "abc"
    assert set(row._dq_errors) == {"grade_invalid", "int_rate_invalid"}


def test_silver_output_has_contract_columns_and_metadata(spark):
    prepared = prepare_batch(_bronze(spark, [loan(1)]), datetime(2026, 11, 16, tzinfo=UTC))
    assert prepared.valid_deduped.columns == SILVER_COLUMNS
    row = prepared.valid_deduped.first()
    assert row._batch_id == "batch_001_2015"
    assert row._first_loaded_at == row._last_updated_at
