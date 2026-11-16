"""Regras de qualidade linha a linha e portões de carga/tabela."""

from __future__ import annotations

import pytest
from tests.factory import footer, loan, raw_df

from lending_pipeline.quality import (
    QualityGateError,
    SchemaContractError,
    apply_row_rules,
    check_quarantine_rate,
    check_required_columns,
    check_unique_key,
    split_valid_quarantine,
)
from lending_pipeline.transforms import standardize


def _check(spark, rows):
    return apply_row_rules(standardize(raw_df(spark, rows)))


def test_valid_loan_has_no_errors_or_warnings(spark):
    row = _check(spark, [loan(1)]).first()
    assert row._dq_errors == []
    assert row._dq_warnings == []


@pytest.mark.parametrize(
    ("overrides", "rule"),
    [
        ({"term": " 48 months"}, "term_invalid"),
        ({"grade": "H"}, "grade_invalid"),
        ({"status": "Restructured"}, "loan_status_invalid"),
        ({"loan_amnt": "-10"}, "loan_amnt_invalid"),
        ({"int_rate": "abc"}, "int_rate_invalid"),
        ({"issue_d": "13/2015"}, "issue_date_invalid"),
        ({"out_prncp": ""}, "out_prncp_invalid"),
    ],
)
def test_error_rules_send_row_to_quarantine(spark, overrides, rule):
    valid, quarantine = split_valid_quarantine(_check(spark, [loan(1, **overrides)]))
    assert valid.count() == 0
    assert rule in quarantine.first()._dq_errors


def test_footer_row_goes_to_quarantine(spark):
    _, quarantine = split_valid_quarantine(_check(spark, [footer()]))
    assert "loan_id_invalid" in quarantine.first()._dq_errors


@pytest.mark.parametrize(
    ("overrides", "rule"),
    [
        ({"funded_amnt_inv": "20000.0"}, "funded_inv_gt_funded"),
        ({"sub_grade": "C1"}, "sub_grade_mismatch"),
        ({"dti": "999"}, "dti_out_of_range"),
        ({"status": "Fully Paid", "out_prncp": "50.0"}, "resolved_with_open_balance"),
    ],
)
def test_warning_rules_keep_the_row(spark, overrides, rule):
    valid, quarantine = split_valid_quarantine(_check(spark, [loan(1, **overrides)]))
    assert quarantine.count() == 0
    assert rule in valid.first()._dq_warnings


def test_required_columns_contract():
    check_required_columns(["id", "loan_amnt"], required=["id", "loan_amnt"])
    with pytest.raises(SchemaContractError, match="loan_status"):
        check_required_columns(["id"], required=["id", "loan_status"])


def test_quarantine_rate_gate():
    check_quarantine_rate(rows_total=1000, rows_quarantined=10, max_rate=0.05)
    with pytest.raises(QualityGateError):
        check_quarantine_rate(rows_total=1000, rows_quarantined=60, max_rate=0.05)
    with pytest.raises(QualityGateError):
        check_quarantine_rate(rows_total=0, rows_quarantined=0, max_rate=0.05)


def test_unique_key_gate(spark):
    ok = spark.createDataFrame([(1,), (2,)], "loan_id bigint")
    check_unique_key(ok)
    with pytest.raises(QualityGateError, match="duplicado"):
        check_unique_key(spark.createDataFrame([(1,), (1,)], "loan_id bigint"))
    with pytest.raises(QualityGateError, match="nulo"):
        check_unique_key(spark.createDataFrame([(1,), (None,)], "loan_id bigint"))
