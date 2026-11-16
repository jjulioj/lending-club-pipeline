"""Tipagem, flags de negócio, hash e deduplicação (Spark puro, sem Delta)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from tests.factory import footer, loan, raw_df

from lending_pipeline.transforms import BUSINESS_COLUMNS, deduplicate_versions, standardize, with_row_hash


def _one(spark, **overrides):
    return standardize(raw_df(spark, [loan(1, **overrides)])).first()


def test_parses_source_formats(spark):
    row = _one(
        spark,
        int_rate=" 13.56%",
        term=" 60 months",
        issue_d="Dec-2015",
        emp_length="< 1 year",
        loan_amnt="1,000.50",
        revol_util="45.1%",
    )
    assert row.int_rate == Decimal("13.5600")
    assert row.term_months == 60
    assert row.issue_date == dt.date(2015, 12, 1)
    assert row.issue_year == 2015
    assert row.issue_quarter == "2015-Q4"
    assert row.emp_length_years == 0
    assert row.loan_amnt == Decimal("1000.50")
    assert row.revol_util == Decimal("45.10")


@pytest.mark.parametrize(("raw", "expected"), [("10+ years", 10), ("3 years", 3), ("n/a", None), ("", None)])
def test_emp_length(spark, raw, expected):
    assert _one(spark, emp_length=raw).emp_length_years == expected


def test_invalid_values_become_null_instead_of_failing(spark):
    # Com ANSI ligado, um cast comum derrubaria o job; aqui vira nulo e a qualidade decide
    row = _one(spark, int_rate="abc", loan_amnt="", issue_d="2015-12", term="??")
    assert row.int_rate is None
    assert row.loan_amnt is None
    assert row.issue_date is None
    assert row.term_months is None


def test_footer_row_has_null_loan_id(spark):
    row = standardize(raw_df(spark, [footer()])).first()
    assert row.loan_id is None
    assert row.source_id.startswith("Total amount funded")


@pytest.mark.parametrize(
    ("status", "is_default", "is_resolved", "meets_policy", "normalized"),
    [
        ("Fully Paid", False, True, True, "Fully Paid"),
        ("Current", False, False, True, "Current"),
        ("Charged Off", True, True, True, "Charged Off"),
        ("Default", True, True, True, "Default"),
        ("Late (31-120 days)", False, False, True, "Late (31-120 days)"),
        ("In Grace Period", False, False, True, "In Grace Period"),
        ("Does not meet the credit policy. Status:Charged Off", True, True, False, "Charged Off"),
        ("Does not meet the credit policy. Status:Fully Paid", False, True, False, "Fully Paid"),
    ],
)
def test_default_definition(spark, status, is_default, is_resolved, meets_policy, normalized):
    row = _one(spark, status=status)
    assert row.is_default is is_default
    assert row.is_resolved is is_resolved
    assert row.meets_credit_policy is meets_policy
    assert row.loan_status_normalized == normalized


def test_standardize_publishes_all_business_columns(spark):
    df = standardize(raw_df(spark, [loan(1)]))
    assert set(BUSINESS_COLUMNS) <= set(df.columns)


def test_row_hash_changes_only_when_content_changes(spark):
    rows = [loan(1), loan(2), loan(3, out_prncp="123.0")]
    df = with_row_hash(standardize(raw_df(spark, rows))).select("loan_id", "_row_hash").collect()
    hashes = {r.loan_id: r._row_hash for r in df}
    same_content = with_row_hash(standardize(raw_df(spark, [loan(1)]))).first()._row_hash
    assert hashes[1] == same_content
    assert hashes[1] != hashes[3]


def test_deduplicate_keeps_most_recent_version(spark):
    initial = loan(1, status="Current", last_pymnt_d=None, last_credit_pull_d="Dec-2015", total_pymnt="0.0")
    final = loan(1, status="Fully Paid")
    exact_dup = loan(2)
    rows = [initial, final, exact_dup, exact_dup]
    df = deduplicate_versions(with_row_hash(standardize(raw_df(spark, rows))))
    result = {r.loan_id: r.loan_status for r in df.collect()}
    assert result == {1: "Fully Paid", 2: "Fully Paid"}
