"""Indicadores da gold calculados à mão num cenário pequeno."""

from __future__ import annotations

from decimal import Decimal

import pytest
from tests.factory import loan, raw_df

from lending_pipeline.gold import build_cube, kpi_queries, run_kpi_queries
from lending_pipeline.transforms import standardize


@pytest.fixture
def kpis(spark):
    rows = [
        # amount, rate: ponderada = (10*10000 + 20*30000) / 40000 = 17.5 ; simples = 15
        loan(1, issue_d="Jan-2015", status="Fully Paid", amount=10000.0, int_rate="10.0", grade="A"),
        loan(2, issue_d="Feb-2015", status="Charged Off", amount=30000.0, int_rate="20.0", grade="C"),
        loan(3, issue_d="Mar-2016", status="Current", amount=20000.0, int_rate="12.0", grade="A"),
        loan(4, issue_d="Apr-2016", status="Late (31-120 days)", amount=10000.0, int_rate="16.0", grade="B"),
    ]
    silver = standardize(raw_df(spark, rows))
    return run_kpi_queries(spark, build_cube(silver))


def test_all_sql_files_are_loaded():
    assert {"kpi_portfolio_summary", "kpi_by_period", "kpi_by_state", "kpi_by_grade", "kpi_by_purpose"} <= set(
        kpi_queries()
    )


def test_portfolio_summary(kpis):
    s = kpis["kpi_portfolio_summary"].first()
    assert s.loan_count == 4
    assert s.originated_amount == Decimal("70000.00")
    assert s.funded_amount == Decimal("70000.00")
    # saldo: Current e Late com 50% em aberto (factory) -> 10000 + 5000
    assert s.outstanding_principal == Decimal("15000.00")
    assert s.default_count == 1
    assert s.default_funded_amount == Decimal("30000.00")
    # perda de principal = 30000 - 30% recebido
    assert s.default_principal_loss == Decimal("21000.00")
    assert s.default_rate == pytest.approx(0.25)
    # encerrados: Fully Paid + Charged Off
    assert s.default_rate_resolved == pytest.approx(0.5)


def test_weighted_rate_differs_from_simple_average(kpis):
    y2015 = {r.issue_year: r for r in kpis["kpi_by_period"].collect()}[2015]
    assert float(y2015.avg_int_rate) == pytest.approx(15.0)
    assert float(y2015.weighted_avg_int_rate) == pytest.approx(17.5)


def test_ratios_are_recomputed_per_cut_not_averaged(kpis):
    by_grade = {r.grade: r for r in kpis["kpi_by_grade"].collect()}
    assert by_grade["A"].loan_count == 2
    assert by_grade["A"].default_rate == pytest.approx(0.0)
    assert by_grade["C"].default_rate == pytest.approx(1.0)
    # sem empréstimos encerrados no grade B: taxa "sobre encerrados" fica nula, não divide por zero
    assert by_grade["B"].default_rate_resolved is None
