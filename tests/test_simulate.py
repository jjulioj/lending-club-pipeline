"""Simulação das cargas incrementais a partir do snapshot."""

from __future__ import annotations

from pyspark.sql import functions as F
from tests.factory import portfolio, raw_df

from lending_pipeline.io import read_raw_csv
from lending_pipeline.simulate import SimulationParams, plan_batches, write_batches

PARAMS = SimulationParams(update_fraction=0.6, conflict_fraction=0.15, duplicate_fraction=0.1, seed=7)


def _plan(spark, params=PARAMS):
    return plan_batches(raw_df(spark, portfolio()), params)


def test_one_batch_per_vintage_plus_updates(spark):
    batches = sorted(r[0] for r in _plan(spark).select("_batch_id").distinct().collect())
    assert batches == ["batch_001_2015", "batch_002_2016", "batch_003_2017", "batch_004_updates"]


def test_every_loan_is_delivered_and_updates_come_after_initial_version(spark):
    planned = _plan(spark)
    source_ids = {r["id"] for r in portfolio()}
    assert {r.id for r in planned.select("id").distinct().collect()} == source_ids

    initial = {r.id: r._batch_id for r in planned.where("_kind = 'initial'").collect()}
    updates = {r.id: r._batch_id for r in planned.where("_kind = 'update'").collect()}
    conflicts = {r.id: r._batch_id for r in planned.where("_kind = 'conflict_final'").collect()}
    assert initial, "o cenário de teste precisa gerar versões iniciais"
    for loan_id, batch in updates.items():
        assert initial[loan_id] < batch  # a versão real chega numa carga posterior
    for loan_id, batch in conflicts.items():
        assert initial[loan_id] == batch  # conflito: as duas versões na mesma carga


def test_initial_version_looks_like_a_fresh_loan(spark):
    initial = _plan(spark).where("_kind = 'initial'").first()
    assert initial.loan_status == "Current"
    assert initial.out_prncp == initial.funded_amnt
    assert initial.total_rec_prncp == "0.0"
    assert initial.last_pymnt_d is None


def test_current_and_credit_policy_loans_never_get_updates(spark):
    planned = plan_batches(raw_df(spark, portfolio()), SimulationParams(update_fraction=1.0))
    initial_status = {
        r.loan_status
        for r in planned.where("_kind = 'update' OR _kind = 'conflict_final'").select("loan_status").collect()
    }
    assert "Current" not in initial_status
    assert not any(s.startswith("Does not meet") for s in initial_status)


def test_simulation_is_deterministic(spark):
    def snapshot():
        return sorted(tuple(r) for r in _plan(spark).select("id", "_batch_id", "_kind", "loan_status").collect())

    assert snapshot() == snapshot()


def test_footer_rows_are_distributed_into_vintage_batches(spark):
    footers = _plan(spark).where(F.col("id").startswith("Total amount")).collect()
    assert len(footers) >= 2
    assert all(not r._batch_id.endswith("updates") for r in footers)


def test_written_batches_round_trip(spark, tmp_path):
    planned = _plan(spark)
    summary = write_batches(planned, tmp_path / "landing")
    expected = {r._batch_id: r["count"] for r in planned.groupBy("_batch_id").count().collect()}
    assert {s["batch_id"]: s["rows"] for s in summary} == expected

    for batch_id, rows in expected.items():
        back = read_raw_csv(spark, tmp_path / "landing" / batch_id)
        assert back.count() == rows
    # texto com quebra de linha, vírgula e aspas sobrevive à escrita/leitura do CSV
    all_back = read_raw_csv(spark, str(tmp_path / "landing" / "batch_*"))
    texts = [r.desc for r in all_back.where(F.col("desc").isNotNull()).collect()]
    assert 'linha um\nlinha dois, com vírgula e "aspas"' in texts
