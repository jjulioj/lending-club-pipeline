"""Testes de integração do pipeline completo com Delta Lake.

Cobrem os requisitos de operação do case:
- o estado final da silver reproduz o snapshot original (novos + atualizações);
- reprocessar a mesma carga não duplica nem altera nada;
- reprocessar uma carga antiga não sobrescreve dados mais novos;
- reentrega de arquivo corrigido é detectada e aplicada;
- carga inválida (qualidade ou contrato) falha sem tocar na silver;
- coluna nova na origem é absorvida na bronze sem quebrar a silver.
"""

from __future__ import annotations

import csv
import dataclasses

import pytest
from pyspark.sql import functions as F
from tests.factory import HEADER, loan, portfolio, raw_df, write_csv

from lending_pipeline.control import FAILED, SKIPPED, SUCCESS
from lending_pipeline.lake import tables
from lending_pipeline.pipeline import run_pipeline
from lending_pipeline.reporting import demo_reprocess, kpi_report, status_table
from lending_pipeline.silver import SILVER_COLUMNS
from lending_pipeline.simulate import SimulationParams, plan_batches, write_batches
from lending_pipeline.transforms import standardize

pytestmark = pytest.mark.delta

PARAMS = SimulationParams(update_fraction=0.6, conflict_fraction=0.15, duplicate_fraction=0.1, seed=7)


@pytest.fixture
def env(config, tmp_path):
    return dataclasses.replace(
        config,
        landing_dir=tmp_path / "landing",
        lake_dir=tmp_path / "lake",
        reports_dir=tmp_path / "reports",
        max_quarantine_rate=0.5,
    )


@pytest.fixture
def processed(spark, env):
    """Cenário padrão: carteira simulada em 4 cargas e processada uma vez."""
    write_batches(plan_batches(raw_df(spark, portfolio()), PARAMS), env.landing_dir)
    summary = run_pipeline(spark, env)
    assert not summary.failed, [r.error_message for r in summary.records]
    return env


def _delta(spark, path):
    return spark.read.format("delta").load(path)


def _silver(spark, env):
    return _delta(spark, tables(env).silver_loans)


def _signature(spark, env):
    row = _silver(spark, env).agg(F.count("*").alias("n"), F.expr("bit_xor(xxhash64(loan_id, _row_hash))").alias("s"))
    return tuple(row.first())


def _expected_final_state(spark):
    snapshot = standardize(raw_df(spark, portfolio())).where("loan_id IS NOT NULL")
    return {r.loan_id: (r.loan_status, r.out_prncp) for r in snapshot.collect()}


def _rewrite_landing(env, batch_id, loan_id, **changes):
    """Simula reentrega de arquivo corrigido: altera linhas de um empréstimo no CSV da carga."""
    batch_dir = env.landing_dir / batch_id
    part = next(p for p in batch_dir.glob("part-*.csv"))
    with part.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames, rows = reader.fieldnames, list(reader)
    for row in rows:
        if row["id"] == str(loan_id):
            row.update(changes)
    with part.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    # O Hadoop guarda um .crc ao lado do arquivo; com o conteúdo alterado ele precisa sair
    for crc in batch_dir.glob(".*.crc"):
        crc.unlink()


def test_end_to_end_reproduces_the_snapshot(spark, processed):
    env = processed
    control = _delta(spark, tables(env).ops_load_control).orderBy("batch_seq").collect()
    assert [r.status for r in control] == [SUCCESS] * 4
    assert sum(r.rows_updated for r in control) > 0, "o cenário precisa exercitar atualizações"

    expected = _expected_final_state(spark)
    silver = _silver(spark, env)
    assert silver.count() == len(expected)
    assert {r.loan_id: (r.loan_status, r.out_prncp) for r in silver.collect()} == expected

    quarantine = _delta(spark, tables(env).silver_quarantine).collect()
    assert len(quarantine) >= 2
    assert all("loan_id_invalid" in r._dq_errors for r in quarantine)

    summary = _delta(spark, tables(env).gold("kpi_portfolio_summary")).first()
    assert summary.loan_count == len(expected)


def test_reprocessing_is_idempotent_and_order_safe(spark, processed):
    env = processed
    t = tables(env)
    before = _signature(spark, env)
    bronze_before = _delta(spark, t.bronze_loans).where("_batch_id = 'batch_001_2015'").count()

    # 1) Mesmas cargas de novo: tudo ignorado pelo checksum
    rerun = run_pipeline(spark, env)
    assert {r.status for r in rerun.records} == {SKIPPED}

    # 2) Carga antiga reprocessada à força: nada inserido, nada atualizado, nada duplicado.
    #    Os empréstimos dela que foram atualizados depois NÃO voltam para a versão antiga.
    forced = run_pipeline(spark, env, only_batches=["batch_001_2015"], force=True).records[0]
    assert (forced.status, forced.rows_inserted, forced.rows_updated) == (SUCCESS, 0, 0)
    assert _signature(spark, env) == before
    assert _delta(spark, t.bronze_loans).where("_batch_id = 'batch_001_2015'").count() == bronze_before

    # 3) Reentrega corrigida de uma carga: detectada pelo checksum e aplicada sem --force
    target = _silver(spark, env).where("_batch_id = 'batch_003_2017'").orderBy("loan_id").first().loan_id
    _rewrite_landing(env, "batch_003_2017", target, int_rate="21.5")
    redelivery = run_pipeline(spark, env)
    by_batch = {r.batch_id: r for r in redelivery.records}
    assert by_batch["batch_003_2017"].status == SUCCESS
    assert by_batch["batch_003_2017"].rows_updated >= 1
    assert by_batch["batch_001_2015"].status == SKIPPED
    updated = _silver(spark, env).where(F.col("loan_id") == target).first()
    assert float(updated.int_rate) == pytest.approx(21.5)
    assert _silver(spark, env).count() == before[0]


def test_failed_batches_leave_silver_untouched(spark, processed):
    env = processed
    t = tables(env)
    before = _signature(spark, env)

    # Portão de qualidade: 10 de 11 linhas inválidas
    bad = [loan(50_000 + i, issue_d="Jan-2018", grade="Z") for i in range(10)] + [loan(60_000, issue_d="Jan-2018")]
    bad_dir = env.landing_dir / "batch_005_bad"
    write_csv(bad, bad_dir / "part-00000.csv")
    record = run_pipeline(spark, env).records[-1]
    assert record.status == FAILED
    assert "QualityGateError" in record.error_message
    assert _signature(spark, env) == before
    assert _silver(spark, env).where("loan_id = 60000").count() == 0
    # a quarentena fica gravada para investigação
    assert _delta(spark, t.silver_quarantine).where("_batch_id = 'batch_005_bad'").count() == 10

    # Contrato: carga sem coluna obrigatória falha antes de escrever na bronze
    for f in bad_dir.iterdir():
        f.unlink()
    bad_dir.rmdir()
    columns = [c for c in HEADER if c != "loan_status"]
    write_csv([loan(70_000, issue_d="Feb-2018")], env.landing_dir / "batch_006_contract" / "part-00000.csv", columns)
    record = run_pipeline(spark, env).records[-1]
    assert record.status == FAILED
    assert "SchemaContractError" in record.error_message
    assert _delta(spark, t.bronze_loans).where("_batch_id = 'batch_006_contract'").count() == 0
    assert _signature(spark, env) == before


def test_new_source_column_is_absorbed(spark, processed):
    env = processed
    t = tables(env)
    columns = HEADER + ["new_source_field"]
    row = loan(80_001, issue_d="Feb-2018", new_source_field="x")
    write_csv([row], env.landing_dir / "batch_005_2018" / "part-00000.csv", columns)

    record = run_pipeline(spark, env).records[-1]
    assert record.status == SUCCESS

    bronze = _delta(spark, t.bronze_loans)
    assert "new_source_field" in bronze.columns
    assert bronze.where("_batch_id = 'batch_005_2018'").first().new_source_field == "x"
    assert bronze.where("_batch_id = 'batch_001_2015'").first().new_source_field is None
    assert _silver(spark, env).columns == SILVER_COLUMNS
    assert _silver(spark, env).where("loan_id = 80001").count() == 1


def test_reports_and_demo(spark, processed):
    env = processed
    demo = demo_reprocess(spark, env, "batch_002_2016")
    assert "nenhuma duplicidade e nenhum dado alterado" in demo
    assert "Indicadores da carteira" in kpi_report(spark, env)
    assert "batch_004_updates" in status_table(spark, env)
