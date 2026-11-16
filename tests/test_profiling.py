"""O profiling roda de ponta a ponta e aponta as armadilhas do arquivo."""

from __future__ import annotations

from tests.factory import loan, portfolio, write_csv

from lending_pipeline.profiling import run_profile


def test_profile_report(spark, config, tmp_path):
    rows = portfolio() + [loan(1001)]  # 1001 já existe na carteira: id repetido
    raw = write_csv(rows, tmp_path / "accepted_2007_to_2018Q4.csv")
    report = run_profile(spark, config, raw_file=raw, output_file=tmp_path / "report.md").read_text()

    assert f"Linhas: **{len(rows):,}**" in report
    assert "| `id` não numérico | 2 |" in report
    assert "| `id` repetidos (ids com mais de 1 linha) | 1 |" in report
    assert "Total amount funded in policy code 1" in report
