"""Testes de fumaça: ambiente, configuração e Delta funcionando."""

from __future__ import annotations

import pytest


def test_config_points_to_temp_dirs(config):
    assert "lake" in str(config.lake_dir)
    assert config.spark_shuffle_partitions == 2


def test_spark_runs_in_utc_with_ansi(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    # Spark 4 liga ANSI por padrão: cast inválido gera erro, por isso o código usa try_cast
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"


@pytest.mark.delta
def test_delta_write_and_read(spark, config):
    path = str(config.lake_dir / "_smoke")
    spark.range(5).write.format("delta").mode("overwrite").save(path)
    assert spark.read.format("delta").load(path).count() == 5
