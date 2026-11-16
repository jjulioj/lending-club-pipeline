"""Criação da SparkSession.

Localmente (Docker/CI) a sessão é criada com Delta Lake configurado via pip.
No Databricks já existe uma sessão ativa com Delta, então ela é reaproveitada:
o mesmo código roda nos dois ambientes sem alteração.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from .config import Config


def get_spark(config: Config, app_name: str = "lending-pipeline", with_delta: bool = True) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.spark_master)
        .config("spark.driver.memory", config.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions))
        # Datas de processamento sempre em UTC, independente da máquina
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if with_delta:
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # No Docker os jars do Delta são baixados no build para este diretório
        ivy_dir = os.environ.get("SPARK_JARS_IVY")
        if ivy_dir:
            builder = builder.config("spark.jars.ivy", ivy_dir)
        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
