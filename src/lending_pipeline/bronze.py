"""Camada bronze: dado como chegou, tudo string, com metadados de carga.

Particionada por `_batch_id`. Cada carga substitui atomicamente a própria
partição (replaceWhere), então reprocessar a mesma carga não duplica nada.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .io import read_raw_csv
from .lake import overwrite_partition
from .quality import check_required_columns


def read_landing_batch(spark: SparkSession, path: Path, multiline: bool) -> DataFrame:
    df = read_raw_csv(spark, path, multiline=multiline)
    # Contrato com a origem: falha antes de qualquer escrita se faltar coluna obrigatória
    check_required_columns(df.columns)
    return df


def ingest_batch(
    spark: SparkSession,
    bronze_path: str,
    landing_path: Path,
    batch_id: str,
    batch_seq: int,
    run_id: str,
    ingested_at: datetime,
    multiline: bool = True,
) -> int:
    source = read_landing_batch(spark, landing_path, multiline)
    source_cols = [c for c in source.columns if not c.startswith("_")]
    df = source.select(
        *[F.col(f"`{c}`") for c in source_cols],
        F.lit(batch_id).alias("_batch_id"),
        F.lit(batch_seq).cast("int").alias("_batch_seq"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
        F.lit(run_id).alias("_run_id"),
    )
    overwrite_partition(df, bronze_path, "_batch_id", batch_id)
    return read_bronze_batch(spark, bronze_path, batch_id).count()


def read_bronze_batch(spark: SparkSession, bronze_path: str, batch_id: str) -> DataFrame:
    return spark.read.format("delta").load(bronze_path).where(F.col("_batch_id") == batch_id)
