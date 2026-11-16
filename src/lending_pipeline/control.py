"""Tabela de controle de cargas (ops.load_control).

Log append-only: cada tentativa de processar uma carga grava uma linha com
status (SUCCESS, FAILED ou SKIPPED), checksum do arquivo, contagens e erro.
O status vigente de uma carga é o da tentativa mais recente.

Uma carga só é registrada ao final da tentativa. Se o processo morrer no
meio, não há registro de sucesso e a carga é reprocessada na próxima
execução, o que é seguro porque todas as escritas são idempotentes.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

SUCCESS, FAILED, SKIPPED = "SUCCESS", "FAILED", "SKIPPED"

CONTROL_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("batch_id", StringType()),
        StructField("batch_seq", IntegerType()),
        StructField("source_path", StringType()),
        StructField("source_checksum", StringType()),
        StructField("status", StringType()),
        StructField("forced", StringType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
        StructField("rows_read", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("rows_valid", LongType()),
        StructField("rows_duplicates_removed", LongType()),
        StructField("rows_with_warnings", LongType()),
        StructField("rows_inserted", LongType()),
        StructField("rows_updated", LongType()),
        StructField("rows_unchanged", LongType()),
        StructField("silver_version", LongType()),
        StructField("error_message", StringType()),
    ]
)


@dataclass
class LoadRecord:
    run_id: str
    batch_id: str
    batch_seq: int
    source_path: str
    source_checksum: str
    status: str
    forced: str
    started_at: datetime
    finished_at: datetime | None = None
    rows_read: int | None = None
    rows_quarantined: int | None = None
    rows_valid: int | None = None
    rows_duplicates_removed: int | None = None
    rows_with_warnings: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_unchanged: int | None = None
    silver_version: int | None = None
    error_message: str | None = field(default=None)


def append_record(spark: SparkSession, path: str, record: LoadRecord) -> None:
    values = asdict(record)
    row = tuple(values[f.name] for f in CONTROL_SCHEMA.fields)
    spark.createDataFrame([row], CONTROL_SCHEMA).write.format("delta").mode("append").save(path)


def latest_by_batch(spark: SparkSession, path: str) -> dict[str, dict]:
    """Última tentativa registrada de cada carga."""
    from .lake import delta_exists

    if not delta_exists(spark, path):
        return {}
    w = Window.partitionBy("batch_id").orderBy(F.col("started_at").desc(), F.col("finished_at").desc())
    rows = (
        spark.read.format("delta")
        .load(path)
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .collect()
    )
    return {r["batch_id"]: r.asDict() for r in rows}


def last_success_by_batch(spark: SparkSession, path: str) -> dict[str, dict]:
    """Último processamento bem-sucedido de cada carga (ignora SKIPPED e FAILED)."""
    from .lake import delta_exists

    if not delta_exists(spark, path):
        return {}
    w = Window.partitionBy("batch_id").orderBy(F.col("started_at").desc())
    rows = (
        spark.read.format("delta")
        .load(path)
        .where(F.col("status") == SUCCESS)
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .collect()
    )
    return {r["batch_id"]: r.asDict() for r in rows}


def directory_checksum(path: Path) -> str:
    """SHA-256 do conteúdo dos arquivos de dados de uma carga (ignora _SUCCESS e .crc).

    Detecta reentrega de arquivo corrigido: mesmo nome de carga, conteúdo
    diferente. Só o conteúdo conta (o Spark gera nomes de arquivo com UUID
    aleatório), e a combinação independe da ordem dos arquivos. Em nuvem, o
    equivalente barato seria o ETag/MD5 do objeto.
    """
    file_digests = []
    for file in path.rglob("*"):
        if not file.is_file() or file.name.startswith(("_", ".")):
            continue
        digest = hashlib.sha256()
        with file.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        file_digests.append(digest.hexdigest())
    return hashlib.sha256("".join(sorted(file_digests)).encode()).hexdigest()
