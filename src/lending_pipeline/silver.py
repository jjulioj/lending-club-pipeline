"""Camada silver: estado atual de cada empréstimo, tipado e validado.

Fluxo por carga:
  bronze da carga -> tipagem -> regras de qualidade -> quarentena (erros)
  -> deduplicação dentro da carga -> MERGE na silver.

Regra do MERGE (chave `loan_id`):
  - empréstimo novo: insere;
  - empréstimo existente: atualiza só se o conteúdo mudou (hash diferente)
    E a carga é igual ou mais nova que a que gerou a versão atual.
Consequências:
  - reprocessar a mesma carga não altera nada (idempotência);
  - reprocessar uma carga antiga não sobrescreve dados mais novos;
  - uma carga reentregue corrigida atualiza só o que ainda "pertence" a ela.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from .lake import current_version, delta_exists, last_operation_metrics, overwrite_partition
from .quality import apply_row_rules, split_valid_quarantine
from .schema import SOURCE_COLUMNS
from .transforms import BUSINESS_COLUMNS, deduplicate_versions, ensure_source_columns, standardize, with_row_hash

SILVER_TECHNICAL_COLUMNS = [
    "_dq_warnings",
    "_row_hash",
    "_batch_id",
    "_batch_seq",
    "_source_file",
    "_run_id",
    "_first_loaded_at",
    "_last_updated_at",
]
SILVER_COLUMNS = BUSINESS_COLUMNS + SILVER_TECHNICAL_COLUMNS

MERGE_UPDATE_CONDITION = "s._row_hash <> t._row_hash AND s._batch_seq >= t._batch_seq"


@dataclass
class SilverBatch:
    checked: DataFrame  # todas as linhas, tipadas, com _dq_errors/_dq_warnings
    valid_deduped: DataFrame  # pronto para o MERGE
    quarantine: DataFrame  # pronto para a tabela de quarentena


def prepare_batch(bronze_batch: DataFrame, processed_at: datetime) -> SilverBatch:
    """Transformações da carga, sem escrever nada (testável sem Delta)."""
    raw = ensure_source_columns(bronze_batch)
    # Guarda o valor original de cada coluna para investigar a quarentena
    raw = raw.withColumn("_raw", F.struct(*[F.col(f"`{c}`") for c in SOURCE_COLUMNS]))
    checked = apply_row_rules(with_row_hash(standardize(raw)))
    valid, quarantine = split_valid_quarantine(checked)

    ts = F.lit(processed_at).cast("timestamp")
    valid_deduped = deduplicate_versions(valid).select(
        *BUSINESS_COLUMNS,
        "_dq_warnings",
        "_row_hash",
        "_batch_id",
        "_batch_seq",
        "_source_file",
        "_run_id",
        ts.alias("_first_loaded_at"),
        ts.alias("_last_updated_at"),
    )
    quarantine_out = quarantine.select(
        "source_id",
        "loan_id",
        "_dq_errors",
        "_dq_warnings",
        "_raw",
        "_batch_id",
        "_batch_seq",
        "_source_file",
        "_run_id",
        ts.alias("_quarantined_at"),
    )
    return SilverBatch(checked=checked, valid_deduped=valid_deduped, quarantine=quarantine_out)


def _nullable(schema: StructType) -> StructType:
    # Evita que uma coluna criada como NOT NULL (ex.: vinda de um literal) rejeite nulos no futuro
    return StructType([StructField(f.name, f.dataType, True) for f in schema.fields])


def ensure_silver_table(spark: SparkSession, path: str, schema: StructType) -> None:
    from delta.tables import DeltaTable

    # Só cria na primeira carga: recriar sobre tabela existente poderia conflitar com propriedades/schema
    if delta_exists(spark, path):
        return
    (
        DeltaTable.createIfNotExists(spark)
        .location(path)
        .addColumns(_nullable(schema))
        # Change Data Feed: permite que a gold (ou outro consumidor) leia só o que mudou
        .property("delta.enableChangeDataFeed", "true")
        .execute()
    )


def merge_into_silver(spark: SparkSession, path: str, source: DataFrame) -> dict[str, int]:
    """MERGE idempotente da carga na silver. Retorna inseridos/atualizados/versão."""
    from delta.tables import DeltaTable

    ensure_silver_table(spark, path, source.schema)
    version_before = current_version(spark, path)
    update_set = {c: f"s.`{c}`" for c in SILVER_COLUMNS if c != "_first_loaded_at"}
    insert_values = {c: f"s.`{c}`" for c in SILVER_COLUMNS}
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), "t.loan_id = s.loan_id")
        .whenMatchedUpdate(condition=MERGE_UPDATE_CONDITION, set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    metrics = last_operation_metrics(spark, path, version_before)
    return {
        "inserted": metrics.get("numTargetRowsInserted", 0),
        "updated": metrics.get("numTargetRowsUpdated", 0),
        "version_before": version_before,
        "version_after": current_version(spark, path),
    }


def write_quarantine(quarantine: DataFrame, path: str, batch_id: str) -> None:
    # Substitui a quarentena da carga: reprocessar não acumula linhas rejeitadas repetidas
    overwrite_partition(quarantine, path, "_batch_id", batch_id)


def restore_silver(spark: SparkSession, path: str, version: int | None) -> None:
    """Desfaz o MERGE de uma carga que falhou na validação pós-carga (time travel do Delta)."""
    from delta.tables import DeltaTable

    if version is None or not delta_exists(spark, path):
        return
    current = current_version(spark, path)
    if current is not None and current > version:
        DeltaTable.forPath(spark, path).restoreToVersion(version)


def cache(df: DataFrame) -> DataFrame:
    return df.persist(StorageLevel.MEMORY_AND_DISK)
