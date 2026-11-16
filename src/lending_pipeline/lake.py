"""Localização das tabelas do lakehouse e helpers de escrita Delta.

Localmente as tabelas são caminhos (data/lake/<camada>/<tabela>). No
Databricks, o equivalente seria um catálogo Unity Catalog com um schema por
camada; só este módulo precisaria mudar.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .config import Config


@dataclass(frozen=True)
class Tables:
    bronze_loans: str
    silver_loans: str
    silver_quarantine: str
    ops_load_control: str
    gold_dir: str

    def gold(self, name: str) -> str:
        return f"{self.gold_dir}/{name}"


def tables(config: Config) -> Tables:
    root = config.lake_dir
    return Tables(
        bronze_loans=str(root / "bronze" / "loans_raw"),
        silver_loans=str(root / "silver" / "loans"),
        silver_quarantine=str(root / "silver" / "loans_quarantine"),
        ops_load_control=str(root / "ops" / "load_control"),
        gold_dir=str(root / "gold"),
    )


def delta_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def current_version(spark: SparkSession, path: str) -> int | None:
    """Versão atual da tabela Delta (None se ela ainda não existe)."""
    from delta.tables import DeltaTable

    if not delta_exists(spark, path):
        return None
    return DeltaTable.forPath(spark, path).history(1).select("version").first()[0]


def last_operation_metrics(spark: SparkSession, path: str, version_before: int | None) -> dict[str, int]:
    """Métricas da última operação, só se ela gerou uma versão nova.

    Se a operação não gerou commit (nada mudou), devolve vazio em vez de
    reaproveitar por engano as métricas da operação anterior.
    """
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, path).history(1).select("version", "operationMetrics").first()
    if version_before is not None and row["version"] <= version_before:
        return {}
    return {k: int(v) for k, v in (row["operationMetrics"] or {}).items() if str(v).lstrip("-").isdigit()}


def overwrite_partition(df: DataFrame, path: str, partition_col: str, value: str) -> None:
    """Substitui atomicamente uma partição inteira (ex.: uma carga) numa tabela Delta.

    Reexecutar com os mesmos dados produz o mesmo resultado: é a base da
    idempotência da bronze e da quarentena. `mergeSchema` aceita colunas novas
    vindas da origem (evolução de schema) sem quebrar a carga.
    """
    spark = df.sparkSession
    writer = df.write.format("delta").option("mergeSchema", "true")
    if not delta_exists(spark, path):
        writer.partitionBy(partition_col).mode("append").save(path)
        return
    safe_value = value.replace("'", "''")
    writer.mode("overwrite").option("replaceWhere", f"{partition_col} = '{safe_value}'").save(path)
