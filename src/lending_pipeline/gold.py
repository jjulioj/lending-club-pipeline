"""Camada gold: indicadores da carteira.

1. `loan_portfolio_cube`: agregado no grão (safra, trimestre, UF, grade,
   finalidade) guardando só medidas ADITIVAS (contagens, somas e os
   numeradores/denominadores das taxas). Taxas não são somáveis: média de
   médias dá resultado errado. Guardando os componentes, qualquer recorte
   calcula a taxa certa.
2. As consultas em `sql/gold/*.sql` leem o cubo e geram as tabelas de KPI
   (por período, UF, grade, finalidade...). São as "consultas utilizadas"
   da camada analítica, versionadas como SQL puro.

A gold é reconstruída inteira a partir da silver a cada execução: é pequena,
barata e fica sempre consistente com a silver, inclusive depois de
reprocessamentos. Em volume muito maior, passaria a ser incremental lendo o
Change Data Feed da silver (ver README, Parte D).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import PROJECT_ROOT

log = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql" / "gold"
CUBE_VIEW = "loan_portfolio_cube"
CUBE_DIMENSIONS = ["issue_year", "issue_quarter", "addr_state", "grade", "purpose"]


def build_cube(silver: DataFrame, built_at: datetime | None = None) -> DataFrame:
    is_default = F.coalesce(F.col("is_default"), F.lit(False))
    is_resolved = F.coalesce(F.col("is_resolved"), F.lit(False))
    zero = F.lit(0).cast("decimal(18,2)")
    cube = silver.groupBy(*CUBE_DIMENSIONS).agg(
        F.count("*").alias("loan_count"),
        F.sum("loan_amnt").alias("originated_amount"),
        F.sum("funded_amnt").alias("funded_amount"),
        F.sum("funded_amnt_inv").alias("funded_amount_inv"),
        F.sum("out_prncp").alias("outstanding_principal"),
        # componentes das taxas de juros (simples e ponderada pelo valor financiado)
        F.sum("int_rate").alias("sum_int_rate"),
        F.sum(F.col("int_rate") * F.col("funded_amnt")).alias("sum_int_rate_x_funded"),
        # default (definição em schema.py)
        F.sum(F.when(is_default, 1).otherwise(0)).alias("default_count"),
        F.sum(F.when(is_default, F.col("funded_amnt")).otherwise(zero)).alias("default_funded_amount"),
        F.sum(F.when(is_default, F.col("funded_amnt") - F.col("total_rec_prncp")).otherwise(zero)).alias(
            "default_principal_loss"
        ),
        F.sum(F.when(is_default, F.col("recoveries")).otherwise(zero)).alias("default_recoveries"),
        # denominadores "sobre encerrados"
        F.sum(F.when(is_resolved, 1).otherwise(0)).alias("resolved_count"),
        F.sum(F.when(is_resolved, F.col("funded_amnt")).otherwise(zero)).alias("resolved_funded_amount"),
    )
    return cube.withColumn("_built_at", F.lit(built_at or datetime.now(UTC)).cast("timestamp"))


def kpi_queries(sql_dir: Path = SQL_DIR) -> dict[str, str]:
    """Nome da tabela -> SQL. Cada arquivo sql/gold/<nome>.sql vira a tabela gold/<nome>."""
    return {p.stem: p.read_text(encoding="utf-8") for p in sorted(sql_dir.glob("*.sql"))}


def run_kpi_queries(spark: SparkSession, cube: DataFrame, sql_dir: Path = SQL_DIR) -> dict[str, DataFrame]:
    cube.createOrReplaceTempView(CUBE_VIEW)
    return {name: spark.sql(query) for name, query in kpi_queries(sql_dir).items()}


def build_gold(spark: SparkSession, silver_path: str, gold_table_path, built_at: datetime) -> dict[str, int]:
    """Reconstrói o cubo e as tabelas de KPI. `gold_table_path(name)` devolve o caminho de cada tabela."""
    silver = spark.read.format("delta").load(silver_path)
    cube = build_cube(silver, built_at)
    cube_path = gold_table_path(CUBE_VIEW)
    cube.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cube_path)

    # As KPIs leem o cubo já gravado (e não o plano a partir da silver de novo)
    stored_cube = spark.read.format("delta").load(cube_path)
    counts = {CUBE_VIEW: stored_cube.count()}
    for name, df in run_kpi_queries(spark, stored_cube).items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_table_path(name))
        counts[name] = spark.read.format("delta").load(gold_table_path(name)).count()
        log.info("gold.%s: %d linhas", name, counts[name])
    return counts
