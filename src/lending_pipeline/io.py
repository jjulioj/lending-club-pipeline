"""Leitura de arquivos de origem."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def resolve_raw_file(configured: Path) -> Path:
    """Aceita o arquivo configurado ou a variante .csv/.csv.gz do mesmo nome.

    O download do Kaggle traz o arquivo compactado e uma pasta com o .csv
    descompactado; qualquer um dos dois serve.
    """
    if configured.exists():
        return configured
    name = configured.name.removesuffix(".gz").removesuffix(".csv")
    candidates = sorted(configured.parent.glob(f"{name}.csv*")) + sorted(configured.parent.glob(f"*/{name}.csv*"))
    if not candidates:
        raise FileNotFoundError(
            f"Arquivo bruto não encontrado em {configured}. Baixe 'accepted_2007_to_2018Q4.csv.gz' "
            "do Kaggle (wordsforthewise/lending-club) e coloque em data/raw/."
        )
    return candidates[0]


def read_raw_csv(spark: SparkSession, path: Path | str, multiline: bool = True) -> DataFrame:
    """Lê o CSV de origem com todas as colunas como string.

    Tipagem não acontece aqui de propósito: a bronze guarda o dado como chegou,
    e a conversão de tipos é feita na silver, onde falhas viram quarentena em
    vez de derrubar a carga.
    """
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", multiline)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(str(path))
    )
