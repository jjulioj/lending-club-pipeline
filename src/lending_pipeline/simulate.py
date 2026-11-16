"""Simulação de cargas incrementais a partir do snapshot único do Kaggle.

O dataset é um retrato único (estado de cada empréstimo no fim de 2018), mas
o case pede cargas periódicas com registros novos E atualizações. Estratégia
(decisão documentada no README):

1. Cada safra (ano de `issue_d`) vira uma carga com os empréstimos originados
   naquele ano.
2. Para uma fração dos empréstimos cujo status mudou desde a originação
   (qualquer status diferente de Current), a carga da safra traz uma
   "versão inicial" (Current, saldo cheio, nada pago) e a versão real chega
   na carga seguinte, como atualização. A última safra recebe uma carga extra
   só de atualizações.
3. Para exercitar a deduplicação, algumas cargas trazem duas versões do mesmo
   empréstimo (conflito dentro da carga) e linhas 100% duplicadas.
4. Linhas sem `issue_d` válido (ex.: rodapés do arquivo) são distribuídas
   entre as cargas, como chegariam na vida real; o pipeline as põe em quarentena.

Propriedade importante: depois de processar todas as cargas, o estado final
da silver deve ser igual ao snapshot original. Isso é verificado em teste.
A amostragem usa hash do id (não rand()), então a simulação é determinística.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import CREDIT_POLICY_PREFIX, SOURCE_DATE_FORMAT

log = logging.getLogger(__name__)

BATCH_DIR_PREFIX = "batch_"
CSV_WRITE_OPTIONS = {"header": "true", "quote": '"', "escape": '"'}

# Colunas zeradas na "versão inicial" (estado logo após a originação)
_ZEROED_ON_ORIGINATION = [
    "total_pymnt",
    "total_pymnt_inv",
    "total_rec_prncp",
    "total_rec_int",
    "total_rec_late_fee",
    "recoveries",
    "collection_recovery_fee",
    "last_pymnt_amnt",
]
_NULLED_ON_ORIGINATION = ["last_pymnt_d", "next_pymnt_d"]


@dataclass(frozen=True)
class SimulationParams:
    update_fraction: float = 0.30  # fração dos empréstimos com status alterado que chegam como update
    conflict_fraction: float = 0.002  # fração com as duas versões na mesma carga
    duplicate_fraction: float = 0.001  # fração de linhas repetidas de forma idêntica
    seed: int = 42


def _bucket(*cols: Column, seed: int) -> Column:
    """Número determinístico em [0, 1) a partir do hash das colunas."""
    return F.pmod(F.xxhash64(*cols, F.lit(seed)), F.lit(1_000_000)) / F.lit(1_000_000.0)


def _initial_version(df: DataFrame) -> DataFrame:
    """Reconstrói como o empréstimo apareceria logo após a originação."""
    replacements: dict[str, Column] = {"loan_status": F.lit("Current")}
    if "out_prncp" in df.columns and "funded_amnt" in df.columns:
        replacements["out_prncp"] = F.col("funded_amnt")
    if "out_prncp_inv" in df.columns and "funded_amnt_inv" in df.columns:
        replacements["out_prncp_inv"] = F.col("funded_amnt_inv")
    for c in _ZEROED_ON_ORIGINATION:
        if c in df.columns:
            replacements[c] = F.lit("0.0")
    for c in _NULLED_ON_ORIGINATION:
        if c in df.columns:
            replacements[c] = F.lit(None).cast("string")
    if "last_credit_pull_d" in df.columns:
        # consulta de crédito mais antiga que a da versão real: a versão real vence na deduplicação
        replacements["last_credit_pull_d"] = F.col("issue_d")
    return df.withColumns(replacements)


def plan_batches(raw: DataFrame, params: SimulationParams) -> DataFrame:
    """Atribui cada linha (e cada versão gerada) a uma carga.

    Retorna as colunas originais + `_batch_id` e `_kind`
    (new | initial | update | conflict_final | duplicate).
    """
    source_cols = raw.columns
    issue_year = F.year(F.try_to_timestamp(F.trim(F.col("issue_d")), F.lit(SOURCE_DATE_FORMAT)))
    base = raw.withColumn("_issue_year", issue_year)

    years = [
        r[0]
        for r in base.where("_issue_year IS NOT NULL").select("_issue_year").distinct().orderBy("_issue_year").collect()
    ]
    if not years:
        raise ValueError("Nenhum issue_d válido no arquivo: impossível montar as safras")
    first_year, n_years = years[0], years[-1] - years[0] + 1
    log.info("Safras de %s a %s (%d cargas + 1 de atualizações)", years[0], years[-1], n_years)

    status = F.trim(F.col("loan_status"))
    evolved = F.coalesce((status != "Current") & ~status.startswith(CREDIT_POLICY_PREFIX), F.lit(False))
    pick = _bucket(F.col("id"), seed=params.seed)

    base = (
        base.withColumn(
            "_vintage_seq",
            F.when(F.col("_issue_year").isNotNull(), F.col("_issue_year") - F.lit(first_year) + 1)
            # sem safra válida (rodapés etc.): espalha entre as cargas
            .otherwise(F.pmod(F.xxhash64(*[F.col(c) for c in source_cols]), F.lit(n_years)) + 1),
        )
        .withColumn("_selected", evolved & (pick < params.update_fraction))
        .withColumn("_conflict", F.col("_selected") & (pick < params.conflict_fraction))
    )

    real_rows = base.withColumns(
        {
            "_batch_seq": F.when(F.col("_selected") & ~F.col("_conflict"), F.col("_vintage_seq") + 1).otherwise(
                F.col("_vintage_seq")
            ),
            "_kind": F.when(F.col("_conflict"), "conflict_final").when(F.col("_selected"), "update").otherwise("new"),
        }
    )
    initial_rows = _initial_version(base.where("_selected")).withColumns(
        {"_batch_seq": F.col("_vintage_seq"), "_kind": F.lit("initial")}
    )
    planned = real_rows.unionByName(initial_rows)

    duplicates = planned.where(
        _bucket(F.col("id"), F.col("_kind"), seed=params.seed + 1) < params.duplicate_fraction
    ).withColumn("_kind", F.lit("duplicate"))
    planned = planned.unionByName(duplicates)

    label = F.when(F.col("_batch_seq") > n_years, F.lit("updates")).otherwise(
        (F.col("_batch_seq") + F.lit(first_year - 1)).cast("string")
    )
    return planned.withColumn(
        "_batch_id", F.format_string(f"{BATCH_DIR_PREFIX}%03d_%s", F.col("_batch_seq"), label)
    ).select(*source_cols, "_batch_id", "_kind")


def write_batches(planned: DataFrame, landing_dir: Path) -> list[dict]:
    """Escreve um diretório CSV por carga em `landing_dir`. Retorna o resumo por carga."""
    planned = planned.persist(StorageLevel.MEMORY_AND_DISK)
    summary_rows = (
        planned.groupBy("_batch_id")
        .pivot("_kind", ["new", "initial", "update", "conflict_final", "duplicate"])
        .count()
        .na.fill(0)
        .orderBy("_batch_id")
        .collect()
    )
    landing_dir.mkdir(parents=True, exist_ok=True)
    for old in landing_dir.glob(f"{BATCH_DIR_PREFIX}*"):
        shutil.rmtree(old)

    source_cols = [c for c in planned.columns if not c.startswith("_")]
    summary = []
    for row in summary_rows:
        batch_id = row["_batch_id"]
        target = landing_dir / batch_id
        (
            planned.where(F.col("_batch_id") == batch_id)
            .select(*source_cols)
            .coalesce(1)
            .write.mode("overwrite")
            .options(**CSV_WRITE_OPTIONS)
            .csv(str(target))
        )
        info = {k: row[k] for k in ["new", "initial", "update", "conflict_final", "duplicate"]}
        info["batch_id"] = batch_id
        info["rows"] = sum(row[k] for k in ["new", "initial", "update", "conflict_final", "duplicate"])
        summary.append(info)
        log.info("Carga %s: %d linhas", batch_id, info["rows"])
    planned.unpersist()
    return summary


def summary_markdown(summary: list[dict], params: SimulationParams) -> str:
    header = "| Carga | Linhas | Novos | Versão inicial | Atualizações | Conflito na carga | Duplicatas exatas |"
    lines = [
        "# Cargas simuladas",
        "",
        f"Parâmetros: update_fraction={params.update_fraction}, conflict_fraction={params.conflict_fraction}, "
        f"duplicate_fraction={params.duplicate_fraction}, seed={params.seed}.",
        "",
        "- **Novos**: empréstimo chega uma vez, já no estado final.",
        "- **Versão inicial**: empréstimo chega como Current, sem pagamentos; a versão real vem depois.",
        "- **Atualizações**: versão real de empréstimos que chegaram como versão inicial na carga anterior.",
        "- **Conflito na carga**: versão real na mesma carga da versão inicial (a deduplicação escolhe a real).",
        "",
        header,
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for s in summary:
        lines.append(
            f"| `{s['batch_id']}` | {s['rows']:,} | {s['new']:,} | {s['initial']:,} | {s['update']:,} | "
            f"{s['conflict_final']:,} | {s['duplicate']:,} |"
        )
    total = {k: sum(s[k] for s in summary) for k in ["rows", "new", "initial", "update", "conflict_final", "duplicate"]}
    lines.append(
        f"| **Total** | {total['rows']:,} | {total['new']:,} | {total['initial']:,} | {total['update']:,} | "
        f"{total['conflict_final']:,} | {total['duplicate']:,} |"
    )
    return "\n".join(lines) + "\n"
