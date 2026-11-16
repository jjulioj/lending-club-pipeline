"""Orquestração: descobre as cargas em landing e processa bronze -> silver -> gold.

Cargas são processadas em ordem de sequência. Uma carga é ignorada quando já
foi processada com sucesso com o mesmo conteúdo (mesmo checksum); `force`
reprocessa mesmo assim. Uma carga reentregue com conteúdo diferente é
reprocessada automaticamente.
"""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .bronze import ingest_batch, read_bronze_batch
from .config import Config
from .control import (
    FAILED,
    SKIPPED,
    SUCCESS,
    LoadRecord,
    append_record,
    directory_checksum,
    last_success_by_batch,
)
from .gold import CUBE_VIEW, build_gold
from .lake import Tables, delta_exists, tables
from .quality import check_quarantine_rate, check_unique_key
from .silver import cache, merge_into_silver, prepare_batch, restore_silver, write_quarantine

log = logging.getLogger(__name__)

BATCH_PATTERN = re.compile(r"^batch_(\d{3})_[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class LandingBatch:
    batch_id: str
    seq: int
    path: Path


@dataclass
class RunSummary:
    records: list[LoadRecord] = field(default_factory=list)
    gold_counts: dict[str, int] | None = None

    @property
    def failed(self) -> bool:
        return any(r.status == FAILED for r in self.records)


def _now() -> datetime:
    return datetime.now(UTC)


def discover_batches(landing_dir: Path) -> list[LandingBatch]:
    """Cargas disponíveis: diretórios `batch_NNN_<rótulo>`. A sequência NNN define a ordem."""
    if not landing_dir.exists():
        return []
    batches = []
    for path in landing_dir.iterdir():
        match = BATCH_PATTERN.match(path.name)
        if path.is_dir() and match:
            batches.append(LandingBatch(batch_id=path.name, seq=int(match.group(1)), path=path))
    return sorted(batches, key=lambda b: b.seq)


def process_batch(
    spark: SparkSession, config: Config, t: Tables, batch: LandingBatch, checksum: str, forced: bool
) -> LoadRecord:
    started = _now()
    record = LoadRecord(
        run_id=str(uuid.uuid4()),
        batch_id=batch.batch_id,
        batch_seq=batch.seq,
        source_path=str(batch.path),
        source_checksum=checksum,
        status=FAILED,
        forced=str(forced).lower(),
        started_at=started,
    )
    merged, version_before, cached = False, None, []
    try:
        log.info("[%s] bronze", batch.batch_id)
        record.rows_read = ingest_batch(
            spark, t.bronze_loans, batch.path, batch.batch_id, batch.seq, record.run_id, started, config.csv_multiline
        )

        log.info("[%s] silver: tipagem e qualidade", batch.batch_id)
        prepared = prepare_batch(read_bronze_batch(spark, t.bronze_loans, batch.batch_id), started)
        cached.append(cache(prepared.checked))
        stats = prepared.checked.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.size("_dq_errors") > 0, 1).otherwise(0)).alias("quarantined"),
            F.sum(F.when((F.size("_dq_errors") == 0) & (F.size("_dq_warnings") > 0), 1).otherwise(0)).alias("warned"),
        ).first()
        record.rows_quarantined = stats.quarantined or 0
        record.rows_valid = stats.total - record.rows_quarantined
        record.rows_with_warnings = stats.warned or 0

        # Quarentena gravada antes do portão de qualidade: se a carga falhar, dá para investigar
        write_quarantine(prepared.quarantine, t.silver_quarantine, batch.batch_id)
        check_quarantine_rate(stats.total, record.rows_quarantined, config.max_quarantine_rate)

        deduped = cache(prepared.valid_deduped)
        cached.append(deduped)
        rows_to_merge = deduped.count()
        record.rows_duplicates_removed = record.rows_valid - rows_to_merge

        log.info("[%s] silver: MERGE de %d linhas", batch.batch_id, rows_to_merge)
        result = merge_into_silver(spark, t.silver_loans, deduped)
        merged, version_before = True, result["version_before"]

        # Validação pós-carga: se a tabela final violar a chave, desfaz o MERGE
        check_unique_key(spark.read.format("delta").load(t.silver_loans))

        record.rows_inserted = result["inserted"]
        record.rows_updated = result["updated"]
        record.rows_unchanged = rows_to_merge - result["inserted"] - result["updated"]
        record.silver_version = result["version_after"]
        record.status = SUCCESS
        log.info(
            "[%s] OK: %d lidas, %d quarentena, %d inseridas, %d atualizadas, %d sem mudança",
            batch.batch_id,
            record.rows_read,
            record.rows_quarantined,
            record.rows_inserted,
            record.rows_updated,
            record.rows_unchanged,
        )
    except Exception as exc:  # noqa: BLE001 - toda falha precisa virar registro na tabela de controle
        message = f"{type(exc).__name__}: {exc}"
        if merged:
            restore_silver(spark, t.silver_loans, version_before)
            message += f" | silver restaurada para a versão {version_before}"
        record.status = FAILED
        record.error_message = message[:4000]
        log.error("[%s] FALHOU: %s", batch.batch_id, message)
    finally:
        record.finished_at = _now()
        append_record(spark, t.ops_load_control, record)
        for df in cached:
            df.unpersist()
    return record


def run_pipeline(
    spark: SparkSession,
    config: Config,
    only_batches: list[str] | None = None,
    force: bool = False,
    build_gold_after: bool = True,
) -> RunSummary:
    t = tables(config)
    batches = discover_batches(config.landing_dir)
    if not batches:
        raise FileNotFoundError(f"Nenhuma carga em {config.landing_dir}. Rode o comando `simulate` antes.")
    if only_batches:
        unknown = set(only_batches) - {b.batch_id for b in batches}
        if unknown:
            raise ValueError(f"Cargas não encontradas em landing: {', '.join(sorted(unknown))}")
        batches = [b for b in batches if b.batch_id in only_batches]

    summary = RunSummary()
    last_success = last_success_by_batch(spark, t.ops_load_control)
    processed_any = False

    for batch in batches:
        checksum = directory_checksum(batch.path)
        previous = last_success.get(batch.batch_id)
        if not force and previous and previous["source_checksum"] == checksum:
            now = _now()
            record = LoadRecord(
                run_id=str(uuid.uuid4()),
                batch_id=batch.batch_id,
                batch_seq=batch.seq,
                source_path=str(batch.path),
                source_checksum=checksum,
                status=SKIPPED,
                forced="false",
                started_at=now,
                finished_at=now,
                error_message=f"já processada com sucesso em {previous['finished_at']} com o mesmo conteúdo",
            )
            append_record(spark, t.ops_load_control, record)
            summary.records.append(record)
            log.info("[%s] ignorada: já processada com o mesmo conteúdo", batch.batch_id)
            continue

        if previous and previous["source_checksum"] != checksum:
            log.info("[%s] conteúdo diferente do último processamento: reprocessando", batch.batch_id)
        record = process_batch(spark, config, t, batch, checksum, forced=force)
        summary.records.append(record)
        if record.status == FAILED:
            log.error("Execução interrompida na carga %s. Corrija e rode de novo.", batch.batch_id)
            return summary
        processed_any = True

    if build_gold_after and (processed_any or not delta_exists(spark, t.gold(CUBE_VIEW))):
        log.info("gold: reconstruindo indicadores")
        summary.gold_counts = build_gold(spark, t.silver_loans, t.gold, _now())
    return summary
