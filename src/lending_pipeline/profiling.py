"""Profiling do arquivo bruto de empréstimos aceitos.

Objetivo: confirmar no dado real as hipóteses usadas no desenho do pipeline
(chave única, formatos, domínios e consistência entre valores) ANTES de
escrever as regras de limpeza. O resultado vai para docs/profiling_report.md
e serve de evidência para as decisões documentadas no README.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .io import read_raw_csv, resolve_raw_file

log = logging.getLogger(__name__)

# Colunas que o pipeline pretende usar. Se alguma não existir no arquivo, o
# relatório aponta: isso derrubaria uma suposição do desenho.
EXPECTED_COLUMNS = [
    "id",
    "member_id",
    "loan_amnt",
    "funded_amnt",
    "funded_amnt_inv",
    "term",
    "int_rate",
    "installment",
    "grade",
    "sub_grade",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "verification_status",
    "issue_d",
    "loan_status",
    "purpose",
    "addr_state",
    "dti",
    "revol_util",
    "out_prncp",
    "out_prncp_inv",
    "total_pymnt",
    "total_rec_prncp",
    "total_rec_int",
    "recoveries",
    "last_pymnt_d",
    "next_pymnt_d",
    "last_credit_pull_d",
    "policy_code",
    "application_type",
]

DISTRIBUTION_COLUMNS = [
    "loan_status",
    "term",
    "grade",
    "home_ownership",
    "verification_status",
    "purpose",
    "policy_code",
    "application_type",
    "emp_length",
]

FORMAT_SAMPLE_COLUMNS = ["int_rate", "revol_util", "term", "issue_d", "last_pymnt_d", "emp_length"]

DATE_FORMAT = "MMM-yyyy"  # ex.: "Dec-2015"


def _blank(c: str) -> Column:
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def _num(c: str) -> Column:
    """String -> double tolerante a '%' e espaços. try_cast evita erro com ANSI ligado (Spark 4)."""
    return F.expr(f"try_cast(regexp_replace(trim(`{c}`), '%', '') as double)")


def _date(c: str) -> Column:
    return F.try_to_timestamp(F.trim(F.col(c)), F.lit(DATE_FORMAT)).cast("date")


def _count_if(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0))


def _md_table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join("" if v is None else str(v).replace("|", "\\|") for v in row) + " |")
    return "\n".join(out)


def _pct(part: int, total: int) -> str:
    return f"{(100.0 * part / total):.2f}%" if total else "n/a"


def run_profile(
    spark: SparkSession,
    config: Config,
    raw_file: Path | None = None,
    output_file: Path | None = None,
) -> Path:
    started = time.monotonic()
    source = resolve_raw_file(raw_file or config.raw_file)
    output_file = output_file or (config.reports_dir / "profiling_report.md")
    log.info("Profiling de %s", source)

    raw = read_raw_csv(spark, source, multiline=config.csv_multiline)
    all_columns = raw.columns
    present = [c for c in EXPECTED_COLUMNS if c in all_columns]
    missing = [c for c in EXPECTED_COLUMNS if c not in all_columns]

    # Hash da linha inteira (todas as colunas) para detectar duplicatas exatas
    # sem precisar de distinct() sobre ~150 colunas.
    df = (
        raw.select(*[F.col(c) for c in present], F.xxhash64(*[F.col(c) for c in all_columns]).alias("_row_hash"))
        .repartition(8)
        .cache()
    )
    total = df.count()
    log.info("%d linhas lidas, %d colunas no arquivo", total, len(all_columns))

    sections: list[str] = []
    sections.append(
        "\n".join(
            [
                "# Profiling do arquivo bruto",
                "",
                f"- Arquivo: `{source.name}`",
                f"- Gerado em: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
                f"- Spark: {spark.version}",
                f"- Linhas: **{total:,}**",
                f"- Colunas no arquivo: **{len(all_columns)}**",
                f"- Colunas esperadas ausentes: {', '.join(missing) if missing else 'nenhuma'}",
            ]
        )
    )

    # ---------------------------------------------------------------- chave
    id_numeric = F.expr("try_cast(trim(id) as bigint)")
    key_stats = df.agg(
        _count_if(_blank("id")).alias("id_blank"),
        _count_if(~_blank("id") & id_numeric.isNull()).alias("id_non_numeric"),
        F.countDistinct("id").alias("id_distinct"),
        (_count_if(_blank("member_id")) if "member_id" in present else F.lit(None)).alias("member_id_blank"),
        F.countDistinct("_row_hash").alias("distinct_rows"),
    ).first()

    dup_ids = (
        df.where(~_blank("id"))
        .groupBy("id")
        .agg(F.count("*").alias("n"), F.countDistinct("_row_hash").alias("versions"))
        .where("n > 1")
        .agg(
            F.count("*").alias("ids_duplicados"),
            F.coalesce(F.sum("n"), F.lit(0)).alias("linhas_envolvidas"),
            _count_if(F.col("versions") > 1).alias("ids_com_conteudo_diferente"),
        )
        .first()
    )

    non_numeric_rows = df.where(~_blank("id") & id_numeric.isNull())
    filled_cols = sum(F.when(~_blank(c), 1).otherwise(0) for c in present if c != "id")
    non_numeric_samples = (
        non_numeric_rows.select(F.substring("id", 1, 70).alias("id"), filled_cols.alias("cols_preenchidas"))
        .limit(5)
        .collect()
    )

    sections.append(
        "\n".join(
            [
                "## Chave do empréstimo",
                "",
                _md_table(
                    ["Métrica", "Valor"],
                    [
                        ["`id` nulo ou vazio", f"{key_stats.id_blank:,}"],
                        ["`id` não numérico", f"{key_stats.id_non_numeric:,}"],
                        ["`id` distintos", f"{key_stats.id_distinct:,}"],
                        ["`id` repetidos (ids com mais de 1 linha)", f"{dup_ids.ids_duplicados:,}"],
                        ["Linhas envolvidas em `id` repetido", f"{dup_ids.linhas_envolvidas:,}"],
                        ["`id` repetido com conteúdo diferente", f"{dup_ids.ids_com_conteudo_diferente:,}"],
                        ["Linhas 100% duplicadas (todas as colunas)", f"{total - key_stats.distinct_rows:,}"],
                        [
                            "`member_id` nulo ou vazio",
                            "coluna ausente"
                            if key_stats.member_id_blank is None
                            else f"{key_stats.member_id_blank:,} ({_pct(key_stats.member_id_blank, total)})",
                        ],
                    ],
                ),
                "",
                "Amostra de `id` não numérico (e quantas das colunas analisadas vêm preenchidas na linha):",
                "",
                _md_table(
                    ["id (70 primeiros caracteres)", "colunas preenchidas"],
                    [[r.id, r.cols_preenchidas] for r in non_numeric_samples],
                )
                if non_numeric_samples
                else "_nenhum_",
            ]
        )
    )

    # ---------------------------------------------------------------- nulos
    null_stats = df.agg(*[_count_if(_blank(c)).alias(c) for c in present]).first().asDict()
    sections.append(
        "\n".join(
            [
                "## Nulos ou vazios por coluna",
                "",
                _md_table(
                    ["Coluna", "Nulos/vazios", "%"],
                    [[f"`{c}`", f"{null_stats[c]:,}", _pct(null_stats[c], total)] for c in present],
                ),
            ]
        )
    )

    # ------------------------------------------------------------- formatos
    fmt_parts = ["## Formatos observados (valores mais frequentes)", ""]
    for c in [c for c in FORMAT_SAMPLE_COLUMNS if c in present]:
        top = df.where(~_blank(c)).groupBy(c).count().orderBy(F.desc("count")).limit(5).collect()
        fmt_parts.append(f"- `{c}`: " + ", ".join(f"`{r[c]}`" for r in top))
    sections.append("\n".join(fmt_parts))

    # ---------------------------------------------------------------- datas
    if "issue_d" in present:
        date_stats = df.agg(
            _count_if(~_blank("issue_d") & _date("issue_d").isNull()).alias("issue_unparsed"),
            F.min(_date("issue_d")).alias("issue_min"),
            F.max(_date("issue_d")).alias("issue_max"),
            (
                _count_if(~_blank("last_pymnt_d") & _date("last_pymnt_d").isNull())
                if "last_pymnt_d" in present
                else F.lit(None)
            ).alias("last_pymnt_unparsed"),
            (F.max(_date("last_pymnt_d")) if "last_pymnt_d" in present else F.lit(None)).alias("last_pymnt_max"),
        ).first()
        sections.append(
            "\n".join(
                [
                    f"## Datas (formato esperado `{DATE_FORMAT}`)",
                    "",
                    _md_table(
                        ["Métrica", "Valor"],
                        [
                            ["`issue_d` preenchido mas não parseável", f"{date_stats.issue_unparsed:,}"],
                            ["`issue_d` mínimo", date_stats.issue_min],
                            ["`issue_d` máximo", date_stats.issue_max],
                            ["`last_pymnt_d` preenchido mas não parseável", date_stats.last_pymnt_unparsed],
                            ["`last_pymnt_d` máximo", date_stats.last_pymnt_max],
                        ],
                    ),
                ]
            )
        )

    # ------------------------------------------------- consistência de valores
    checks: list[tuple[str, Column]] = []
    if {"loan_amnt", "funded_amnt", "funded_amnt_inv"} <= set(present):
        checks += [
            (
                "`loan_amnt` <= 0 ou não numérico",
                (_num("loan_amnt").isNull() | (_num("loan_amnt") <= 0)) & ~_blank("id"),
            ),
            ("`funded_amnt` > `loan_amnt`", _num("funded_amnt") > _num("loan_amnt") + 0.01),
            ("`funded_amnt` < `loan_amnt`", _num("funded_amnt") < _num("loan_amnt") - 0.01),
            ("`funded_amnt_inv` > `funded_amnt`", _num("funded_amnt_inv") > _num("funded_amnt") + 0.01),
            ("`funded_amnt_inv` < `funded_amnt`", _num("funded_amnt_inv") < _num("funded_amnt") - 0.01),
        ]
    if "int_rate" in present:
        checks.append(
            (
                "`int_rate` fora de [0, 40] ou não numérico",
                (_num("int_rate").isNull() | (_num("int_rate") < 0) | (_num("int_rate") > 40)) & ~_blank("id"),
            )
        )
    if {"out_prncp", "funded_amnt"} <= set(present):
        checks += [
            ("`out_prncp` < 0", _num("out_prncp") < 0),
            ("`out_prncp` > `funded_amnt`", _num("out_prncp") > _num("funded_amnt") + 0.01),
        ]
    if {"total_rec_prncp", "funded_amnt"} <= set(present):
        checks.append(("`total_rec_prncp` > `funded_amnt`", _num("total_rec_prncp") > _num("funded_amnt") + 0.01))
    if {"loan_status", "out_prncp"} <= set(present):
        status = F.trim(F.col("loan_status"))
        checks += [
            ("Charged Off com `out_prncp` > 0", status.endswith("Charged Off") & (_num("out_prncp") > 0)),
            ("Fully Paid com `out_prncp` > 0", status.endswith("Fully Paid") & (_num("out_prncp") > 0)),
            ("Current com `out_prncp` = 0", (status == "Current") & (_num("out_prncp") == 0)),
        ]
    if {"grade", "sub_grade"} <= set(present):
        checks.append(
            ("`sub_grade` não começa com `grade`", ~_blank("grade") & ~F.col("sub_grade").startswith(F.col("grade")))
        )
    if "annual_inc" in present:
        checks.append(("`annual_inc` <= 0", _num("annual_inc") <= 0))
    if "dti" in present:
        checks += [("`dti` < 0", _num("dti") < 0), ("`dti` > 100", _num("dti") > 100)]
    if "addr_state" in present:
        checks.append(
            ("`addr_state` com tamanho diferente de 2", ~_blank("addr_state") & (F.length(F.trim("addr_state")) != 2))
        )

    if checks:
        res = df.agg(*[_count_if(cond).alias(f"c{i}") for i, (_, cond) in enumerate(checks)]).first()
        sections.append(
            "\n".join(
                [
                    "## Consistência entre valores",
                    "",
                    _md_table(
                        ["Regra", "Linhas", "%"],
                        [
                            [label, f"{res[f'c{i}']:,}", _pct(res[f"c{i}"], total)]
                            for i, (label, _) in enumerate(checks)
                        ],
                    ),
                ]
            )
        )

    # --------------------------------------------------------- distribuições
    dist_parts = ["## Distribuições", ""]
    for c in [c for c in DISTRIBUTION_COLUMNS if c in present]:
        rows = df.groupBy(F.col(c)).count().orderBy(F.desc("count")).limit(25).collect()
        dist_parts += [
            f"### `{c}`",
            "",
            _md_table(
                ["Valor", "Linhas", "%"],
                [[r[c] if r[c] is not None else "_(nulo)_", f"{r['count']:,}", _pct(r["count"], total)] for r in rows],
            ),
            "",
        ]
    if "addr_state" in present:
        n_states = df.where(~_blank("addr_state")).select("addr_state").distinct().count()
        dist_parts.append(f"`addr_state`: {n_states} valores distintos.")
    sections.append("\n".join(dist_parts))

    elapsed = time.monotonic() - started
    sections.append(f"---\n_Tempo de execução: {elapsed:.0f}s_\n")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n\n".join(sections), encoding="utf-8")
    df.unpersist()
    log.info("Relatório salvo em %s (%.0fs)", output_file, elapsed)
    return output_file
