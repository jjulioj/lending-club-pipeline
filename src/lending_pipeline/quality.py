"""Validações de qualidade de dados.

Três níveis:
1. Contrato da carga (antes de escrever): colunas obrigatórias presentes.
2. Linha a linha (na silver): regras `error` mandam a linha para a quarentena;
   regras `warning` mantêm a linha, mas registram o alerta em `_dq_warnings`.
3. Carga e tabela (depois do MERGE): taxa de quarentena dentro do limite,
   chave única e não nula na silver.

Implementação própria em vez de Great Expectations/Deequ: são ~15 regras
simples, expressas em Spark, sem dependência extra e fáceis de ler e testar.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import KNOWN_LOAN_STATUSES, REQUIRED_SOURCE_COLUMNS, VALID_GRADES, VALID_TERMS


class SchemaContractError(Exception):
    """A carga não traz as colunas mínimas esperadas."""


class QualityGateError(Exception):
    """A carga ou a tabela violou um limite de qualidade e não deve ser publicada."""


@dataclass(frozen=True)
class Rule:
    name: str
    severity: str  # "error" | "warning"
    violation: Column  # True quando a linha VIOLA a regra
    description: str


def row_rules() -> list[Rule]:
    """Regras aplicadas sobre o DataFrame já tipado (saída de transforms.standardize)."""
    c = F.col
    return [
        # ---- error: linha vai para a quarentena
        Rule("loan_id_invalid", "error", c("loan_id").isNull(), "id ausente ou não numérico (ex.: linha de rodapé)"),
        Rule("issue_date_invalid", "error", c("issue_date").isNull(), "issue_d ausente ou fora do formato MMM-yyyy"),
        Rule("loan_amnt_invalid", "error", c("loan_amnt").isNull() | (c("loan_amnt") <= 0), "loan_amnt nulo ou <= 0"),
        Rule("funded_amnt_invalid", "error", c("funded_amnt").isNull() | (c("funded_amnt") < 0), "funded_amnt nulo/<0"),
        Rule(
            "int_rate_invalid",
            "error",
            c("int_rate").isNull() | (c("int_rate") < 0) | (c("int_rate") > 100),
            "int_rate nulo ou fora de [0, 100]",
        ),
        Rule("term_invalid", "error", ~F.coalesce(c("term_months").isin(VALID_TERMS), F.lit(False)), "term != 36/60"),
        Rule("grade_invalid", "error", ~F.coalesce(c("grade").isin(VALID_GRADES), F.lit(False)), "grade fora de A-G"),
        Rule(
            "loan_status_invalid",
            "error",
            ~F.coalesce(c("loan_status").isin(KNOWN_LOAN_STATUSES), F.lit(False)),
            "loan_status fora do domínio conhecido (evita classificar default errado em silêncio)",
        ),
        Rule("out_prncp_invalid", "error", c("out_prncp").isNull() | (c("out_prncp") < 0), "out_prncp nulo ou < 0"),
        # ---- warning: linha publicada, alerta registrado
        Rule("funded_gt_loan_amnt", "warning", c("funded_amnt") > c("loan_amnt"), "funded_amnt > loan_amnt"),
        Rule(
            "funded_inv_gt_funded", "warning", c("funded_amnt_inv") > c("funded_amnt"), "funded_amnt_inv > funded_amnt"
        ),
        Rule("out_prncp_gt_funded", "warning", c("out_prncp") > c("funded_amnt"), "saldo em aberto > valor financiado"),
        Rule(
            "sub_grade_mismatch", "warning", ~c("sub_grade").startswith(c("grade")), "sub_grade não pertence ao grade"
        ),
        Rule("addr_state_invalid", "warning", ~F.coalesce(c("addr_state").rlike("^[A-Z]{2}$"), F.lit(False)), "UF"),
        Rule("dti_out_of_range", "warning", (c("dti") < 0) | (c("dti") > 100), "dti fora de [0, 100]"),
        Rule(
            "resolved_with_open_balance",
            "warning",
            c("loan_status_normalized").isin("Fully Paid", "Charged Off") & (c("out_prncp") > 0),
            "empréstimo encerrado com saldo em aberto",
        ),
        # ---- warnings adicionados depois do profiling do dado real (docs/profiling_report.md)
        Rule(
            "current_with_zero_balance",
            "warning",
            (c("loan_status") == "Current") & (c("out_prncp") == 0),
            "Current com saldo zero (provavelmente quitado perto da data do snapshot)",
        ),
        Rule(
            "rec_prncp_gt_funded",
            "warning",
            # tolerância de 1 centavo, a mesma do profiling: diferença de arredondamento não é anomalia
            (c("total_rec_prncp") - c("funded_amnt")) > 0.01,
            "principal recebido maior que o valor financiado (acima de 1 centavo)",
        ),
        Rule("annual_inc_non_positive", "warning", c("annual_inc") <= 0, "renda anual <= 0"),
    ]


def _violations(rules: list[Rule]) -> Column:
    flagged = F.array(*[F.when(r.violation, F.lit(r.name)) for r in rules])
    return F.filter(flagged, lambda x: x.isNotNull())


def apply_row_rules(df: DataFrame, rules: list[Rule] | None = None) -> DataFrame:
    """Adiciona `_dq_errors` e `_dq_warnings` (arrays com os nomes das regras violadas)."""
    rules = rules or row_rules()
    errors = [r for r in rules if r.severity == "error"]
    warnings = [r for r in rules if r.severity == "warning"]
    return df.withColumn("_dq_errors", _violations(errors)).withColumn("_dq_warnings", _violations(warnings))


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.where(F.size("_dq_errors") == 0)
    quarantine = df.where(F.size("_dq_errors") > 0)
    return valid, quarantine


def check_required_columns(columns: list[str], required: list[str] = REQUIRED_SOURCE_COLUMNS) -> None:
    missing = [c for c in required if c not in columns]
    if missing:
        raise SchemaContractError(f"Carga sem colunas obrigatórias: {', '.join(missing)}")


def check_quarantine_rate(rows_total: int, rows_quarantined: int, max_rate: float) -> None:
    if rows_total == 0:
        raise QualityGateError("Carga vazia")
    rate = rows_quarantined / rows_total
    if rate > max_rate:
        raise QualityGateError(
            f"Taxa de quarentena {rate:.2%} acima do limite de {max_rate:.2%} "
            f"({rows_quarantined:,} de {rows_total:,} linhas)"
        )


def check_unique_key(df: DataFrame, key: str = "loan_id") -> None:
    stats = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct(key).alias("distinct_keys"),
        F.sum(F.when(F.col(key).isNull(), 1).otherwise(0)).alias("null_keys"),
    ).first()
    if stats.null_keys:
        raise QualityGateError(f"{stats.null_keys} linhas com {key} nulo na tabela")
    if stats.rows != stats.distinct_keys:
        raise QualityGateError(f"{key} duplicado: {stats.rows:,} linhas para {stats.distinct_keys:,} chaves")
