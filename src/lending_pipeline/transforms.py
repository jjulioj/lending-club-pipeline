"""Transformações puras (DataFrame -> DataFrame), sem leitura nem escrita.

Separadas do I/O Delta para poderem ser testadas com Spark puro e reutilizadas
em qualquer ambiente (local, Docker, Databricks).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .schema import (
    CREDIT_POLICY_PREFIX,
    DEFAULT_STATUSES,
    RESOLVED_STATUSES,
    SOURCE_COLUMNS,
    SOURCE_DATE_FORMAT,
)

MONEY = "decimal(18,2)"
RATE = "decimal(9,4)"

# Colunas de negócio publicadas na silver, na ordem em que aparecem
BUSINESS_COLUMNS = [
    "loan_id",
    "loan_amnt",
    "funded_amnt",
    "funded_amnt_inv",
    "term_months",
    "int_rate",
    "installment",
    "grade",
    "sub_grade",
    "emp_length_years",
    "home_ownership",
    "annual_inc",
    "verification_status",
    "issue_date",
    "issue_year",
    "issue_quarter",
    "loan_status",
    "loan_status_normalized",
    "meets_credit_policy",
    "is_default",
    "is_resolved",
    "purpose",
    "addr_state",
    "dti",
    "revol_util",
    "out_prncp",
    "out_prncp_inv",
    "total_pymnt",
    "total_pymnt_inv",
    "total_rec_prncp",
    "total_rec_int",
    "total_rec_late_fee",
    "recoveries",
    "collection_recovery_fee",
    "last_pymnt_date",
    "last_pymnt_amnt",
    "next_pymnt_date",
    "last_credit_pull_date",
    "policy_code",
    "application_type",
    "initial_list_status",
]


# --------------------------------------------------------------------------- parsing
def clean_str(c: str) -> Column:
    """Trim + string vazia vira nulo."""
    trimmed = F.trim(F.col(c))
    return F.when(trimmed == "", None).otherwise(trimmed)


def parse_decimal(c: str, type_: str = MONEY) -> Column:
    """String -> decimal tolerante a '%', '$', ',' e espaços.

    Usa try_cast: com ANSI ligado (padrão no Spark 4), um cast inválido
    derrubaria o job inteiro; aqui ele vira nulo e a regra de qualidade decide.
    """
    return F.regexp_replace(clean_str(c), "[%$,]", "").try_cast(type_)


def parse_int(c: str) -> Column:
    # via double: aceita "1" e "1.0"
    return clean_str(c).try_cast("double").try_cast("int")


def parse_bigint_id(c: str) -> Column:
    # Só aceita id puramente numérico: linhas de rodapé ("Total amount funded...") viram nulo
    value = clean_str(c)
    return F.when(value.rlike(r"^\d+$"), value.try_cast("bigint"))


def parse_month(c: str) -> Column:
    """'Dec-2015' -> 2015-12-01."""
    return F.try_to_timestamp(clean_str(c), F.lit(SOURCE_DATE_FORMAT)).cast("date")


def _first_number(value: Column) -> Column:
    digits = F.regexp_extract(value, r"(\d+)", 1)
    return F.when(digits != "", digits.try_cast("int"))


def parse_term_months(c: str) -> Column:
    """' 36 months' -> 36."""
    return _first_number(clean_str(c))


def parse_emp_length(c: str) -> Column:
    """'< 1 year' -> 0, '10+ years' -> 10, '3 years' -> 3, 'n/a' -> nulo."""
    value = clean_str(c)
    return (
        F.when(value.isNull() | (F.lower(value) == "n/a"), None)
        .when(value.startswith("<"), F.lit(0))
        .otherwise(_first_number(value))
    )


# --------------------------------------------------------------------------- standardize
def ensure_source_columns(df: DataFrame, columns: list[str] = SOURCE_COLUMNS) -> DataFrame:
    """Adiciona como nulas as colunas opcionais que não vieram na carga."""
    missing = [c for c in columns if c not in df.columns]
    for c in missing:
        df = df.withColumn(c, F.lit(None).cast("string"))
    return df


def standardize(raw: DataFrame) -> DataFrame:
    """Converte a bronze (tudo string) para o modelo tipado da silver.

    Mantém as colunas técnicas (prefixo `_`) e o `id` original em `source_id`,
    usado para rastrear linhas rejeitadas na quarentena.
    """
    df = ensure_source_columns(raw)
    status = clean_str("loan_status")
    status_normalized = F.when(status.startswith(CREDIT_POLICY_PREFIX), F.substring_index(status, ":", -1)).otherwise(
        status
    )
    issue_date = parse_month("issue_d")

    typed = {
        "loan_id": parse_bigint_id("id"),
        "loan_amnt": parse_decimal("loan_amnt"),
        "funded_amnt": parse_decimal("funded_amnt"),
        "funded_amnt_inv": parse_decimal("funded_amnt_inv"),
        "term_months": parse_term_months("term"),
        "int_rate": parse_decimal("int_rate", RATE),
        "installment": parse_decimal("installment"),
        "grade": F.upper(clean_str("grade")),
        "sub_grade": F.upper(clean_str("sub_grade")),
        "emp_length_years": parse_emp_length("emp_length"),
        "home_ownership": F.upper(clean_str("home_ownership")),
        "annual_inc": parse_decimal("annual_inc"),
        "verification_status": clean_str("verification_status"),
        "issue_date": issue_date,
        "issue_year": F.year(issue_date),
        "issue_quarter": F.when(
            issue_date.isNotNull(),
            F.concat(F.year(issue_date).cast("string"), F.lit("-Q"), F.quarter(issue_date).cast("string")),
        ),
        "loan_status": status,
        "loan_status_normalized": status_normalized,
        "meets_credit_policy": F.when(status.isNotNull(), ~status.startswith(CREDIT_POLICY_PREFIX)),
        "is_default": F.when(status.isNotNull(), status_normalized.isin(DEFAULT_STATUSES)),
        "is_resolved": F.when(status.isNotNull(), status_normalized.isin(RESOLVED_STATUSES)),
        "purpose": F.lower(clean_str("purpose")),
        "addr_state": F.upper(clean_str("addr_state")),
        "dti": parse_decimal("dti", "decimal(9,2)"),
        "revol_util": parse_decimal("revol_util", "decimal(9,2)"),
        "out_prncp": parse_decimal("out_prncp"),
        "out_prncp_inv": parse_decimal("out_prncp_inv"),
        "total_pymnt": parse_decimal("total_pymnt"),
        "total_pymnt_inv": parse_decimal("total_pymnt_inv"),
        "total_rec_prncp": parse_decimal("total_rec_prncp"),
        "total_rec_int": parse_decimal("total_rec_int"),
        "total_rec_late_fee": parse_decimal("total_rec_late_fee"),
        "recoveries": parse_decimal("recoveries"),
        "collection_recovery_fee": parse_decimal("collection_recovery_fee"),
        "last_pymnt_date": parse_month("last_pymnt_d"),
        "last_pymnt_amnt": parse_decimal("last_pymnt_amnt"),
        "next_pymnt_date": parse_month("next_pymnt_d"),
        "last_credit_pull_date": parse_month("last_credit_pull_d"),
        "policy_code": parse_int("policy_code"),
        "application_type": clean_str("application_type"),
        "initial_list_status": clean_str("initial_list_status"),
    }
    technical = [c for c in df.columns if c.startswith("_")]
    return df.select(
        *[expr.alias(name) for name, expr in typed.items()],
        F.col("id").alias("source_id"),
        *technical,
    )


def with_row_hash(df: DataFrame, columns: list[str] = BUSINESS_COLUMNS) -> DataFrame:
    """Hash do conteúdo de negócio: detecta se uma nova versão mudou algo de fato."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in columns]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def deduplicate_versions(df: DataFrame) -> DataFrame:
    """Mantém uma única versão por empréstimo dentro da mesma carga.

    Necessário antes do MERGE: se duas linhas da origem casarem com a mesma
    linha do destino, o Delta aborta o MERGE. Critério de "mais recente":
    último pagamento, depois última consulta de crédito, depois total pago
    (valores acumulados nunca diminuem); o hash só desempata de forma
    determinística. Duplicatas exatas caem no mesmo critério.
    """
    w = Window.partitionBy("loan_id").orderBy(
        F.col("last_pymnt_date").desc_nulls_last(),
        F.col("last_credit_pull_date").desc_nulls_last(),
        F.col("total_pymnt").desc_nulls_last(),
        F.col("_row_hash").desc(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
