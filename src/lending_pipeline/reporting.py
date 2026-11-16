"""Relatórios em markdown gerados a partir das tabelas: evidências versionáveis no repositório.

- `kpi_report`: indicadores da gold + qualidade (quarentena e alertas).
- `demo_reprocess`: demonstração da Parte C (reexecução de uma carga já processada).
- `status_table`: situação de cada carga na tabela de controle.
"""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .control import latest_by_batch
from .lake import current_version, delta_exists, tables
from .pipeline import run_pipeline

RATE_COLUMNS = {"default_rate", "default_rate_resolved", "default_amount_rate_resolved"}
# Dimensões são rótulos: 2015 não vira "2,015"
DIMENSION_COLUMNS = {"issue_year", "issue_quarter", "grade", "purpose", "addr_state", "regra"}
PCT_POINT_COLUMNS = {"avg_int_rate", "weighted_avg_int_rate"}
LABELS = {
    "loan_count": "Empréstimos",
    "originated_amount": "Valor originado (loan_amnt)",
    "funded_amount": "Valor financiado (funded_amnt)",
    "outstanding_principal": "Saldo de principal em aberto",
    "avg_int_rate": "Taxa média de juros",
    "weighted_avg_int_rate": "Taxa média ponderada (por funded_amnt)",
    "default_count": "Empréstimos em default",
    "default_funded_amount": "Valor financiado em default",
    "default_principal_loss": "Perda de principal (default)",
    "default_rate": "Taxa de default (sobre total)",
    "default_rate_resolved": "Taxa de default (sobre encerrados)",
    "default_amount_rate_resolved": "Taxa de default em valor (sobre encerrados)",
}


def _fmt(column: str, value) -> str:
    if value is None:
        return "-"
    if column in DIMENSION_COLUMNS:
        return str(value)
    if column in RATE_COLUMNS:
        return f"{float(value) * 100:.2f}%"
    if column in PCT_POINT_COLUMNS:
        return f"{float(value):.2f}%"
    if isinstance(value, (Decimal, float)):
        return f"{float(value):,.0f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _md(df: DataFrame, columns: list[str] | None = None, limit: int | None = None) -> str:
    columns = columns or df.columns
    rows = (df.limit(limit) if limit else df).select(*columns).collect()
    header = "| " + " | ".join(LABELS.get(c, c) for c in columns) + " |"
    sep = "|" + "|".join("---" if i == 0 else "---:" for i in range(len(columns))) + "|"
    body = ["| " + " | ".join(_fmt(c, r[c]) for c in columns) + " |" for r in rows]
    return "\n".join([header, sep, *body])


def kpi_report(spark: SparkSession, config: Config) -> str:
    t = tables(config)

    def gold(name: str) -> DataFrame:
        return spark.read.format("delta").load(t.gold(name))

    summary = gold("kpi_portfolio_summary").first().asDict()
    summary_md = "\n".join(
        ["| Indicador | Valor |", "|---|---:|"] + [f"| {LABELS.get(k, k)} | {_fmt(k, v)} |" for k, v in summary.items()]
    )
    main_cols = [
        "loan_count",
        "funded_amount",
        "outstanding_principal",
        "weighted_avg_int_rate",
        "default_count",
        "default_rate",
        "default_rate_resolved",
    ]

    quarantine = spark.read.format("delta").load(t.silver_quarantine)
    q_by_rule = (
        quarantine.select(F.explode("_dq_errors").alias("regra"))
        .groupBy("regra")
        .agg(F.count("*").alias("linhas"))
        .orderBy(F.desc("linhas"))
    )
    silver = spark.read.format("delta").load(t.silver_loans)
    w_by_rule = (
        silver.select(F.explode("_dq_warnings").alias("regra"))
        .groupBy("regra")
        .agg(F.count("*").alias("linhas"))
        .orderBy(F.desc("linhas"))
    )

    parts = [
        "# Indicadores da carteira",
        "",
        "Gerado a partir das tabelas gold (`sql/gold/*.sql`). Definição de default: perda realizada "
        "(Charged Off e Default, incluindo os empréstimos fora da política de crédito com esses status).",
        "",
        "## Visão consolidada",
        "",
        summary_md,
        "",
        "## Por safra (ano de originação)",
        "",
        _md(gold("kpi_by_period"), ["issue_year", *main_cols]),
        "",
        "## Por grade",
        "",
        _md(gold("kpi_by_grade"), ["grade", *main_cols]),
        "",
        "## Por finalidade",
        "",
        _md(gold("kpi_by_purpose").orderBy(F.desc("funded_amount")), ["purpose", *main_cols]),
        "",
        "## Por UF (10 maiores em valor financiado)",
        "",
        _md(gold("kpi_by_state").orderBy(F.desc("funded_amount")), ["addr_state", *main_cols], limit=10),
        "",
        "## Qualidade",
        "",
        f"- Linhas na silver: {silver.count():,}",
        f"- Linhas em quarentena: {quarantine.count():,}",
        "",
        "Quarentena por regra (uma linha pode violar mais de uma):",
        "",
        _md(q_by_rule) if q_by_rule.count() else "_nenhuma_",
        "",
        "Alertas (linhas publicadas, mas sinalizadas em `_dq_warnings`):",
        "",
        _md(w_by_rule) if w_by_rule.count() else "_nenhum_",
        "",
    ]
    return "\n".join(parts)


def _fingerprint(spark: SparkSession, config: Config, batch_id: str) -> dict:
    t = tables(config)
    silver = spark.read.format("delta").load(t.silver_loans)
    content = silver.agg(
        F.count("*").alias("rows"),
        # XOR dos hashes: assinatura do conteúdo, independente da ordem das linhas
        F.expr("bit_xor(xxhash64(loan_id, _row_hash))").alias("signature"),
    ).first()
    bronze = spark.read.format("delta").load(t.bronze_loans).where(F.col("_batch_id") == batch_id).count()
    quarantine = spark.read.format("delta").load(t.silver_quarantine).where(F.col("_batch_id") == batch_id).count()
    kpis = spark.read.format("delta").load(t.gold("kpi_portfolio_summary")).first()
    return {
        "Linhas na silver": content.rows,
        "Assinatura do conteúdo da silver": content.signature,
        f"Linhas da carga {batch_id} na bronze": bronze,
        f"Linhas da carga {batch_id} na quarentena": quarantine,
        "Valor financiado total (gold)": kpis["funded_amount"],
        "Taxa de default (gold)": kpis["default_rate"],
        "Versão da tabela silver": current_version(spark, t.silver_loans),
    }


def demo_reprocess(spark: SparkSession, config: Config, batch_id: str) -> str:
    """Executa a mesma carga de novo, sem e com --force, e compara antes/depois."""
    before = _fingerprint(spark, config, batch_id)
    plain = run_pipeline(spark, config, only_batches=[batch_id], force=False)
    forced = run_pipeline(spark, config, only_batches=[batch_id], force=True)
    after = _fingerprint(spark, config, batch_id)

    rows = [f"| {k} | {before[k]} | {after[k]} | {'igual' if before[k] == after[k] else 'diferente'} |" for k in before]
    r1, r2 = plain.records[0], forced.records[0]
    identical = all(before[k] == after[k] for k in before if not k.startswith("Versão"))
    return "\n".join(
        [
            f"# Demonstração: reprocessamento da carga `{batch_id}`",
            "",
            "Cenário: a carga já foi processada com sucesso e é executada de novo.",
            "",
            "1. **Execução normal** (`run --batch ...`): o checksum do arquivo é igual ao do último sucesso, "
            f"então a carga é ignorada. Status registrado: **{r1.status}**.",
            "2. **Execução forçada** (`run --batch ... --force`): a carga passa por todo o pipeline de novo. "
            f"Status: **{r2.status}**, inseridas: **{r2.rows_inserted}**, atualizadas: **{r2.rows_updated}**, "
            f"sem mudança: **{r2.rows_unchanged}**.",
            "",
            "| Métrica | Antes | Depois | Resultado |",
            "|---|---:|---:|---|",
            *rows,
            "",
            "**Conclusão: "
            + ("nenhuma duplicidade e nenhum dado alterado.**" if identical else "ATENÇÃO, houve diferença.**"),
            "",
            "Por que funciona: a bronze substitui a partição da carga (replaceWhere), a quarentena idem, e o MERGE "
            "da silver só atualiza quando o conteúdo mudou e a carga é igual ou mais nova que a da versão atual. "
            "A versão da tabela pode avançar (o Delta registra o MERGE no histórico), mas o conteúdo é o mesmo.",
            "",
        ]
    )


def status_table(spark: SparkSession, config: Config) -> str:
    t = tables(config)
    if not delta_exists(spark, t.ops_load_control):
        return "Nenhuma carga registrada ainda."
    latest = sorted(latest_by_batch(spark, t.ops_load_control).values(), key=lambda r: r["batch_seq"])
    cols = ["batch_id", "status", "rows_read", "rows_quarantined", "rows_inserted", "rows_updated", "finished_at"]
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in latest:
        lines.append("| " + " | ".join("-" if r[c] is None else str(r[c]) for c in cols) + " |")
    return "\n".join(lines)
