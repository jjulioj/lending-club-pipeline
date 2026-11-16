"""Fábrica de dados sintéticos no formato do arquivo do Lending Club (tudo string).

Os testes não usam o dataset real: cada cenário é montado aqui com valores
coerentes entre si (saldo, pagamentos e status), mais as armadilhas vistas no
arquivo real (rodapé no campo id, espaços, '%', texto com quebra de linha).
"""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from lending_pipeline.schema import SOURCE_COLUMNS

# Colunas extras que existem no arquivo real mas o pipeline não publica
EXTRA_COLUMNS = ["member_id", "emp_title", "title", "desc"]
HEADER = SOURCE_COLUMNS + EXTRA_COLUMNS


def loan(
    loan_id: int,
    issue_d: str = "Dec-2015",
    status: str = "Fully Paid",
    amount: float = 10000.0,
    int_rate: str = "13.56",
    grade: str = "B",
    addr_state: str = "CA",
    purpose: str = "debt_consolidation",
    term: str = " 36 months",
    **overrides,
) -> dict:
    """Um empréstimo com valores coerentes com o status."""
    normalized = status.split(":")[-1]
    if normalized == "Fully Paid":
        out_prncp, rec_prncp, recoveries, last_pymnt = 0.0, amount, 0.0, "Jan-2019"
    elif normalized in ("Charged Off", "Default"):
        out_prncp, rec_prncp, recoveries, last_pymnt = 0.0, round(amount * 0.3, 2), 500.0, "Jun-2017"
    else:  # Current, Late, In Grace Period
        out_prncp, rec_prncp, recoveries, last_pymnt = round(amount * 0.5, 2), round(amount * 0.5, 2), 0.0, "Feb-2019"

    row = {c: None for c in HEADER}
    row.update(
        {
            "id": str(loan_id),
            "loan_amnt": f"{amount}",
            "funded_amnt": f"{amount}",
            "funded_amnt_inv": f"{amount}",
            "term": term,
            "int_rate": int_rate,
            "installment": f"{round(amount / 30, 2)}",
            "grade": grade,
            "sub_grade": f"{grade}3",
            "emp_length": "10+ years",
            "home_ownership": "RENT",
            "annual_inc": "65000.0",
            "verification_status": "Verified",
            "issue_d": issue_d,
            "loan_status": status,
            "purpose": purpose,
            "addr_state": addr_state,
            "dti": "18.2",
            "revol_util": "45.1",
            "out_prncp": f"{out_prncp}",
            "out_prncp_inv": f"{out_prncp}",
            "total_pymnt": f"{round(rec_prncp * 1.15, 2)}",
            "total_pymnt_inv": f"{round(rec_prncp * 1.15, 2)}",
            "total_rec_prncp": f"{rec_prncp}",
            "total_rec_int": f"{round(rec_prncp * 0.15, 2)}",
            "total_rec_late_fee": "0.0",
            "recoveries": f"{recoveries}",
            "collection_recovery_fee": "0.0",
            "last_pymnt_d": last_pymnt,
            "last_pymnt_amnt": "300.0",
            "next_pymnt_d": "Mar-2019" if normalized not in ("Fully Paid", "Charged Off") else None,
            "last_credit_pull_d": "Mar-2019",
            "policy_code": "1",
            "application_type": "Individual",
            "initial_list_status": "w",
            "emp_title": "Engineer",
            "title": "Debt consolidation",
            "desc": None,
        }
    )
    row.update({k: (None if v is None else str(v)) for k, v in overrides.items()})
    return row


def footer(text: str = "Total amount funded in policy code 1: 1741781700") -> dict:
    row = {c: None for c in HEADER}
    row["id"] = text
    return row


def raw_df(spark: SparkSession, rows: list[dict], columns: list[str] | None = None) -> DataFrame:
    columns = columns or HEADER
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame([tuple(r.get(c) for c in columns) for r in rows], schema)


def write_csv(rows: list[dict], path: Path, columns: list[str] | None = None) -> Path:
    """Escreve um CSV como o de origem (com cabeçalho, aspas padrão RFC 4180)."""
    columns = columns or HEADER
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        for r in rows:
            writer.writerow(["" if r.get(c) is None else r[c] for c in columns])
    return path


def portfolio() -> list[dict]:
    """Carteira pequena com 3 safras, todos os status relevantes e rodapés."""
    statuses = [
        "Fully Paid",
        "Current",
        "Charged Off",
        "Late (31-120 days)",
        "In Grace Period",
        "Default",
        "Current",
        "Fully Paid",
    ]
    rows = []
    loan_id = 1000
    for year in (2015, 2016, 2017):
        for i, status in enumerate(statuses):
            loan_id += 1
            rows.append(
                loan(
                    loan_id,
                    issue_d=f"{['Jan', 'Apr', 'Jul', 'Oct'][i % 4]}-{year}",
                    status=status,
                    amount=5000.0 + 1000 * i,
                    int_rate=f"{8 + i}.5" if i % 2 else f" {8 + i}.25%",
                    grade="ABCDEFG"[i % 7],
                    addr_state=["CA", "NY", "TX"][i % 3],
                    purpose=["debt_consolidation", "credit_card"][i % 2],
                    desc='linha um\nlinha dois, com vírgula e "aspas"' if i == 0 else None,
                )
            )
    rows.append(loan(9001, issue_d="Mar-2015", status="Does not meet the credit policy. Status:Charged Off"))
    rows.append(loan(9002, issue_d="Mar-2015", status="Does not meet the credit policy. Status:Fully Paid"))
    rows.append(footer("Total amount funded in policy code 1: 1741781700"))
    rows.append(footer("Total amount funded in policy code 2: 521953170"))
    return rows
