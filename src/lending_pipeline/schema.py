"""Contrato de dados: colunas de origem esperadas, domínios e definições de negócio.

Tudo que é regra de negócio (o que é default, o que é empréstimo encerrado,
quais colunas a silver publica) fica centralizado aqui, para que a definição
documentada no README e o código sejam a mesma coisa.
"""

from __future__ import annotations

# Colunas mínimas que toda carga precisa trazer. Se faltar alguma, a carga é
# rejeitada antes de qualquer escrita (quebra de contrato com a origem).
REQUIRED_SOURCE_COLUMNS = [
    "id",
    "loan_amnt",
    "funded_amnt",
    "funded_amnt_inv",
    "term",
    "int_rate",
    "installment",
    "grade",
    "sub_grade",
    "issue_d",
    "loan_status",
    "purpose",
    "addr_state",
    "out_prncp",
    "total_rec_prncp",
]

# Colunas opcionais: se vierem, são tipadas e publicadas; se não vierem, ficam nulas.
OPTIONAL_SOURCE_COLUMNS = [
    "emp_length",
    "home_ownership",
    "annual_inc",
    "verification_status",
    "dti",
    "revol_util",
    "out_prncp_inv",
    "total_pymnt",
    "total_pymnt_inv",
    "total_rec_int",
    "total_rec_late_fee",
    "recoveries",
    "collection_recovery_fee",
    "last_pymnt_d",
    "last_pymnt_amnt",
    "next_pymnt_d",
    "last_credit_pull_d",
    "policy_code",
    "application_type",
    "initial_list_status",
]

SOURCE_COLUMNS = REQUIRED_SOURCE_COLUMNS + OPTIONAL_SOURCE_COLUMNS

# Formato de mês usado pelo Lending Club (ex.: "Dec-2015")
SOURCE_DATE_FORMAT = "MMM-yyyy"

VALID_TERMS = [36, 60]
VALID_GRADES = ["A", "B", "C", "D", "E", "F", "G"]

# Prefixo usado nos empréstimos de 2007-2010 que não atendiam à política de
# crédito vigente. O status "real" vem depois dos dois pontos.
CREDIT_POLICY_PREFIX = "Does not meet the credit policy. Status:"

KNOWN_LOAN_STATUSES = [
    "Fully Paid",
    "Current",
    "Charged Off",
    "Late (31-120 days)",
    "In Grace Period",
    "Late (16-30 days)",
    "Default",
    f"{CREDIT_POLICY_PREFIX}Fully Paid",
    f"{CREDIT_POLICY_PREFIX}Charged Off",
]

# ---------------------------------------------------------------------------
# Definição de default (decisão documentada no README)
# Default = perda realizada. Avaliado sobre o status normalizado, ou seja,
# "Does not meet the credit policy. Status:Charged Off" também é default.
#   - Charged Off: baixado como perda pela instituição.
#   - Default: 121+ dias de atraso; na prática precede o Charged Off.
# Atrasos (Late, In Grace Period) NÃO são default: medem inadimplência, e o
# bucket "Late (31-120 days)" mistura faixas de risco muito diferentes.
# ---------------------------------------------------------------------------
DEFAULT_STATUSES = ["Charged Off", "Default"]

# Empréstimos com desfecho conhecido: denominador da taxa de default "sobre
# encerrados", que não é distorcida por safras recentes ainda em aberto.
RESOLVED_STATUSES = ["Fully Paid", "Charged Off", "Default"]
