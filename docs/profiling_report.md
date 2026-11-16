# Profiling do arquivo bruto

- Arquivo: `accepted_2007_to_2018Q4.csv.gz`
- Gerado em: 2026-11-16 22:10 UTC
- Spark: 4.0.1
- Linhas: **2,260,701**
- Colunas no arquivo: **151**
- Colunas esperadas ausentes: nenhuma

## Chave do empréstimo

| Métrica | Valor |
|---|---|
| `id` nulo ou vazio | 0 |
| `id` não numérico | 33 |
| `id` distintos | 2,260,701 |
| `id` repetidos (ids com mais de 1 linha) | 0 |
| Linhas envolvidas em `id` repetido | 0 |
| `id` repetido com conteúdo diferente | 0 |
| Linhas 100% duplicadas (todas as colunas) | 0 |
| `member_id` nulo ou vazio | 2,260,701 (100.00%) |

Amostra de `id` não numérico (e quantas das colunas analisadas vêm preenchidas na linha):

| id (70 primeiros caracteres) | colunas preenchidas |
|---|---|
| Total amount funded in policy code 2: 520780182 | 0 |
| Total amount funded in policy code 1: 2063142975 | 0 |
| Total amount funded in policy code 1: 6417608175 | 0 |
| Total amount funded in policy code 1: 1404586950 | 0 |
| Total amount funded in policy code 2: 662815446 | 0 |

## Nulos ou vazios por coluna

| Coluna | Nulos/vazios | % |
|---|---|---|
| `id` | 0 | 0.00% |
| `member_id` | 2,260,701 | 100.00% |
| `loan_amnt` | 33 | 0.00% |
| `funded_amnt` | 33 | 0.00% |
| `funded_amnt_inv` | 33 | 0.00% |
| `term` | 33 | 0.00% |
| `int_rate` | 33 | 0.00% |
| `installment` | 33 | 0.00% |
| `grade` | 33 | 0.00% |
| `sub_grade` | 33 | 0.00% |
| `emp_length` | 146,940 | 6.50% |
| `home_ownership` | 33 | 0.00% |
| `annual_inc` | 37 | 0.00% |
| `verification_status` | 33 | 0.00% |
| `issue_d` | 33 | 0.00% |
| `loan_status` | 33 | 0.00% |
| `purpose` | 33 | 0.00% |
| `addr_state` | 33 | 0.00% |
| `dti` | 1,744 | 0.08% |
| `revol_util` | 1,835 | 0.08% |
| `out_prncp` | 33 | 0.00% |
| `out_prncp_inv` | 33 | 0.00% |
| `total_pymnt` | 33 | 0.00% |
| `total_rec_prncp` | 33 | 0.00% |
| `total_rec_int` | 33 | 0.00% |
| `recoveries` | 33 | 0.00% |
| `last_pymnt_d` | 2,460 | 0.11% |
| `next_pymnt_d` | 1,345,343 | 59.51% |
| `last_credit_pull_d` | 105 | 0.00% |
| `policy_code` | 33 | 0.00% |
| `application_type` | 33 | 0.00% |

## Formatos observados (valores mais frequentes)

- `int_rate`: `11.99`, `5.32`, `10.99`, `13.99`, `11.49`
- `revol_util`: `0.0`, `57.0`, `48.0`, `59.0`, `61.0`
- `term`: ` 36 months`, ` 60 months`
- `issue_d`: `Mar-2016`, `Oct-2015`, `May-2018`, `Oct-2018`, `Aug-2018`
- `last_pymnt_d`: `Mar-2019`, `Feb-2019`, `Aug-2018`, `Jan-2019`, `Mar-2018`
- `emp_length`: `10+ years`, `2 years`, `< 1 year`, `3 years`, `1 year`

## Datas (formato esperado `MMM-yyyy`)

| Métrica | Valor |
|---|---|
| `issue_d` preenchido mas não parseável | 0 |
| `issue_d` mínimo | 2007-06-01 |
| `issue_d` máximo | 2018-12-01 |
| `last_pymnt_d` preenchido mas não parseável | 0 |
| `last_pymnt_d` máximo | 2019-03-01 |

## Consistência entre valores

| Regra | Linhas | % |
|---|---|---|
| `loan_amnt` <= 0 ou não numérico | 33 | 0.00% |
| `funded_amnt` > `loan_amnt` | 0 | 0.00% |
| `funded_amnt` < `loan_amnt` | 2,065 | 0.09% |
| `funded_amnt_inv` > `funded_amnt` | 0 | 0.00% |
| `funded_amnt_inv` < `funded_amnt` | 151,111 | 6.68% |
| `int_rate` fora de [0, 40] ou não numérico | 33 | 0.00% |
| `out_prncp` < 0 | 0 | 0.00% |
| `out_prncp` > `funded_amnt` | 0 | 0.00% |
| `total_rec_prncp` > `funded_amnt` | 21 | 0.00% |
| Charged Off com `out_prncp` > 0 | 0 | 0.00% |
| Fully Paid com `out_prncp` > 0 | 0 | 0.00% |
| Current com `out_prncp` = 0 | 4,537 | 0.20% |
| `sub_grade` não começa com `grade` | 0 | 0.00% |
| `annual_inc` <= 0 | 1,667 | 0.07% |
| `dti` < 0 | 2 | 0.00% |
| `dti` > 100 | 2,561 | 0.11% |
| `addr_state` com tamanho diferente de 2 | 0 | 0.00% |

## Distribuições

### `loan_status`

| Valor | Linhas | % |
|---|---|---|
| Fully Paid | 1,076,751 | 47.63% |
| Current | 878,317 | 38.85% |
| Charged Off | 268,559 | 11.88% |
| Late (31-120 days) | 21,467 | 0.95% |
| In Grace Period | 8,436 | 0.37% |
| Late (16-30 days) | 4,349 | 0.19% |
| Does not meet the credit policy. Status:Fully Paid | 1,988 | 0.09% |
| Does not meet the credit policy. Status:Charged Off | 761 | 0.03% |
| Default | 40 | 0.00% |
| _(nulo)_ | 33 | 0.00% |

### `term`

| Valor | Linhas | % |
|---|---|---|
|  36 months | 1,609,754 | 71.21% |
|  60 months | 650,914 | 28.79% |
| _(nulo)_ | 33 | 0.00% |

### `grade`

| Valor | Linhas | % |
|---|---|---|
| B | 663,557 | 29.35% |
| C | 650,053 | 28.75% |
| A | 433,027 | 19.15% |
| D | 324,424 | 14.35% |
| E | 135,639 | 6.00% |
| F | 41,800 | 1.85% |
| G | 12,168 | 0.54% |
| _(nulo)_ | 33 | 0.00% |

### `home_ownership`

| Valor | Linhas | % |
|---|---|---|
| MORTGAGE | 1,111,450 | 49.16% |
| RENT | 894,929 | 39.59% |
| OWN | 253,057 | 11.19% |
| ANY | 996 | 0.04% |
| OTHER | 182 | 0.01% |
| NONE | 54 | 0.00% |
| _(nulo)_ | 33 | 0.00% |

### `verification_status`

| Valor | Linhas | % |
|---|---|---|
| Source Verified | 886,231 | 39.20% |
| Not Verified | 744,806 | 32.95% |
| Verified | 629,631 | 27.85% |
| _(nulo)_ | 33 | 0.00% |

### `purpose`

| Valor | Linhas | % |
|---|---|---|
| debt_consolidation | 1,277,877 | 56.53% |
| credit_card | 516,971 | 22.87% |
| home_improvement | 150,457 | 6.66% |
| other | 139,440 | 6.17% |
| major_purchase | 50,445 | 2.23% |
| medical | 27,488 | 1.22% |
| small_business | 24,689 | 1.09% |
| car | 24,013 | 1.06% |
| vacation | 15,525 | 0.69% |
| moving | 15,403 | 0.68% |
| house | 14,136 | 0.63% |
| wedding | 2,355 | 0.10% |
| renewable_energy | 1,445 | 0.06% |
| educational | 424 | 0.02% |
| _(nulo)_ | 33 | 0.00% |

### `policy_code`

| Valor | Linhas | % |
|---|---|---|
| 1.0 | 2,260,668 | 100.00% |
| _(nulo)_ | 33 | 0.00% |

### `application_type`

| Valor | Linhas | % |
|---|---|---|
| Individual | 2,139,958 | 94.66% |
| Joint App | 120,710 | 5.34% |
| _(nulo)_ | 33 | 0.00% |

### `emp_length`

| Valor | Linhas | % |
|---|---|---|
| 10+ years | 748,005 | 33.09% |
| 2 years | 203,677 | 9.01% |
| < 1 year | 189,988 | 8.40% |
| 3 years | 180,753 | 8.00% |
| 1 year | 148,403 | 6.56% |
| _(nulo)_ | 146,940 | 6.50% |
| 5 years | 139,698 | 6.18% |
| 4 years | 136,605 | 6.04% |
| 6 years | 102,628 | 4.54% |
| 7 years | 92,695 | 4.10% |
| 8 years | 91,914 | 4.07% |
| 9 years | 79,395 | 3.51% |

`addr_state`: 51 valores distintos.

---
_Tempo de execução: 89s_
