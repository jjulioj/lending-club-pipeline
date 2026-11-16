# Indicadores da carteira

Gerado a partir das tabelas gold (`sql/gold/*.sql`). Definição de default: perda realizada (Charged Off e Default, incluindo os empréstimos fora da política de crédito com esses status).

## Visão consolidada

| Indicador | Valor |
|---|---:|
| Empréstimos | 2,260,668 |
| Valor originado (loan_amnt) | 34,016,115,925 |
| Valor financiado (funded_amnt) | 34,004,208,600 |
| Saldo de principal em aberto | 9,510,384,856 |
| Taxa média de juros | 13.09% |
| Taxa média ponderada (por funded_amnt) | 13.38% |
| Empréstimos em default | 269,360 |
| Valor financiado em default | 4,185,600,275 |
| Perda de principal (default) | 3,004,813,004 |
| Taxa de default (sobre total) | 11.92% |
| Taxa de default (sobre encerrados) | 19.98% |
| Taxa de default em valor (sobre encerrados) | 21.56% |

## Por safra (ano de originação)

| issue_year | Empréstimos | Valor financiado (funded_amnt) | Saldo de principal em aberto | Taxa média ponderada (por funded_amnt) | Empréstimos em default | Taxa de default (sobre total) | Taxa de default (sobre encerrados) |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2,007 | 603 | 4,791,550 | 0 | 12.21% | 158 | 26.20% | 26.20% |
| 2,008 | 2,393 | 19,975,025 | 0 | 12.37% | 496 | 20.73% | 20.73% |
| 2,009 | 5,281 | 51,814,750 | 0 | 12.81% | 723 | 13.69% | 13.69% |
| 2,010 | 12,537 | 126,351,175 | 0 | 12.43% | 1,757 | 14.01% | 14.01% |
| 2,011 | 21,721 | 257,363,650 | 0 | 13.18% | 3,297 | 15.18% | 15.18% |
| 2,012 | 53,367 | 717,942,625 | 0 | 14.57% | 8,644 | 16.20% | 16.20% |
| 2,013 | 134,814 | 1,982,759,550 | 4,834 | 14.80% | 21,024 | 15.59% | 15.60% |
| 2,014 | 235,629 | 3,503,840,175 | 31,224,070 | 14.06% | 41,162 | 17.47% | 18.45% |
| 2,015 | 421,095 | 6,417,608,175 | 314,725,141 | 12.94% | 75,804 | 18.00% | 20.18% |
| 2,016 | 434,407 | 6,400,541,700 | 740,794,253 | 13.46% | 68,252 | 15.71% | 23.29% |
| 2,017 | 443,579 | 6,584,957,075 | 2,477,182,083 | 13.63% | 39,169 | 8.83% | 23.13% |
| 2,018 | 495,242 | 7,936,263,150 | 5,946,454,475 | 12.74% | 8,874 | 1.79% | 15.76% |

## Por grade

| grade | Empréstimos | Valor financiado (funded_amnt) | Saldo de principal em aberto | Taxa média ponderada (por funded_amnt) | Empréstimos em default | Taxa de default (sobre total) | Taxa de default (sobre encerrados) |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 433,027 | 6,321,329,325 | 2,002,677,953 | 7.07% | 14,214 | 3.28% | 6.04% |
| B | 663,557 | 9,401,557,325 | 2,675,986,004 | 10.67% | 52,661 | 7.94% | 13.40% |
| C | 650,053 | 9,773,557,750 | 2,847,282,831 | 14.18% | 85,805 | 13.20% | 22.44% |
| D | 324,424 | 5,096,087,575 | 1,368,665,328 | 18.18% | 61,264 | 18.88% | 30.38% |
| E | 135,639 | 2,365,386,850 | 455,426,148 | 21.79% | 36,199 | 26.69% | 38.43% |
| F | 41,800 | 798,480,275 | 120,184,690 | 25.46% | 14,585 | 34.89% | 45.15% |
| G | 12,168 | 247,809,500 | 40,161,901 | 28.13% | 4,632 | 38.07% | 49.67% |

## Por finalidade

| purpose | Empréstimos | Valor financiado (funded_amnt) | Saldo de principal em aberto | Taxa média ponderada (por funded_amnt) | Empréstimos em default | Taxa de default (sobre total) | Taxa de default (sobre encerrados) |
|---|---:|---:|---:|---:|---:|---:|---:|
| debt_consolidation | 1,277,877 | 20,397,134,375 | 5,556,163,175 | 13.84% | 165,327 | 12.94% | 21.16% |
| credit_card | 516,971 | 7,918,298,325 | 2,307,074,551 | 11.94% | 50,057 | 9.68% | 16.93% |
| home_improvement | 150,457 | 2,205,605,925 | 621,448,367 | 13.02% | 15,576 | 10.35% | 17.76% |
| other | 139,440 | 1,460,696,525 | 451,670,258 | 14.48% | 16,508 | 11.84% | 21.08% |
| major_purchase | 50,445 | 639,335,600 | 188,690,822 | 13.27% | 5,498 | 10.90% | 18.61% |
| small_business | 24,689 | 404,880,250 | 103,881,230 | 15.45% | 4,652 | 18.84% | 29.86% |
| medical | 27,488 | 260,306,875 | 77,960,462 | 13.90% | 3,411 | 12.41% | 21.85% |
| car | 24,013 | 225,472,675 | 59,967,053 | 12.59% | 2,157 | 8.98% | 14.72% |
| house | 14,136 | 221,839,600 | 78,533,262 | 14.41% | 1,599 | 11.31% | 21.91% |
| moving | 15,403 | 129,160,175 | 35,019,912 | 14.92% | 2,229 | 14.47% | 23.40% |
| vacation | 15,525 | 98,675,925 | 25,990,107 | 13.61% | 1,744 | 11.23% | 19.20% |
| wedding | 2,355 | 24,518,925 | 39,173 | 14.69% | 292 | 12.40% | 12.43% |
| renewable_energy | 1,445 | 15,522,450 | 3,942,532 | 14.91% | 222 | 15.36% | 23.72% |
| educational | 424 | 2,760,975 | 3,953 | 12.19% | 88 | 20.75% | 20.80% |

## Por UF (10 maiores em valor financiado)

| addr_state | Empréstimos | Valor financiado (funded_amnt) | Saldo de principal em aberto | Taxa média ponderada (por funded_amnt) | Empréstimos em default | Taxa de default (sobre total) | Taxa de default (sobre encerrados) |
|---|---:|---:|---:|---:|---:|---:|---:|
| CA | 314,533 | 4,806,351,950 | 1,262,491,355 | 13.24% | 38,642 | 12.29% | 19.63% |
| TX | 186,335 | 2,930,155,075 | 814,488,390 | 13.27% | 21,897 | 11.75% | 19.84% |
| NY | 186,389 | 2,766,038,050 | 762,493,531 | 13.51% | 24,277 | 13.02% | 22.05% |
| FL | 161,991 | 2,332,060,875 | 672,133,931 | 13.42% | 20,608 | 12.72% | 21.50% |
| IL | 91,173 | 1,410,059,775 | 412,480,688 | 13.25% | 9,399 | 10.31% | 18.12% |
| NJ | 83,132 | 1,315,538,050 | 371,885,499 | 13.27% | 10,260 | 12.34% | 21.12% |
| GA | 74,196 | 1,136,340,900 | 327,636,201 | 13.54% | 8,013 | 10.80% | 18.43% |
| PA | 76,939 | 1,131,593,850 | 317,305,652 | 13.47% | 9,510 | 12.36% | 20.83% |
| OH | 75,132 | 1,076,886,475 | 307,103,930 | 13.48% | 9,020 | 12.01% | 20.52% |
| VA | 62,954 | 1,012,674,150 | 277,768,485 | 13.40% | 7,602 | 12.08% | 19.94% |

## Qualidade

- Linhas na silver: 2,260,668
- Linhas em quarentena: 33

Quarentena por regra (uma linha pode violar mais de uma):

| regra | count |
|---|---:|
| int_rate_invalid | 33 |
| term_invalid | 33 |
| out_prncp_invalid | 33 |
| funded_amnt_invalid | 33 |
| loan_amnt_invalid | 33 |
| issue_date_invalid | 33 |
| loan_status_invalid | 33 |
| loan_id_invalid | 33 |
| grade_invalid | 33 |

Alertas (linhas publicadas, mas sinalizadas em `_dq_warnings`):

| regra | count |
|---|---:|
| current_with_zero_balance | 4,537 |
| dti_out_of_range | 2,563 |
| annual_inc_non_positive | 1,667 |
| rec_prncp_gt_funded | 136 |
