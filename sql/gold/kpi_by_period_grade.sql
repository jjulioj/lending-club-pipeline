-- Evolução por safra dentro de cada grade (risco ao longo do tempo)
-- Fonte: loan_portfolio_cube (gold), que guarda só medidas aditivas.
-- Taxas são recalculadas a partir de somas de numeradores e denominadores,
-- nunca pela média de taxas já agregadas.
--   avg_int_rate                 = média simples de int_rate (% a.a.)
--   weighted_avg_int_rate        = soma(int_rate * funded_amnt) / soma(funded_amnt)
--   default_rate                 = empréstimos em default / total de empréstimos
--   default_rate_resolved        = empréstimos em default / empréstimos encerrados (Fully Paid, Charged Off, Default)
--   default_amount_rate_resolved = valor financiado em default / valor financiado dos encerrados
SELECT
    issue_year,
    grade,
    SUM(loan_count)                                                               AS loan_count,
    SUM(originated_amount)                                                        AS originated_amount,
    SUM(funded_amount)                                                            AS funded_amount,
    SUM(outstanding_principal)                                                    AS outstanding_principal,
    ROUND(try_divide(SUM(sum_int_rate), SUM(loan_count)), 4)                      AS avg_int_rate,
    ROUND(try_divide(SUM(sum_int_rate_x_funded), SUM(funded_amount)), 4)          AS weighted_avg_int_rate,
    SUM(default_count)                                                            AS default_count,
    SUM(default_funded_amount)                                                    AS default_funded_amount,
    SUM(default_principal_loss)                                                   AS default_principal_loss,
    ROUND(try_divide(SUM(default_count), SUM(loan_count)), 6)                     AS default_rate,
    ROUND(try_divide(SUM(default_count), SUM(resolved_count)), 6)                 AS default_rate_resolved,
    ROUND(try_divide(SUM(default_funded_amount), SUM(resolved_funded_amount)), 6) AS default_amount_rate_resolved
FROM loan_portfolio_cube
GROUP BY issue_year, grade
ORDER BY issue_year, grade
