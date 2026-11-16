# CLAUDE.md: contexto e regras do projeto para o agente de IA

Este arquivo é lido automaticamente pelo Claude Code ao abrir o repositório e
serve de briefing persistente. As decisões de negócio e de arquitetura são do
autor (Júlio). O agente implementa, propõe alternativas e aponta riscos.

## Contexto

Case técnico de Engenharia de Dados: pipeline da carteira de empréstimos do
Lending Club (Kaggle `wordsforthewise/lending-club`, arquivo de empréstimos
aceitos). Ingestão → tratamento → validação → camada analítica, com cargas
incrementais (novos registros + atualizações) e reprocessamento idempotente.

## Decisões já tomadas (não alterar sem perguntar)

- Stack: Python 3.11, PySpark 4.0.1, Delta Lake 4.0.0, execução em Docker.
  O código deve rodar também no Databricks sem mudança (ver `spark.py`).
- Arquitetura medallion: bronze (como chegou, tudo string) → silver (tipado,
  limpo, estado atual por empréstimo) → gold (indicadores).
- Chave única do empréstimo: `id`.
- Simulação de cargas: fatias por safra (`issue_d`) + lotes de atualização
  gerados com semente fixa.
- Default = perda realizada: `Charged Off`, `Default` e
  `Does not meet the credit policy. Status:Charged Off`.
- Valor originado = `loan_amnt`; valor financiado = `funded_amnt`; a taxa
  média ponderada usa `funded_amnt`.

## Regras técnicas

- Spark 4 roda com ANSI ligado: cast inválido derruba o job. Converter
  string com `try_cast` / `try_to_timestamp`, nunca com `cast` direto.
- Transformações são funções puras (DataFrame → DataFrame), separadas da
  leitura/escrita Delta, para poderem ser testadas sem Delta.
- Toda escrita precisa ser segura para reexecução (mesma carga processada
  duas vezes = mesmo resultado, sem duplicar).
- Sem pandas no caminho dos dados; tudo em Spark.
- Código novo vem com teste. Antes de dar algo como pronto:
  `docker compose run --rm tests` e `ruff check .`.
- Identificadores em inglês; comentários, docstrings e documentação em português.
- Nunca versionar dados (`data/` está no .gitignore).
- Decisões e correções relevantes são registradas em `docs/ai_log.md`.

## Comandos

```bash
docker compose build                                   # imagem com Java 17 + PySpark + Delta
docker compose run --rm tests                          # testes (inclui os com Delta)
docker compose run --rm pipeline profile               # profiling do arquivo bruto
docker compose run --rm pipeline simulate              # gera as cargas em landing
docker compose run --rm pipeline run                   # processa cargas pendentes + gold
docker compose run --rm pipeline run --batch X --force # reprocessa uma carga
docker compose run --rm pipeline report                # docs/kpi_report.md
docker compose run --rm pipeline demo-reprocess --batch X
ruff check . && ruff format --check .
```

## Onde fica cada coisa

- Regras de negócio e domínios: `src/lending_pipeline/schema.py`
- Regras de qualidade: `src/lending_pipeline/quality.py`
- Consultas da gold: `sql/gold/*.sql`
- Testes sem Delta rodam com `LENDING_TEST_DELTA=0 pytest`
