# Uso de IA no desenvolvimento

> ✏️ **Rascunho.** Revisar em primeira pessoa e completar os trechos marcados com ✏️ com o que de fato
> acontecer na execução. Nada aqui deve descrever algo que não aconteceu.

## Resumo

Usei o Claude (Anthropic) como par de desenvolvimento do começo ao fim: leitura do enunciado, planejamento,
comparação de alternativas, implementação, testes e documentação. A divisão foi clara desde o início:
**decisões de negócio e de arquitetura são minhas; a IA propõe alternativas, implementa e aponta riscos;
nada entra sem teste ou sem validação no dado real.** O registro cronológico completo está em
[docs/ai_log.md](docs/ai_log.md).

A IA escreveu a maior parte do código. Meu trabalho foi definir o que construir, decidir os pontos de
negócio, revisar o código, executar e validar no dado real, e responder pelo resultado.

## Ferramentas

| Ferramenta | Uso |
|---|---|
| Claude (app, com execução de código no ambiente da própria IA) | Planejamento, implementação, testes com Spark e dados sintéticos, documentação |
| `CLAUDE.md` no repositório | Briefing persistente para o agente: decisões já tomadas, regras técnicas e comandos |

> ✏️ Se usar o Claude Code localmente (por exemplo para rodar e depurar no Docker), acrescentar aqui. Se não
> usar, avaliar se o `CLAUDE.md` continua fazendo sentido no repositório.

## Divisão do trabalho

| Atividade | IA | Eu |
|---|---|---|
| Plano de execução | Propôs dividir em três noites e separar decisões de implementação | Aprovei e ajustei quando o domingo não rendeu |
| Stack | Comparou PySpark + Delta, Databricks Free e DuckDB + dbt | Levantei a premissa de que o avaliador lê o código e não executa; escolhi PySpark + Delta |
| Regras de negócio (default, valores, simulação de cargas) | Trouxe alternativas com prós e contras | Decidi cada uma (tabela em [docs/ai_log.md](docs/ai_log.md)) |
| Profiling | Escreveu o script e listou hipóteses sobre o dataset | Rodei no dado real ✏️ e confirmei ou derrubei as hipóteses |
| Código do pipeline e testes | Implementou | Revisei, executei no Docker e no dado completo ✏️ |
| Documentação | Rascunhou README e este arquivo | Revisei, completei com os resultados reais e assumi o texto final |

## Como validei o que a IA gerou

- **Testes automatizados (57):** unitários com Spark (tipagem, regras, deduplicação, simulação, indicadores
  calculados à mão) e de integração com Delta (pipeline de ponta a ponta, reprocessamento, portões de
  qualidade, evolução de schema). Rodam no CI a cada push.
- **Propriedade verificável:** depois de todas as cargas simuladas, a silver tem que ser idêntica ao
  snapshot original. Se a lógica de atualização tivesse erro, esse teste quebraria.
- **Profiling antes de confiar nas regras:** as regras de limpeza partiram de hipóteses da IA sobre o
  dataset; o profiling no dado real decidiu o que ficava. ✏️ (quais hipóteses se confirmaram e o que mudou)
- **Conferência dos números:** ✏️ (ex.: total de empréstimos na silver contra o profiling; taxa de default
  conferida por uma consulta independente)
- **Leitura do código:** ✏️ (o que revisei com mais atenção e por quê)

## Onde a IA errou, foi corrigida ou teve limitação

- **Limitação de ambiente:** o ambiente onde a IA executa código não acessa o Kaggle nem o repositório de
  onde vêm os jars do Delta. Ela testou com dados sintéticos e Spark puro; a parte com Delta só foi
  validada no meu Docker e no CI. ✏️ (o que aconteceu na primeira execução com Delta)
- ✏️ Correções feitas a partir do profiling do dado real.
- ✏️ Qualquer ponto em que discordei da IA ou mudei o que ela propôs.

## Exemplos de pedidos à IA

> ✏️ Escolher 3 ou 4 pedidos reais, de preferência os que mostram critério. Exemplos que aconteceram:
> - Pedi opinião de stack explicando a premissa de que o avaliador não executaria o projeto, e a
>   recomendação levou isso em conta (CI e evidências no README).
> - Pedi que as decisões de negócio viessem como alternativas para eu escolher, não decididas pela IA.

## O que eu não deleguei

- As decisões de negócio: chave, definição de default, o que é valor originado e financiado, estratégia de
  simulação.
- A validação no dado real e a conferência dos números finais.
- O texto final do que foi entregue.

## Cuidados que eu manteria numa empresa

- Dados sensíveis ou internos não vão para ferramentas de IA fora do ambiente aprovado pela empresa. Aqui o
  dataset é público, então não houve restrição.
- Código gerado por IA passa pela mesma revisão e pelos mesmos testes que qualquer código.
- Afirmações técnicas da IA (comportamento de uma API, de uma versão) são conferidas na documentação ou
  em teste antes de virar premissa.

## O que mudou no meu jeito de trabalhar

> ✏️ Duas ou três frases honestas: onde a IA acelerou de verdade, onde atrapalhou ou exigiu atenção, e o
> que eu faria diferente.
