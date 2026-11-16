# Uso de IA no desenvolvimento

## Resumo

Usei o Claude (Anthropic), pelo app de conversa, como par de desenvolvimento do começo ao fim: leitura do
enunciado, planejamento, comparação de alternativas, implementação, testes e documentação. A IA escreveu a
maior parte do código e da documentação. O meu papel foi tomar as decisões (stack e regras de negócio),
executar e validar tudo no dado real, conciliar os números e responder pelo resultado.

O registro cronológico completo, inclusive os erros, está em [docs/ai_log.md](docs/ai_log.md).

## Ferramentas e acesso

| Ferramenta | Uso |
|---|---|
| Claude (app), com execução de código no ambiente da própria IA | Planejamento, implementação, testes com Spark e dados sintéticos, documentação |
| Docker local e GitHub Actions | Execução no dado real e validação automática a cada push (não são IA, mas foi onde o código gerado foi validado) |

A IA não teve acesso à minha máquina nem ao dataset. Todo comando local fui eu que executei. O que
compartilhei com ela foi o enunciado, mensagens de log e erro, e os relatórios agregados gerados pelo
pipeline.

## Divisão do trabalho

| Atividade | IA | Eu |
|---|---|---|
| Plano | Propôs dividir em três noites | Replanejei quando o domingo não rendeu: a IA escreveu o pipeline durante o dia e eu executei e validei à noite |
| Stack | Comparou PySpark + Delta, Databricks Free e DuckDB + dbt | Trouxe a premissa de que o avaliador lê o código e não executa; escolhi PySpark + Delta |
| Regras de negócio | Trouxe alternativas com prós e contras | Decidi a definição de default (perda realizada), originado × financiado e a estratégia de simulação das cargas |
| Arquitetura detalhada (medallion, MERGE condicional, cubo com medidas aditivas, tabela de controle) | Propôs e implementou | Aprovei e validei com testes e no dado real |
| Profiling | Escreveu o script e listou hipóteses sobre o dataset | Rodei no dado real; decidi como tratar os achados novos |
| Execução | Sem acesso ao dataset nem ao meu ambiente | Rodei profiling, simulação, pipeline e demonstrações no Docker; acompanhei o CI |
| Conciliação dos números | Apontou uma divergência na revisão | Conferi os resultados do pipeline contra o profiling |
| Documentação | Rascunhou README e este arquivo | Revisei e completei com os resultados reais |

## Como validei o que a IA gerou

- **Testes automatizados (62):** unitários com Spark (tipagem, regras, deduplicação, simulação, indicadores
  calculados à mão) e de integração com Delta (pipeline de ponta a ponta, reprocessamento, portões de
  qualidade, evolução de schema). Rodam no CI a cada push.
- **Propriedade verificável:** depois de todas as cargas simuladas, a silver tem que ser idêntica ao
  snapshot original. Um erro na lógica de atualização quebraria esse teste.
- **Profiling antes de confiar nas regras:** as quatro hipóteses da IA sobre o dataset se confirmaram no
  dado real, e o profiling revelou três situações não previstas, que viraram alertas.
- **Conciliação dos números:** 2.260.668 empréstimos na silver, exatamente o número de ids válidos do
  arquivo; 33 linhas em quarentena, exatamente os 33 rodapés; 269.360 empréstimos em default, a soma exata
  dos status que compõem a definição; a taxa de 19,98% recalculada à parte. A demonstração de
  reprocessamento não mostrou nenhuma diferença.
- **O que revisei com mais atenção:** a conciliação dos números e a definição de default. São o que chega
  a quem usa os indicadores: um erro ali passaria pelos testes técnicos e apareceria como um número errado
  no relatório. O restante do código li em nível geral, apoiado nos testes e no CI.

O primeiro commit concentra a implementação gerada com a IA a partir das decisões acima. Os commits
seguintes registram a validação: execução no CI e no dado real, erros encontrados e ajustes.

## Onde a IA errou e como os erros apareceram

| Erro | Como apareceu | Correção |
|---|---|---|
| Testes de integração com cenário pequeno demais para o portão de qualidade de 5% | CI, na primeira execução dos testes com Delta | Limite próprio para os testes; o teste do portão continua reprovando |
| Profiling quebrava quando não havia `id` repetido (soma sobre zero linhas voltava nula) | Primeira execução no dado real | `coalesce` para zero + teste de regressão |
| Regra de alerta sem a tolerância de 1 centavo usada no profiling (136 casos contra 21) | Conciliação do relatório contra o profiling | Mesma tolerância nas duas pontas + teste; silver reconstruída do zero |

Também houve uma limitação de ambiente: a IA não conseguia executar o código que depende de Delta Lake, e
essa parte só foi validada no CI e no meu Docker. E um erro meu: uma edição manual quebrou o lint
(formatação), e o CI pegou.

Nenhum desses erros foi encontrado lendo código. Foram pegos por CI, execução no dado real e conciliação.
Por isso a validação foi a parte do processo que eu não deleguei.

## Exemplos de pedidos à IA

- **Início:** compartilhei o enunciado e pedi ajuda para construir o projeto e para documentar o uso de IA,
  como o case pede.
- **Stack:** pedi a opinião da IA explicando que, provavelmente, o avaliador não executaria o projeto. A
  recomendação levou isso em conta: CI e evidências de execução no README, para provar que funciona sem
  depender de alguém rodar.
- **Achados do profiling:** a IA propôs tratar os três casos inesperados como alertas, não como erros;
  aceitei porque nenhum deles invalida os indicadores, e descartar esses empréstimos tiraria dado
  legítimo da carteira.
- **Erros:** em todo erro (CI, execução local), mandei o log completo antes de qualquer tentativa de
  contorno, e as correções de código vieram com um teste que reproduz o problema.

## O que eu não deleguei

- As decisões de negócio: definição de default, o que é valor originado e financiado, estratégia de
  simulação, tratamento dos achados do profiling.
- A execução e a validação no dado real, e a conciliação dos números finais.
- O texto final do que foi entregue.

## Cuidados que eu manteria numa empresa

- Dados sensíveis ou internos não vão para ferramentas de IA fora do ambiente aprovado pela empresa. Aqui o
  dataset é público e, mesmo assim, a IA só viu relatórios agregados.
- Código gerado por IA passa pela mesma revisão, pelos mesmos testes e pelo mesmo CI que qualquer código.
- Afirmações técnicas da IA (comportamento de uma API, de uma versão) são conferidas na documentação ou
  em teste antes de virar premissa.

## Reflexão

A IA acelerou o processo inteiro. O meu trabalho foi decidir como as peças se encaixam e garantir que o
resultado estava certo. Dois aprendizados:

- **Velocidade de geração não é confiança.** Os três erros da IA só apareceram quando o código encontrou um
  ambiente limpo, o dado real ou uma conferência independente dos números.
- **O que eu faria diferente:** rodar o profiling no dado real antes de qualquer regra de limpeza (o
  domingo não rendeu e a ordem inverteu) e ler as partes centrais do código (o MERGE e as definições de
  negócio) logo que foram geradas, e não só no fim.
