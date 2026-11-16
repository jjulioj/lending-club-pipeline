# Diário de uso de IA

Registro cronológico de como a IA (Claude) foi usada no desenvolvimento.
É a fonte do `AI_USAGE.md`. Formato de cada entrada: contexto → o que a IA
propôs → o que eu decidi ou corrigi → como foi validado.

---

## 15/11 (domingo): planejamento e esqueleto

**1. Leitura do enunciado e plano.** Compartilhei o PDF do case com o Claude.
Ele propôs dividir o trabalho em três noites (esqueleto + profiling; bronze/silver
+ testes; gold + documentação) e separar o que é decisão minha do que ele
pode implementar.

**2. Escolha de stack.** Levantei a premissa de que o avaliador provavelmente
vai ler o repositório e não executar. O Claude comparou três opções
(PySpark + Delta em Docker, Databricks Free Edition, DuckDB + dbt) e
recomendou PySpark + Delta: é o stack da vaga (Databricks/Azure), e como
ninguém vai rodar, a prova de que funciona tem que vir de CI e de evidências
de execução no README, o que é mais natural com código Python versionado do
que com notebooks. **Decisão: PySpark + Delta em Docker.**

**3. Decisões de negócio.** Pedi que o Claude não decidisse sozinho os pontos
de negócio; ele trouxe alternativas com prós e contras e eu escolhi:

| Ponto | Alternativas consideradas | Decisão |
|---|---|---|
| Simular cargas incrementais | só safras; safras + lotes de update; fatias aleatórias + duplicatas | **Safras + lotes de update** (exercita atualização de registro existente, que o case exige) |
| Definição de default | perda realizada; perda + atraso 31-120 dias; qualquer atraso | **Perda realizada** (critério objetivo; o bucket 31-120 mistura faixas de atraso) |
| Originado × financiado | `loan_amnt`/`funded_amnt`; `funded_amnt`/`funded_amnt_inv` | **`loan_amnt` / `funded_amnt`** |

A taxa de default vai ser reportada sobre a carteira total e sobre os
empréstimos já encerrados, porque safras recentes ainda não maturaram e a
taxa sobre a carteira total subestima o risco delas.

**4. Limitação do ambiente da IA.** O ambiente onde o Claude executa código não
acessa o Kaggle nem o Maven Central (de onde vêm os jars do Delta).
Consequência: ele valida as transformações com Spark puro e dados sintéticos,
e os testes que dependem de Delta rodam no meu Docker e no CI. Os testes
Delta ganharam um marcador (`@pytest.mark.delta`) para isso.

**5. Ponto técnico: ANSI no Spark 4.** Ao testar o parsing, o Claude confirmou
que o Spark 4 liga o modo ANSI por padrão: um `cast` de string inválida
derruba o job em vez de virar nulo. Padrão adotado: `try_cast` /
`try_to_timestamp`. Ficou registrado no `CLAUDE.md` e coberto por um teste
de fumaça.

**6. Profiling antes da lógica.** Em vez de escrever as regras de limpeza com
base no que a IA "sabe" do dataset, pedi um script de profiling para rodar
no dado real primeiro. Hipóteses levantadas pelo Claude, **a validar**:
- existem linhas de rodapé ("Total amount funded in policy code...") no campo `id`;
- `member_id` vem 100% vazio no dataset público;
- `term` vem com espaço à esquerda (" 36 months") e datas no formato `MMM-yyyy`;
- pode haver inconsistências entre `loan_amnt`, `funded_amnt` e `funded_amnt_inv`.

O script foi testado pelo Claude com um CSV sintético que reproduz essas
armadilhas (rodapé no `id`, duplicata exata, texto com quebra de linha
dentro de aspas).

---

## 16/11 (segunda): implementação

**7. Replanejamento por falta de tempo.** No domingo não consegui rodar o
profiling. Para caber nas noites que sobraram, combinamos que o Claude
escreveria o restante do pipeline durante o dia, com as regras baseadas nas
hipóteses e fáceis de ajustar; à noite eu rodo o profiling no dado real e
ajustamos o que ele contradisser. Trade-off assumido: a ordem ideal seria
profiling antes das regras.

**8. O que o Claude implementou** (sobre as decisões das entradas 2 e 3):
simulação de cargas, bronze com `replaceWhere` por carga, silver com MERGE
condicional, quarentena, tabela de controle com checksum, gold com cubo de
medidas aditivas + consultas SQL, relatórios de evidência e testes.

**9. Pontos técnicos que o Claude antecipou** (cada um tem teste):
- MERGE do Delta aborta se duas linhas da origem casam com a mesma linha do
  destino → deduplicação dentro da carga antes do MERGE.
- O Spark escreve CSV escapando aspas com barra invertida por padrão, e a
  leitura usa o padrão RFC (aspas dobradas) → escrita configurada com
  `escape='"'`; teste de ida e volta com texto com quebra de linha e aspas.
- Nomes de arquivo gerados pelo Spark têm UUID aleatório → checksum da carga
  calculado só sobre o conteúdo.
- Editar um CSV à mão invalida o `.crc` do Hadoop e quebra a leitura → o teste
  de reentrega corrigida remove o `.crc`.
- Um MERGE sem mudanças pode não gerar commit → as métricas só são lidas se a
  versão da tabela avançou (senão viriam do commit anterior).
- Reprocessar uma carga antiga não pode sobrescrever dado mais novo →
  condição do MERGE: hash diferente **e** carga igual ou mais nova.
- Taxas não são somáveis → gold guarda numeradores e denominadores; as
  consultas recalculam a taxa em cada recorte.

**10. Validação no ambiente da IA.** 51 testes com Spark puro passaram no
ambiente do Claude. Os 6 testes que dependem de Delta (pipeline de ponta a
ponta, reprocessamento, portões de qualidade, evolução de schema) **ainda não
tinham sido executados** nesse ponto: rodam no meu Docker e no CI.

**11. Primeiro erro real, pego pelo CI.** Na primeira execução dos testes com
Delta (GitHub Actions), 5 testes de integração falharam: o cenário de teste
tinha ~14 linhas por carga, 2 linhas de rodapé caíram na mesma carga (14%) e o
portão de qualidade (limite de 5%) reprovou a carga. O portão funcionou como
deveria; o erro foi do desenho do teste, feito pela IA, que não considerou a
escala do cenário. Correção: limite de 50% só nos testes de integração; o teste
específico do portão continua reprovando (10 de 11 linhas inválidas). O CI
confirmou também que o caminho com Delta (bronze, MERGE, controle) funcionava.

**12. Primeiro contato com o dado real e segundo erro.** O profiling leu
2.260.701 linhas e 151 colunas em ~1 minuto e quebrou ao montar o relatório:
como não existe nenhum `id` repetido no arquivo, uma soma sobre zero linhas
voltou nula em vez de zero. O teste da IA só cobria o caso com repetidos.
Correção: `coalesce` para zero na contagem condicional + teste de regressão
sem repetidos (confirmado falhando antes da correção e passando depois).
Primeiro achado do dado real: `id` não tem repetição, o que sustenta a escolha
da chave.