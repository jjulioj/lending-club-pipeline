# Demonstração: reprocessamento da carga `batch_005_2011`

Cenário: a carga já foi processada com sucesso e é executada de novo.

1. **Execução normal** (`run --batch ...`): o checksum do arquivo é igual ao do último sucesso, então a carga é ignorada. Status registrado: **SKIPPED**.
2. **Execução forçada** (`run --batch ... --force`): a carga passa por todo o pipeline de novo. Status: **SUCCESS**, inseridas: **0**, atualizadas: **0**, sem mudança: **25165**.

| Métrica | Antes | Depois | Resultado |
|---|---:|---:|---|
| Linhas na silver | 2260668 | 2260668 | igual |
| Assinatura do conteúdo da silver | -8782899572633725935 | -8782899572633725935 | igual |
| Linhas da carga batch_005_2011 na bronze | 25246 | 25246 | igual |
| Linhas da carga batch_005_2011 na quarentena | 5 | 5 | igual |
| Valor financiado total (gold) | 34004208600.00 | 34004208600.00 | igual |
| Taxa de default (gold) | 0.119151 | 0.119151 | igual |
| Versão da tabela silver | 13 | 14 | diferente |

**Conclusão: nenhuma duplicidade e nenhum dado alterado.**

Por que funciona: a bronze substitui a partição da carga (replaceWhere), a quarentena idem, e o MERGE da silver só atualiza quando o conteúdo mudou e a carga é igual ou mais nova que a da versão atual. A versão da tabela pode avançar (o Delta registra o MERGE no histórico), mas o conteúdo é o mesmo.
