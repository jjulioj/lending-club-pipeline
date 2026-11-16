# Cargas simuladas

Parâmetros: update_fraction=0.3, conflict_fraction=0.002, duplicate_fraction=0.001, seed=42.

- **Novos**: empréstimo chega uma vez, já no estado final.
- **Versão inicial**: empréstimo chega como Current, sem pagamentos; a versão real vem depois.
- **Atualizações**: versão real de empréstimos que chegaram como versão inicial na carga anterior.
- **Conflito na carga**: versão real na mesma carga da versão inicial (a deduplicação escolhe a real).

| Carga | Linhas | Novos | Versão inicial | Atualizações | Conflito na carga | Duplicatas exatas |
|---|---:|---:|---:|---:|---:|---:|
| `batch_001_2007` | 608 | 529 | 78 | 0 | 1 | 0 |
| `batch_002_2008` | 2,478 | 1,909 | 486 | 77 | 3 | 3 |
| `batch_003_2009` | 5,782 | 3,845 | 1,439 | 483 | 10 | 5 |
| `batch_004_2010` | 14,005 | 9,069 | 3,470 | 1,429 | 26 | 11 |
| `batch_005_2011` | 25,246 | 15,232 | 6,494 | 3,444 | 50 | 26 |
| `batch_006_2012` | 59,963 | 37,385 | 15,984 | 6,444 | 84 | 66 |
| `batch_007_2013` | 151,152 | 94,579 | 40,241 | 15,900 | 271 | 161 |
| `batch_008_2014` | 276,318 | 168,382 | 67,250 | 39,970 | 454 | 262 |
| `batch_009_2015` | 489,127 | 307,951 | 113,145 | 66,796 | 739 | 496 |
| `batch_010_2016` | 547,990 | 344,239 | 90,172 | 112,406 | 600 | 573 |
| `batch_011_2017` | 534,002 | 389,111 | 54,469 | 89,572 | 347 | 503 |
| `batch_012_2018` | 550,078 | 474,901 | 20,341 | 54,122 | 136 | 578 |
| `batch_013_updates` | 20,225 | 0 | 0 | 20,205 | 0 | 20 |
| **Total** | 2,676,974 | 1,847,132 | 413,569 | 410,848 | 2,721 | 2,704 |
