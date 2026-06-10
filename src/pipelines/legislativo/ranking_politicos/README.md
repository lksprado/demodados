# Pipeline: Ranking Políticos

Extrai o ranking de desempenho parlamentar da [API do Politicos.org.br](https://apirest2.politicos.org.br/api/), que pontua parlamentares com base em critérios como presença, projetos apresentados e fiscalização.

## O que coleta

| Script | Fonte | Tabela destino |
|--------|-------|----------------|
| `ranking_parlamentares.py` | Ranking com score e atributos de cada parlamentar | `raw_ranking_parlamentares` |

## Como executar

```bash
python -m src.pipelines.legislativo.ranking_politicos.ranking_parlamentares
```
