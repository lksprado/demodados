# Pipeline: e-Cidadania

Extrai dados do portal [e-Cidadania do Senado Federal](https://www12.senado.leg.br/ecidadania/), que permite aos cidadãos apoiar proposições legislativas.

## O que coleta

| Script | Fonte | Tabela destino |
|--------|-------|----------------|
| `ecidadania_big_numbers.py` | Totais gerais de apoios e proposições | `raw_ecidadania_bignumbers` |
| `ecidadania_mais_votados.py` | Proposições com mais apoios | `raw_ecidadania_mais_votados` |
| `ecidadania_paginas.py` | Lista paginada de todas as proposições | `raw_ecidadania_paginas` |

## Como executar

```bash
python -m src.pipelines.legislativo.ecidadania.ecidadania_big_numbers
python -m src.pipelines.legislativo.ecidadania.ecidadania_mais_votados
python -m src.pipelines.legislativo.ecidadania.ecidadania_paginas
```
