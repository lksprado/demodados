# Pipeline: Senado Federal

Extrai dados públicos da [API de Dados Abertos do Senado Federal](https://legis.senado.leg.br/dadosabertos/).

## O que coleta

| Script | Fonte | Tabela destino |
|--------|-------|----------------|
| `senado_legislatura.py` | Legislaturas e senadores por legislatura | `raw_senado_legislaturas` |
| `senado_senadores.py` | Perfil dos senadores em exercício | `raw_senado_senadores` |
| `senado_votacoes.py` | Votações em plenário | `raw_senado_votacoes` |
| `senado_votos_senadores.py` | Como cada senador votou | `raw_senado_votos_senadores` |
| `senado_votos_orientacao.py` | Orientação de bancada por votação | `raw_senado_votos_orientacao` |
| `senado_status.py` | Status de tramitação das proposições do e-Cidadania | `raw_senado_status` |

## Dependências entre pipelines

`senado_votacoes` gera `id_votacoes.csv`, consumido por `senado_votos_senadores` e `senado_votos_orientacao`.
`senado_status` depende de `ecidadania_paginas_consolidado.csv` gerado pelo pipeline **e-Cidadania**.

## Como executar

```bash
python -m src.pipelines.legislativo.senado.senado_senadores
python -m src.pipelines.legislativo.senado.senado_legislatura
python -m src.pipelines.legislativo.senado.senado_votacoes
python -m src.pipelines.legislativo.senado.senado_votos_senadores
python -m src.pipelines.legislativo.senado.senado_votos_orientacao
python -m src.pipelines.legislativo.senado.senado_status
```
