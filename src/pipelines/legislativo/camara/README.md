# Pipeline: Câmara dos Deputados

Extrai dados públicos da [API de Dados Abertos da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2/).

## O que coleta

| Script | Fonte | Tabela destino |
|--------|-------|----------------|
| `camara_legislaturas.py` | Lista de legislaturas | `raw_camara_legislaturas` |
| `camara_deputados.py` | Perfil de cada deputado | `raw_camara_deputados` |
| `camara_votacoes.py` | Votações em plenário | `raw_camara_votacoes` |
| `camara_votos_deputados.py` | Como cada deputado votou | `raw_camara_votos_deputados` |
| `camara_votos_orientacao.py` | Orientação de bancada por votação | `raw_camara_votos_orientacao` |

## Dependências entre pipelines

`camara_votacoes` gera `id_votacoes.csv`, que é consumido por `camara_votos_deputados` e `camara_votos_orientacao`.

## Como executar

```bash
python -m src.pipelines.legislativo.camara.camara_deputados
python -m src.pipelines.legislativo.camara.camara_votacoes
python -m src.pipelines.legislativo.camara.camara_votos_deputados
python -m src.pipelines.legislativo.camara.camara_votos_orientacao
```
