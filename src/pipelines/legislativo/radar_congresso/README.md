# Pipeline: Radar Congresso

Extrai dados do [Radar Congresso em Foco](https://radar.congressoemfoco.com.br/), que calcula o índice de governismo dos parlamentares com base nos votos em plenário.

## O que coleta

| Script | Fonte | Tabela destino |
|--------|-------|----------------|
| `radar_governismo.py` | Índice de governismo de deputados e senadores | `raw_radar_governismo_deputados` / `raw_radar_governismo_senadores` |
| `radar_parlamentares.py` | Cadastro de parlamentares | `raw_radar_parlamentares` |

## Como executar

```bash
python -m src.pipelines.legislativo.radar_congresso.radar_governismo
python -m src.pipelines.legislativo.radar_congresso.radar_parlamentares
```
