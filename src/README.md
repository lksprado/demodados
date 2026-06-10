# Estrutura do diretório `src/`

Este diretório contém o código-fonte dos pipelines de ingestão de dados. Abaixo, a organização e como criar uma nova pipeline.

## Estrutura de pastas

- `params/`\
  Arquivos de parâmetros (CSVs de IDs, scripts auxiliares como `atualizar_deputados.py` e `dbt_seed_maker.py`).

- `pipelines/`\
  Implementações organizadas por tema:
  - `legislativo/camara/` → Câmara dos Deputados (deputados, votações, votos, legislaturas, orientações de voto).
  - `legislativo/senado/` → Senado Federal (senadores, sessões, votações, votos, orientações).
  - `legislativo/ecidadania/` → Portal E-Cidadania (big numbers, mais votados, páginas).
  - `legislativo/radar_congresso/` → Radar Congresso (governismo, parlamentares).
  - `legislativo/ranking_politicos/` → Ranking Políticos.

  Cada subpasta possui seu próprio arquivo de configuração YAML (`*_config.yml`).

- `utils/`\
  Utilitários reutilizáveis:
  - `extractors/` → `HttpJsonExtractor`: HTTP com retry, back-off exponencial e `fetch_and_save_many()` para fetches paralelos.
  - `loaders/` → `PostgreSQLManager`: carrega DataFrames no schema `raw.*`, adicionando `arquivo_origem` e `data_carga` automaticamente.
  - `transformers/cleaning.py` → `ColumnSanitizer`: API encadeável para normalizar nomes/valores de colunas.
  - `transformers/json_parsers.py` → `normalize_json_object`, `make_df_from_json_list` para achatar JSON aninhado.
  - `transformers/html_parsers.py` → utilitários para parsear HTML (scraping via Playwright/Selenium).
  - `fixers/` → utilitários de correção de arquivos (ex.: `filename_fixer.py`).
  - `pipeline_cfg.py` → contém `PipelineConfig`, `load_source_config` e `GenericETL`.

## Como funcionam os pipelines

### `PipelineConfig`

Dataclass que centraliza todos os caminhos e parâmetros de uma pipeline. O modo preferido de instanciar é via `load_source_config`, que lê um YAML:

```python
from src.utils.pipeline_cfg import PipelineConfig, load_source_config

config = load_source_config("camara_config.yml", source="deputados", env="local")
cfg = PipelineConfig(**config)
```

Campos disponíveis:

| Campo | Tipo | Descrição |
|---|---|---|
| `landing_dir` | `Path \| str` | Diretório dos arquivos brutos |
| `bronze_dir` | `Path \| str \| None` | Diretório pós-transformação |
| `db_table` | `str \| None` | Nome da tabela no banco |
| `url_base` | `str \| None` | URL base da API |
| `subpath` | `str \| None` | Sufixo anexado a `landing_dir` e `bronze_dir` |
| `error_dir` | `Path \| str \| None` | Diretório de fallback para erros |
| `landing_file` | `str \| None` | Nome do arquivo landing (suporta `{date}`) |
| `bronze_file` | `str \| None` | Nome do arquivo bronze (suporta `{date}`) |
| `parameter_dir` | `Path \| str \| None` | Diretório do arquivo de parâmetros |
| `parameter_file` | `str \| None` | Nome do arquivo de parâmetros (ex.: CSV de IDs) |
| `output_param_dir` | `Path \| str \| None` | Diretório do arquivo de parâmetros de saída |
| `output_param_file` | `str \| None` | Nome do arquivo de parâmetros de saída |
| `criar_dirs` | `bool` | Cria diretórios automaticamente no `__post_init__` |

Propriedades derivadas: `landing_filepath`, `bronze_filepath`, `parameter_filepath`, `output_param_filepath`.

### Formato do YAML de configuração

Cada fonte possui um YAML com seções `environments` e `sources`:

```yaml
environments:
  local:
    base_raw: ./data/raw
    base_bronze: ./data/bronze
    base_parameters: ./data/raw/parameters
  airflow:
    base_raw: /usr/local/airflow/mylake/raw/...
    base_bronze: /usr/local/airflow/mylake/bronze/...
    base_parameters: /usr/local/airflow/mylake/raw/.../parameters

sources:
  deputados:
    base_url: https://dadosabertos.camara.leg.br/api/v2/deputados/
    subpath: deputados
    bronze_file: parlamento_deputados.csv
    db_table: raw_camara_deputados
    parameter_file: id_deputados.csv
```

`load_source_config(config_path, source, env)` combina as seções e retorna um dict pronto para `PipelineConfig(**config)`.

### `GenericETL`

Define o contrato de orquestração com quatro métodos:

- `extract()` — executa `extract_fn(cfg)` ou o extrator genérico (GET único → arquivo).
- `transform()` — executa `transform_fn(cfg)` (obrigatório; lança `NotImplementedError` se não fornecido).
- `load()` — executa `load_fn(cfg)` ou o loader genérico (lê bronze CSV → Postgres).
- `run()` — atalho que chama `extract() → transform() → load()` em sequência.

Exemplo completo:

```python
from functools import partial

etl = GenericETL(
    cfg=cfg,
    extract_fn=partial(extract, workers=4),  # extract_fn custom com workers
    transform_fn=transform,
    load_fn=None,                             # usa loader genérico -> Postgres
    log=logger,
)

etl.run()
# ou passo a passo:
# etl.extract()
# etl.transform()
# etl.load()
```

## Como criar uma nova pipeline (passo a passo)

1. **Crie o YAML de configuração** em `src/pipelines/<tema>/<fonte>/<fonte>_config.yml`:
```yaml
environments:
  local:
    base_raw: ./data/raw/<fonte>
    base_bronze: ./data/bronze/<fonte>
  airflow:
    base_raw: /usr/local/airflow/mylake/raw/<fonte>
    base_bronze: /usr/local/airflow/mylake/bronze/<fonte>

sources:
  minha_fonte:
    base_url: https://api.exemplo.com/v1/recurso/
    subpath: minha_fonte
    bronze_file: minha_fonte.csv
    db_table: raw_minha_fonte
    parameter_file: id_minha_fonte.csv  # opcional
```

2. **Implemente `extract`, `transform` e `run_pipeline`** no módulo da pipeline:
```python
_CONFIG_FILE = Path(__file__).parent / "minha_fonte_config.yml"

def extract(cfg: PipelineConfig):
    # lê cfg.parameter_filepath, monta tasks, chama fetch_and_save_many
    ...

def transform(cfg: PipelineConfig):
    # lê cfg.landing_dir, produz cfg.bronze_filepath (CSV sep=";")
    ...

def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(cfg=cfg, extract_fn=extract, transform_fn=transform, log=logger)
    etl.run()

if __name__ == "__main__":
    logging.basicConfig(...)
    config = load_source_config(_CONFIG_FILE, source="minha_fonte", env="local")
    run_pipeline(PipelineConfig(**config))
```

3. **Execute**:
```bash
python -m src.pipelines.<tema>.<fonte>.minha_pipeline
```

## Boas práticas

- **Separador/encoding**: mantenha `sep=";"` no transform e no load (padrão atual).
- **Logging**:
  - Scripts locais: configure `basicConfig` no entry-point (`if __name__ == "__main__"`).
  - Airflow: não chame `basicConfig`; use `logging.getLogger(__name__)`.
- **Idempotência**: o `transform` deve escrever de forma atômica (`.tmp` + rename) quando possível.
- **Parâmetros de saída**: pipelines que geram IDs para etapas posteriores (ex.: votações → votos) devem gravar `cfg.output_param_filepath` ao final do transform.
- **Reexecução parcial**: como tudo é dirigido por `cfg`, é possível reprocessar apenas `transform()` ou `load()` isoladamente se necessário.
