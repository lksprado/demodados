# Estrutura do diretório `src/`

Este diretório contém o código-fonte dos pipelines de ingestão de dados. Abaixo, a organização e como criar uma nova pipeline.

## Estrutura de pastas

- `params/`\
  Arquivos de parâmetros (ex.: CSVs de IDs, configs auxiliares) usados para executar requisições.

- `pipelines/` \
  Implementações das pipelines específicas (ex.: `parlamento_deputados`, `radar_congresso`).
  Cada pipeline segue o template `GenericETL`.

- `utils/` \
  Utilitários reutilizáveis:
  - `extractors/` → classes para extração (ex.: `HttpJsonExtractor`).
  - `loaders/` → classes para carregamento em bancos (ex.: `PostgreSQLManager`).
  - `transformers/` → funções genéricas de limpeza e normalização.
  - `pipeline_cfg.py` → contém `PipelineConfig` e `GenericETL`.

## Como funcionam os pipelines
No arquivo `pipeline_cfg.py` temos:
`PipelineConfig` \
É um `dataclass` que organiza os caminhos e parâmetros necessários para rodar a pipeline.
A ideia é inicializar PipelineConfig a partir de um dicionário de configuração:

```
    PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD = {
        "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
        "landing_dir": "./data/raw/radar_congresso/governismo/",
        "landing_file": "radar_governismo_deputados.json",
        "bronze_dir": "./data/bronze/radar_congresso/governismo/",
        "bronze_file": "radar_governismo_deputados.csv",
        "db_table": "radar_governismo_deputados_raw",
    }

    cfg = PipelineConfig(**PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD)
```
`GenericETL` \
Define o contrato de orquestração:
- `extract()` — baixa/salva landing (pode usar extract_fn custom ou o genérico).

- `validate()` — reabre o CSV bronze e valida com Pandera (usa cfg.bronze_filepath).

- `load()` — reabre o CSV bronze e carrega no Postgres (usa cfg.bronze_filepath).

**Importante**: no desenho atual, tudo é dirigido pelo `cfg`.

Fluxo de execução:
```
def run_governismo_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,           # usa extração genérica (GET único -> arquivo)
        load_fn=None,              # usa loader genérico -> Postgres
        validator=GovernismoSchema,
        log=logger,
    )

    etl.extract()                  # gera landing
    transform_governismo(cfg)      # gera bronze (CSV)
    etl.validate()                 # valida bronze
    etl.load()                     # carrega bronze

```

## Como criar uma nova pipeline (passo a passo)
1. Defina a configuração `PipelineConfig`:
```
PIPELINE_DEPUTADOS_PRD = {
    "parameter_file": "./src/params/id_deputados.csv",
    "url_base": "https://dadosabertos.camara.leg.br/api/v2/deputados/",
    "landing_dir": "./data/raw/camara/deputados/",
    "bronze_dir": "./data/bronze/camara/deputados/",
    "error_dir": "./data/error/camara/deputados/",
    "bronze_file": "parlamento_deputados.csv",
    "db_table": "raw_parlamento_deputados",
}
```
2. Implemente a transformação
Ela deve ler cfg.landing_filepath, produzir cfg.bronze_filepath e não retornar nada:
```
def transform_parlamentares(cfg: PipelineConfig) -> Path:
df = pd.read_json(cfg.landing_filepath, dtype=str)  # Path funciona direto
df = ColumnSanitizer(df).sanitize_columns_names().df
df.to_csv(cfg.bronze_filepath, sep=";", index=False)
logger_p.info(f"CSV SALVO EM: {cfg.bronze_filepath}")
```
3. Instanciar GenericETL e rode etl.run().
```
run_parlamentares_pipeline(PipelineConfig(**PIPELINE_PARLAMENTARES_CONFIG_PRD))
```

## Boas práticas
- Separador/encoding: mantenha o mesmo sep no transform e no validate/load (padrão atual: ;).

- Logging:

  - Scripts locais: configure basicConfig no entry-point.

  - Airflow: não chame basicConfig; use logging.getLogger(__name__).

- Idempotência: a transform deve escrever de forma atômica (.tmp + rename) quando possível.

- Schema: versionar Schemas Pandera quando houver mudança de layout.

- Reexecução: como tudo é baseado em cfg.bronze_filepath, dá pra reprocessar apenas validate() ou load() se necessário.
