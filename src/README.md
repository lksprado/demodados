# Estrutura do diretório `src/`

Este diretório contém o código-fonte dos pipelines de ingestão de dados.
Abaixo descrevemos a organização e, principalmente, como implementar uma nova pipeline.

## Estrutura de pastas

- **params/**
  Arquivos de parâmetros (ex.: CSVs de IDs, configs auxiliares) usados para executar requisições.

- **pipelines/**
  Implementações das pipelines específicas (ex.: `parlamento_deputados`, `radar_congresso`).
  Cada pipeline segue o template `GenericETL`.

- **utils/**
  Utilitários reutilizáveis:
  - `extractors/` → classes para extração (ex.: `HttpJsonExtractor`).
  - `loaders/` → classes para carregamento em bancos (ex.: `PostgreSQLManager`).
  - `transformers/` → funções genéricas de limpeza e normalização.
  - `pipeline_cfg.py` → contém `PipelineConfig` e `GenericETL`.

## Como funciona a implementação dos pipelines
NO arquivo `pipeline_cfg.py` temos:
- `PipelineConfig`
É um **dataclass** que organiza os caminhos e parâmetros necessários para rodar a pipeline.
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
- `GenericETL`
Define o **contrato de ETL**: extract, transform, validate, load.
Cada etapa pode receber uma função helper específica, ou usar a versão genérica (fallback) já implementada.
Isso garante flexibilidade: pipelines simples usam só precisam desenvolver a etapa de transform, pipelines complexas podem customizar todas as etapas.

Exemplo de lógica execução:
```
def run_governismo_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        transform_fn=transform_governismo,
        validate_fn=None,
        load_fn=None,
        validator=GovernismoSchema,
        log=logger_g,
    )
## Execucao
  etl.extract()
  df = etl.transform()
  df = etl.validate(df)
  etl.load(df)
```
➡️ Nesse exemplo:
`extract` usa o método genérico (baixa 1 URL e salva em 1 arquivo).\
`transform` é customizado.\
`validate` usa o schema Pandera se informado.\
`load` usa o carregador genérico para Postgres.

Assim, implementar uma nova pipeline significa apenas:
1. Criar um dicionário de configuração.
2. Definir a função de transformação (mínimo necessário).
3. Instanciar GenericETL e chamar etl.run().
