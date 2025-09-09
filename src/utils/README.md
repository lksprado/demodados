# utils/

Contém utilitários genéricos usados em múltiplas pipelines:

- **extractors/** → módulos para requisições HTTP e leitura de dados.
- **loaders/** → módulos para envio de dados a bancos relacionais.
- **transformers/** → normalização e limpeza de dados.
- **pipeline_cfg.py** → classe base `GenericETL` e `PipelineConfig`, que servem como template para todas as pipelines.
