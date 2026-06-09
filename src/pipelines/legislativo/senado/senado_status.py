"""
Pipeline — Status de Proposições (Senado Federal)

Extrai, transforma e carrega dados de tramitação de proposições com mais de 5000 votos
na plataforma e-Cidadania, a partir da API:
https://legis.senado.leg.br/dadosabertos/processo?sigla={sigla}&numero={numero}&ano={ano}&v=1

Fluxo:
1) extract_status(cfg): lê o CSV de parâmetros (e-Cidadania paginas), filtra proposições
   com >= 5000 votos e baixa um JSON por proposição em data/landing.
2) transform_status(cfg): consolida os JSONs de landing em bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- PipelineConfig com: url_base, landing_dir, bronze_dir, bronze_file, db_table, parameter_file.
- Parâmetro de entrada: CSV de saída do pipeline ecidadania_paginas (coluna total_votos).

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_senado_status")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"


def extract_status(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    parameter_df = pd.read_csv(cfg.parameter_filepath, sep=";")
    parameter_df = parameter_df.loc[
        parameter_df["total_votos"] >= 5000, ["sigla", "numero", "ano"]
    ].drop_duplicates()

    extractor = HttpJsonExtractor(logger)
    for _, row in parameter_df.iterrows():
        sigla = row["sigla"]
        numero = int(row["numero"])
        ano = int(row["ano"])

        filename = f"status_{sigla}_{numero}_{ano}.json"
        url = f"{cfg.url_base}?sigla={sigla}&numero={numero}&ano={ano}&v=1"

        data = extractor.make_http_request(url=url)
        if data:
            extractor.save_response(data, cfg.landing_dir, filename)

    logger.info(f"Extracao Completa em {cfg.landing_dir}")


def transform_status(cfg: PipelineConfig) -> Path:
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = pd.read_json(f)
            if not data.empty:
                df = ColumnSanitizer(data).sanitize_columns_names().df
                dataframes.append(df)
        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_status,
        load_fn=None,
        validator=None,
        log=logger,
    )

    etl.extract()
    transform_status(cfg)
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="status", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_status
