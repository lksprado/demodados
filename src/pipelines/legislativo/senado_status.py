import logging
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup as bs

from ...utils.extractors.https import HttpJsonExtractor
from ...utils.pipeline_cfg import GenericETL, PipelineConfig
from ...utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: senado_status_raw")


def extraction_status(cfg: dict):
    logger.info(f"Extraindo pagina...")
    parameter_df = pd.read_csv(cfg.parameter_file, sep=";")
    parameter_df = parameter_df.loc[
        parameter_df["total_votos"] >= 5000, ["sigla", "numero", "ano"]
    ].drop_duplicates()

    extractor = HttpJsonExtractor(logger)
    for _, row in parameter_df.iterrows():
        sigla = row["sigla"]
        numero = int(row["numero"])
        ano = int(row["ano"])

        filename = f"status_{sigla}_{numero}_{ano}.json"
        landing_file = cfg.landing_dir / filename
        url = f"https://legis.senado.leg.br/dadosabertos/processo?sigla={sigla}&numero={numero}&ano={ano}&v=1"

        data = extractor.make_http_request(url=url)
        if data:
            extractor.save_response(data, cfg.landing_dir, filename)
    logger.info(f"Extracao Completa em {landing_file}")


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


def run_ecidadania_status_pipeline(cfg: dict):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extraction_status,
        load_fn=None,
        validator=None,
        log=logger,
    )

    etl.extract()
    transform_status(cfg)
    etl.load()


if __name__ == "__main__":
    PIPELINE_SENADO_STATUS_CONFIG_PRD = {
        "landing_dir": "./data/raw/senado/status",
        # "landing_file": None,
        "bronze_dir": "./data/bronze/senado/status",
        "bronze_file": "senado_status_consolidado.csv",
        "db_table": "raw_senado_status",
        "parameter_file": "./data/bronze/senado/ecidadania/paginas/ecidadania_paginas_consolidado.csv",
    }

    run_ecidadania_status_pipeline(PipelineConfig(**PIPELINE_SENADO_STATUS_CONFIG_PRD))
