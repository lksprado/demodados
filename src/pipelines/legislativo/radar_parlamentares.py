"""
Pipeline — Radar Congresso (Parlamentares)

Extrai, transforma, valida e carrega a listagem de parlamentares a partir da API:
https://radar.congressoemfoco.com.br/api/busca-parlamentar

Fluxo:
1) extract (opcional neste run): baixa o JSON em data/landing.
2) transform_parlamentares(cfg): normaliza o JSON, sanitiza colunas e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (ParlamentarRadarSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: ParlamentarRadarSchema.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import logging
from pathlib import Path

import pandas as pd

from src.pipelines.legislativo.schema import ParlamentarRadarSchema
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger_p = logging.getLogger("Pipeline: radar_parlamentares_raw")


def transform_parlamentares(cfg: PipelineConfig) -> Path:
    df = pd.read_json(cfg.landing_filepath, dtype=str)  # Path funciona direto
    df = ColumnSanitizer(df).sanitize_columns_names().df

    df.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger_p.info(f"CSV SALVO EM: {cfg.bronze_filepath}")


def run_parlamentares_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        validator=ParlamentarRadarSchema,
        log=logger_p,
    )

    etl.extract()
    transform_parlamentares(cfg)
    etl.validate()
    etl.load()


if __name__ == "__main__":
    PIPELINE_PARLAMENTARES_CONFIG_PRD = {
        "url_base": "https://radar.congressoemfoco.com.br/api/busca-parlamentar",
        "landing_dir": "./data/raw/radar_congresso/parlamentares/",
        "landing_file": "radar_parlamentares.json",
        "bronze_dir": "./data/bronze/radar_congresso/parlamentares/",
        "bronze_file": "radar_parlamentares.csv",
        "db_table": "raw_radar_parlamentares",
    }

    run_parlamentares_pipeline(PipelineConfig(**PIPELINE_PARLAMENTARES_CONFIG_PRD))
