"""
Pipeline — Radar Congresso (Parlamentares)

Extrai, transforma, valida e carrega a listagem de parlamentares a partir da API:
https://radar.congressoemfoco.com.br/api/busca-parlamentar

Fluxo:
1) etl.extract(): baixa o JSON em data/landing.
2) transform(cfg): normaliza o JSON, sanitiza colunas e salva CSV em data/bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

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

from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("raw_radar_parlamentares")

_CONFIG_FILE = Path(__file__).parent / "radar_congresso_config.yml"


def transform(cfg: PipelineConfig) -> Path:
    df = pd.read_json(cfg.landing_filepath, dtype=str)
    df = ColumnSanitizer(df).sanitize_columns_names().df

    df.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"💾 CSV salvo em: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        log=logger,
    )

    etl.extract()
    transform(cfg)
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="parlamentares", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.radar_congresso.radar_parlamentares
