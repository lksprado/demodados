"""
Pipeline — Radar Congresso (Governismo)

Extrai, transforma, valida e carrega dados de governismo a partir da API:
https://radar.congressoemfoco.com.br/api/governismo?casa={camara|senado}

Fluxo:
1) etl.extract(): baixa o JSON em data/landing.
2) transform_governismo(cfg): normaliza o JSON, converte wide→long e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (GovernismoSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: GovernismoSchema.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import json
import logging
import re
from pathlib import Path

import pandas as pd

from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ..schema import GovernismoSchema

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_radar_governismo")

_CONFIG_FILE = Path(__file__).parent / "radar_congresso_config.yml"


def transform_governismo(cfg: PipelineConfig) -> Path:
    with cfg.landing_filepath.open("r") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)

    if df.empty:
        logger.warning("DATAFRAME VAZIO!")

    try:
        parlamentares = df["parlamentares"]
        df_parlamentares = pd.json_normalize(parlamentares)
        df_parlamentares = df_parlamentares.dropna(subset=["id"]).reset_index()
        cols_to_keep = [
            col
            for col in df_parlamentares.columns
            if "trimestral" not in col or "total" in col
        ]
        df_parlamentares = df_parlamentares[cols_to_keep]
        cols_to_rename = [
            col for col in df_parlamentares.columns if "trimestral" in col
        ]
        cols_dict = {}
        for c in cols_to_rename:
            # Extrai "YYYY-MM-DD" do nome da coluna trimestral vinda da API
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", c)
            if date_match:
                date = date_match.group(0)
            new_col_name = f"{date}"
            cols_dict[c] = new_col_name
        df_parlamentares = df_parlamentares.rename(columns=cols_dict)
        cols_renamed = list(cols_dict.values())

        # Converte wide->long
        df_long = df_parlamentares.melt(
            id_vars=["id", "afavor", "n", "total"],
            value_vars=cols_renamed,
            var_name="trimestre",
            value_name="perc_governismo",
        )

        df_long.to_csv(cfg.bronze_filepath, sep=";", index=False)
        logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")
    except Exception as e:
        logger.error(f"ERRO AO TRANSFORMAR {cfg.landing_file} --- {e} ")
        raise


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        validator=GovernismoSchema,
        log=logger,
    )

    etl.extract()
    transform_governismo(cfg)
    etl.validate()
    etl.load()


if __name__ == "__main__":
    config_dep = load_source_config(
        _CONFIG_FILE, source="governismo_deputados", env="local"
    )
    config_sen = load_source_config(
        _CONFIG_FILE, source="governismo_senadores", env="local"
    )
    run_pipeline(PipelineConfig(**config_dep))
    run_pipeline(PipelineConfig(**config_sen))
    # python -m src.pipelines.legislativo.radar_congresso.radar_governismo
