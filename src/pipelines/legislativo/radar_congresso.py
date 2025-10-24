"""
Pipeline Radar Congresso (parlamentares e governismo).
Extrai dados de https://radar.congressoemfoco.com.br/

Fluxo:
1) extract_*: baixa JSON da API do Radar Congresso e salva em data/landing.
2) transform_*: normaliza e salva CSV em data/bronze.
3) load_*: valida com Pandera e insere na tabela Postgres correspondente.

Requisitos:
- Conexão Postgres configurada em PostgreSQLManager
- Schemas Pandera: ParlamentarRadarSchema, GovernismoSchema
"""

import json
import logging
import re

import pandas as pd

from src.pipelines.legislativo.schema import GovernismoSchema, ParlamentarRadarSchema
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer

logger_p = logging.getLogger("Pipeline: radar_parlamentares_raw")


def transform_parlamentares(df, cfg: PipelineConfig):
    df = pd.read_json(cfg.landing_filepath, dtype=str)  # Path funciona direto
    df = ColumnSanitizer(df).sanitize_columns_names().df

    df.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger_p.info(f"CSV SALVO EM: {cfg.bronze_filepath}")
    return df


def run_parlamentares_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        transform_fn=transform_parlamentares,
        load_fn=None,
        validator=ParlamentarRadarSchema,
        log=logger_p,
    )

    etl.extract()
    df = etl.transform()
    df = etl.validate(df)
    etl.load(df)


################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################
################################################################################################################################################################################


logger_g = logging.getLogger("Pipeline: radar_governismo_raw")


def transform_governismo(df, cfg: PipelineConfig):
    """
    Transforma JSON de governismo em formato tabular "long" e salva CSV.

    Args:
        config: dicionário com chaves obrigatórias:
            - "landing_dir", "landing_file", "bronze_dir", "bronze_file"
        log: logger opcional.

    Produz:
        CSV em {bronze_dir}/{bronze_file} com colunas:
        [id, afavor, n, total, trimestre, perc_governismo]

    Raises:
        Exception: Repassa exceções não tratadas de IO / parsing em caso crítico.
    """
    with cfg.landing_filepath.open("r") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)

    if df.empty:
        logger_g.warning("DATAFRAME VAZIO!")

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
                date = date_match.group(0)  # Captura a data
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
        logger_g.info(f"CSV SALVO EM: {cfg.bronze_filepath}")
    except Exception as e:
        logger_g.error(f"ERRO AO TRANSFORMAR {cfg.landing_file} --- {e} ")
        raise
    return df_long


def run_governismo_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        transform_fn=transform_governismo,
        load_fn=None,
        validator=GovernismoSchema,
        log=logger_g,
    )

    etl.extract()
    df = etl.transform()
    df = etl.validate(df)
    etl.load(df)


if __name__ == "__main__":
    PIPELINE_PARLAMENTARES_CONFIG_PRD = {
        "url_base": "https://radar.congressoemfoco.com.br/api/busca-parlamentar",
        "landing_dir": "./data/raw/radar_congresso/parlamentares/",
        "landing_file": "radar_parlamentares.json",
        "bronze_dir": "./data/bronze/radar_congresso/parlamentares/",
        "bronze_file": "radar_parlamentares.csv",
        "db_table": "radar_parlamentares_raw",
    }

    PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD = {
        "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
        "landing_dir": "./data/raw/radar_congresso/governismo/",
        "landing_file": "radar_governismo_deputados.json",
        "bronze_dir": "./data/bronze/radar_congresso/governismo/",
        "bronze_file": "radar_governismo_deputados.csv",
        "db_table": "radar_governismo_deputados_raw",
    }

    PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD = {
        "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=senado",
        "landing_dir": "./data/raw/radar_congresso/governismo/",
        "landing_file": "radar_governismo_senadores.json",
        "bronze_dir": "./data/bronze/radar_congresso/governismo/",
        "bronze_file": "radar_governismo_senadores.csv",
        "db_table": "radar_governismo_senadores_raw",
    }

    run_parlamentares_pipeline(PipelineConfig(**PIPELINE_PARLAMENTARES_CONFIG_PRD))
    run_governismo_pipeline(PipelineConfig(**PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD))
    run_governismo_pipeline(PipelineConfig(**PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD))
