"""
Pipeline Radar Congresso (parlamentares e governismo).
Extrai dados de https://radar.congressoemfoco.com.br/

Fluxo:
1) extract_*: baixa JSON da API do Radar Congresso e salva em data/landing.
2) transform_*: normaliza e salva CSV em data/bronze.
3) load_*: valida com Pandera e insere na tabela Postgres correspondente.

Entradas externas:
- APIs públicas (urls em *_CONFIG_PRD)
Saídas:
- Arquivos: data/landing/.../*.json, data/bronze/.../*.csv
- Tabelas: <schema>.<db_table> (ver configs)

Como rodar (exemplos):
- python radar_congresso.py --pipeline governismo_deputados --extract --transform --load
- python radar_congresso.py --pipeline parlamentares --transform
Requisitos:
- Conexão Postgres configurada em PostgreSQLManager
- Schemas Pandera: ParlamentarRadarSchema, GovernismoSchema
"""

import json
import logging
import os
import re
from typing import Optional

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import GovernismoSchema, ParlamentarRadarSchema
from src.utils.extractors.https import HttpJsonExtractor
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import ColumnSanitizer

PIPELINE_PARLAMENTARES_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/busca-parlamentar",
    "landing_dir": "./data/landing/radar_congresso/parlamentares/",
    "landing_file": "radar_parlamentares.json",
    "bronze_dir": "./data/bronze/radar_congresso/parlamentares/",
    "bronze_file": "radar_parlamentares.csv",
    "db_table": "radar_parlamentares_raw",
}

PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
    "landing_dir": "./data/landing/radar_congresso/governismo/",
    "landing_file": "radar_governismo_deputados.json",
    "bronze_dir": "./data/bronze/radar_congresso/governismo/",
    "bronze_file": "radar_governismo_deputados.csv",
    "db_table": "radar_governismo_deputados_raw",
}

PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=senado",
    "landing_dir": "./data/landing/radar_congresso/governismo/",
    "landing_file": "radar_governismo_senadores.json",
    "bronze_dir": "./data/bronze/radar_congresso/governismo/",
    "bronze_file": "radar_governismo_senadores.csv",
    "db_table": "radar_governismo_senadores_raw",
}


def extract_parlamentares(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    extractor = HttpJsonExtractor(
        url=config["url_base"],
        output_dir=config["landing_dir"],
        filename_fn=config["landing_file"],
        logger=logger,
    )
    extractor.fetch_and_save()


def transform_parlamentares(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    filepath = os.path.join(config["landing_dir"], config["landing_file"])
    if not os.path.exists(config["bronze_dir"]):
        os.makedirs(config["bronze_dir"])

    try:
        df = pd.read_json(filepath, dtype=str)
        df = ColumnSanitizer(df).sanitize_columns_names().df
        file_dest = os.path.join(config["bronze_dir"], config["bronze_file"])
        df.to_csv(file_dest, sep=";", index=False)
        logger.info(f"CSV SALVO EM: {file_dest}")
    except Exception as e:
        logger.error(f"ERRO AO TRANSFORMAR: {filepath} --- {e}")


def load_parlamentares(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    csv = os.path.join(config["bronze_dir"], config["bronze_file"])
    validate_and_load_to_db(
        csv_file=csv,
        table=config["db_table"],
        schema=ParlamentarRadarSchema,
        file=csv,
        logger=logger,
    )


def extract_governismo(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    extractor = HttpJsonExtractor(
        url=config["url_base"],
        output_dir=config["landing_dir"],
        filename_fn=config["landing_file"],
        logger=logger,
    )
    extractor.fetch_and_save()


def transform_governismo(config: dict, log: Optional[logging.Logger] = None):
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
    logger = log or logging.getLogger(__name__)

    filepath = os.path.join(config["landing_dir"], config["landing_file"])

    if not os.path.exists(config["bronze_dir"]):
        os.makedirs(config["bronze_dir"])

    with open(filepath, "r") as f:
        df = json.load(f)
    df = pd.DataFrame(df)

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
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", c)
            if date_match:
                date = date_match.group(0)  # Captura a data
            new_col_name = f"{date}"
            cols_dict[c] = new_col_name
        df_parlamentares = df_parlamentares.rename(columns=cols_dict)
        cols_renamed = list(cols_dict.values())
        df_long = df_parlamentares.melt(
            id_vars=["id", "afavor", "n", "total"],
            value_vars=cols_renamed,
            var_name="trimestre",
            value_name="perc_governismo",
        )
        file_dest = os.path.join(config["bronze_dir"], config["bronze_file"])
        df_long.to_csv(file_dest, sep=";", index=False)
        logger.info(f"CSV SALVO EM: {file_dest}")
    except Exception as e:
        logger.error(f"ERRO AO TRANSFORMAR {filepath} --- {e} ")
        raise


def load_governismo(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    csv = os.path.join(config["bronze_dir"], config["bronze_file"])
    validate_and_load_to_db(
        csv_file=csv,
        table=config["db_table"],
        schema=GovernismoSchema,
        file=csv,
        logger=logger,
    )


def validate_and_load_to_db(csv_file, table, schema, file, logger: logging.Logger):
    df = pd.read_csv(csv_file, sep=";")
    try:
        validated_df = schema.validate(df)
    except SchemaError as e:
        logger.error(f"ERRO DE SCHEMA --- {e}")
    try:
        PostgreSQLManager.send_to_db(
            df=validated_df, table_name=table, filename=file, log=logger
        )
    except Exception as e:
        logger.error(f"ERRO NA CARGA --- {e}")


def pipeline_radar_parlamentares(extraction=False, transformation=False, load=False):
    logger = logger_setting("pipeline_radar_deputados_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extraction == True:
        try:
            extract_parlamentares(config=PIPELINE_PARLAMENTARES_CONFIG_PRD, log=logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transformation == True:
        try:
            transform_parlamentares(
                config=PIPELINE_PARLAMENTARES_CONFIG_PRD, log=logger
            )
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load == True:
        try:
            load_parlamentares(config=PIPELINE_PARLAMENTARES_CONFIG_PRD, log=logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


def pipeline_radar_governismo_deputados(
    extraction=False, transformation=False, load=False
):
    logger = logger_setting("pipeline_radar_governismo_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extraction == True:
        try:
            extract_governismo(
                config=PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD, log=logger
            )
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transformation == True:
        try:
            transform_governismo(
                config=PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD, log=logger
            )
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load == True:
        try:
            load_governismo(config=PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD, log=logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


def pipeline_radar_governismo_senadores(
    extraction=False, transformation=False, load=False
):
    logger = logger_setting("pipeline_radar_governismo_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extraction == True:
        try:
            extract_governismo(
                config=PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD, log=logger
            )
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transformation == True:
        try:
            transform_governismo(
                config=PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD, log=logger
            )
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load == True:
        try:
            load_governismo(config=PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD, log=logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


if __name__ == "__main__":
    pipeline_radar_governismo_deputados(True, True, True)
    pipeline_radar_governismo_senadores(True, True, True)
    pipeline_radar_parlamentares(True, True, True)

    pass
