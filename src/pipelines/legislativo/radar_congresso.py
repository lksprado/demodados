import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import (
    DeputadosRadarSchema,
    GovernismoDeputadoSchema,
)
from src.utils.extractors.https import HttpJsonExtractor
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import ColumnSanitizer

PIPELINE_DEPUTADOS_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/parlamentares",
    "landing_dir": "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/radar_congresso/",
    "landing_file": "radar_deputados.json",
    "bronze_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/radar_congresso/",
    "bronze_file": "radar_deputados.csv",
    "db_table": "radar_deputados_raw ",
}

PIPELINE_GOVERNISMO_CONFIG_PRD = {
    "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
    "landing_dir": "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/radar_congresso/",
    "landing_file": "radar_governismo.json",
    "bronze_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/radar_congresso/",
    "bronze_file": "radar_governismo.csv",
    "db_table": "radar_governismo_raw ",
}


def extract_deputados(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    extractor = HttpJsonExtractor(
        url=config["url_base"],
        output_dir=config["landing_dir"],
        filename_fn=config["landing_file"],
        logger=logger,
    )
    extractor.fetch_and_save()


def transform_deputados(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    filepath = os.path.join(config["landing_dir"], config["landing_file"])
    try:
        df = pd.read_json(filepath, dtype=str)
        df = ColumnSanitizer(df).sanitize_columns_names().df
        file_dest = os.path.join(config["bronze_dir"], config["bronze_file"])
        df.to_csv(file_dest, sep=";", index=False)
        logger.info(f"CSV SALVO EM: {file_dest}")
    except Exception as e:
        logger.error(f"ERRO AO TRANSFORMAR: {filepath} --- {e}")


def load_deputados(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    csv = os.path.join(config["bronze_dir"], config["bronze_file"])
    validate_and_load_to_db(
        csv_file=csv,
        table=config["db_table"],
        schema=DeputadosRadarSchema,
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
    logger = log or logging.getLogger(__name__)

    filepath = os.path.join(config["landing_dir"], config["landing_file"])
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
        schema=GovernismoDeputadoSchema,
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


def pipeline_radar_deputados(extraction=False, transformation=False, load=False):
    logger = logger_setting("pipeline_radar_deputados_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline para carga de radar_deputados_raw")
    if extraction == True:
        extract_deputados(config=PIPELINE_DEPUTADOS_CONFIG_PRD, log=logger)
    if transformation == True:
        transform_deputados(config=PIPELINE_DEPUTADOS_CONFIG_PRD, log=logger)
    if load == True:
        load_deputados(config=PIPELINE_DEPUTADOS_CONFIG_PRD, log=logger)
    logger.info("Finalizado pipeline para carga de radar_deputados_raw")
    logger.info("-" * 100)


def pipeline_radar_governismo(extraction=False, transformation=False, load=False):
    logger = logger_setting("pipeline_radar_governismo_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline para carga de radar_governismo_raw")
    if extraction == True:
        extract_governismo(config=PIPELINE_GOVERNISMO_CONFIG_PRD, log=logger)
    if transformation == True:
        transform_governismo(config=PIPELINE_GOVERNISMO_CONFIG_PRD, log=logger)
    if load == True:
        load_governismo(config=PIPELINE_GOVERNISMO_CONFIG_PRD, log=logger)
    logger.info("Finalizado pipeline para carga de radar_governismo_raw")
    logger.info("-" * 100)


if __name__ == "__main__":
    pipeline_radar_governismo(True, True, True)
