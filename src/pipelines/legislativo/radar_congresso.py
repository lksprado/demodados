import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

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

## URLS
URL_GOVERNISMO = "https://radar.congressoemfoco.com.br/api/governismo?casa=camara"
URL_DEPUTADOS = "https://radar.congressoemfoco.com.br/api/parlamentares"

## PASTAS
PASTA_LANDING_RADAR_CONGRESSO = (
    "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/radar_congresso/"
)
PASTA_BRONZE_RADAR_CONGRESSO = (
    "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/radar_congresso/"
)

## ARQUIVOS
ARQUIVO_JSON_DEPUTADOS = "radar_deputados.json"
ARQUIVO_JSON_GOVERNISMO = "radar_governismo_deputados.json"
ARQUIVO_CSV_DEPUTADOS = "radar_deputados.csv"
ARQUIVO_CSV_GOVERNISMO = "radar_governismo_deputados.csv"

## TABELAS DB
TABELA_RADARCONGRESSO_DEPUTADOS_RAW = "radar_deputados_raw"
TABELA_RADARCONGRESSO_GOVERNISMO_RAW = "radar_governismo_raw"


def extract_deputados():
    logger = logger_setting("pipeline_radar_deputados_raw")
    extractor = HttpJsonExtractor(
        url=URL_DEPUTADOS,
        output_dir=PASTA_LANDING_RADAR_CONGRESSO,
        filename_fn=lambda _: ARQUIVO_JSON_DEPUTADOS,
        logger=logger,
    )
    extractor.fetch_and_save()


def transform_deputados():
    logger = logger_setting("pipeline_radar_deputados_raw")
    filepath = os.path.join(PASTA_LANDING_RADAR_CONGRESSO, ARQUIVO_JSON_DEPUTADOS)
    try:
        df = pd.read_json(filepath, dtype=str)
        df = ColumnSanitizer(df).sanitize_columns_names().df
        file_dest = os.path.join(PASTA_BRONZE_RADAR_CONGRESSO, ARQUIVO_CSV_DEPUTADOS)
        df.to_csv(file_dest, sep=";", index=False)
        logger.info(f"CSV salvo em: {file_dest}")
    except Exception as e:
        logger.error(f"Erro ao transformar {filepath} --- {e}")


def load_deputados():
    logger = logger_setting("pipeline_radar_deputados_raw")
    csv = os.path.join(PASTA_BRONZE_RADAR_CONGRESSO, ARQUIVO_CSV_DEPUTADOS)
    validate_and_load_to_db(
        csv_file=csv,
        table=TABELA_RADARCONGRESSO_DEPUTADOS_RAW,
        schema=DeputadosRadarSchema,
        file=csv,
        logger=logger,
    )


def extract_governismo():
    logger = logger_setting("pipeline_radar_governismo_raw")
    extractor = HttpJsonExtractor(
        url=URL_GOVERNISMO,
        output_dir=PASTA_LANDING_RADAR_CONGRESSO,
        filename_fn=lambda _: ARQUIVO_JSON_GOVERNISMO,
        logger=logger,
    )
    extractor.fetch_and_save()


def transform_governismo():
    logger = logger_setting("pipeline_radar_governismo_raw")
    filepath = os.path.join(PASTA_LANDING_RADAR_CONGRESSO, ARQUIVO_JSON_GOVERNISMO)
    with open(filepath, "r") as f:
        df = json.load(f)
    df = pd.DataFrame(df)

    if df.empty:
        logger.warning("Dataframe vazio!")

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
        file_dest = os.path.join(PASTA_BRONZE_RADAR_CONGRESSO, ARQUIVO_CSV_GOVERNISMO)
        df_long.to_csv(file_dest, sep=";", index=False)
        logger.info(f"CSV salvo em: {file_dest}")
    except Exception as e:
        logger.error(f"Erro ao transformar {filepath} --- {e} ")
        raise


def load_governismo():
    logger = logger_setting("pipeline_radar_governismo_raw")
    csv = os.path.join(PASTA_BRONZE_RADAR_CONGRESSO, ARQUIVO_CSV_GOVERNISMO)
    validate_and_load_to_db(
        csv_file=csv,
        table=TABELA_RADARCONGRESSO_GOVERNISMO_RAW,
        schema=GovernismoDeputadoSchema,
        file=csv,
        logger=logger,
    )


def validate_and_load_to_db(csv_file, table, schema, file, logger: logging.Logger):
    df = pd.read_csv(csv_file, sep=";")
    try:
        validated_df = schema.validate(df)
    except SchemaError as e:
        logger.error(f"Erro de schema --- {e}")
    try:
        PostgreSQLManager.send_to_db(
            df=validated_df, table_name=table, filename=file, log=logger
        )
    except Exception as e:
        logger.error(f"Erro na carga --- {e}")


def pipeline_radar_deputados(extraction=False, transformation=False, load=False):
    logger = logger_setting("pipeline_radar_deputados_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline para carga de radar_deputados_raw")
    if extraction == True:
        extract_deputados()
    if transformation == True:
        transform_deputados()
    if load == True:
        load_deputados()
    logger.info("Finalizado pipeline para carga de radar_deputados_raw")
    logger.info("-" * 100)


def pipeline_radar_governismo(extraction=False, transformation=False, load=False):
    logger = logger_setting("pipeline_radar_governismo_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline para carga de radar_governismo_raw")
    if extraction == True:
        extract_governismo()
    if transformation == True:
        transform_governismo()
    if load == True:
        load_governismo()
    logger.info("Finalizado pipeline para carga de radar_governismo_raw")
    logger.info("-" * 100)


if __name__ == "__main__":
    pipeline_radar_governismo(True, True, True)
    pass
