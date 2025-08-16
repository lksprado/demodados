import logging
import os
import re
import shutil
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import DeputadoSchema
from src.utils.extractors.https import make_http_request, response_to_json
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import ColumnSanitizer
from src.utils.transformers.json_parsers import (
    make_df_from_json_list,
    normalize_json_object,
)

PIPELINE_CONFIG_PRD = {
    "parameter_file": "src/params/id_deputados.csv",
    "url_base": "https://dadosabertos.camara.leg.br/api/v2/deputados/",
    "landing_dir": "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/deputados_detalhes/",
    "bronze_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/deputados_detalhes/",
    "error_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/erros/",
    "db_table": "parlamento_deputados_raw",
}

PIPELINE_CONFIG_TEST = {
    "parameter_file": "src/params/id_deputados.csv",
    "url_base": "https://dadosabertos.camara.leg.br/api/v2/deputados/",
    "landing_dir": "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/teste/",
    "bronze_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/teste/",
    "error_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/erros/",
    "db_table": "tst_parlamento_deputados_raw",
}

cols_to_not_sanitize_values = [
    "uri",
    "urlwebsite",
    "redesocial",
    "datanascimento",
    "datafalecimento",
    "ultimostatus_uri",
    "ultimostatus_uripartido",
    "ultimostatus_urlfoto",
    "ultimostatus_email",
    "ultimostatus_data",
    "ultimostatus_gabinete_telefone",
    "ultimostatus_gabinete_email",
    "arquivo_origem",
    "data_carga",
]


URL_DEPUTADOS_ATUAIS = (
    "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome"
)
PASTA_PARAMETROS = "src/params/"
ARQUIVO_IDS_JSON = os.path.join(PASTA_PARAMETROS, "id_deputados.json")
ARQUIVO_IDS_CSV = os.path.join(PASTA_PARAMETROS, "id_deputados.csv")


def obter_ids_deputados_atuais():
    """Funcao auxiliar para obter todos deputados atuais"""
    data = make_http_request(URL_DEPUTADOS_ATUAIS)
    response_to_json(data, PASTA_PARAMETROS, "id_deputados.json")
    df_ids = make_df_from_json_list(ARQUIVO_IDS_JSON)
    df_ids.to_csv(ARQUIVO_IDS_CSV, sep=";")


def extract_deputado(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    source_file = Path(config["parameter_file"])
    output_dir = Path(config["landing_dir"])
    url_base = str(config["url_base"])

    try:
        df_ids = pd.read_csv(source_file, sep=";")
        id_list = df_ids["id"].to_list()
    except Exception as e:
        logger.error(f"ERRO AO LER ARQUIVO DE IDS: {source_file}")
        raise

    for id in id_list:
        try:
            url = f"{url_base}{id}"
            data = make_http_request(url, log=logger)

            if data:
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = f"{id}_deputado.json"
                response_to_json(data, output_dir, output_file)
            else:
                logger.warning(f"RESPOSTA VAZIA: {url}")
        except Exception as e:
            logger.error(f"ERRO REQUISICAO {url} --- {e}")


def transform_deputado(
    config: dict, cols_to_sanitize: list, log: Optional[logging.Logger] = None
):
    logger = log or logging.getLogger(__name__)
    input_dir = Path(config["landing_dir"])
    output_dir = Path(config["bronze_dir"])

    for f in input_dir.iterdir():
        try:
            dep_id = re.match(r"^\d+", f.name).group()
            data = normalize_json_object(f, "dados")
            if not data.empty:
                df = (
                    ColumnSanitizer(data)
                    .sanitize_columns_names()
                    .not_sanitize_columns_values(cols=cols_to_sanitize)
                    .df
                )
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = f"{dep_id}_deputado.csv"

                file_destination = output_dir / output_file
                df.to_csv(
                    file_destination,
                    sep=";",
                    index=False,
                )
                logger.info(f"CSV SALVO EM: {file_destination}")
            else:
                logger.warning(f"JSON VAZIO:{f}")
        except Exception as e:
            logger.error(f"ERRO AO TRANSFORMAR: {f} --- {e}")


def validate_load_to_db(config: dict, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    input_dir = Path(config["bronze_dir"])
    error_dir = Path(config["error_dir"])
    db_table_name = str(config["db_table"])

    ## VALIDACAO E CARGA
    for f in input_dir.iterdir():
        try:
            df = pd.read_csv(f, sep=";")
            try:
                validated_df = DeputadoSchema.validate(df)
                PostgreSQLManager.send_to_db(
                    df=validated_df,
                    table_name=db_table_name,
                    how="append",
                    filename=f.name,
                    log=logger,
                )
            except SchemaError as e:
                os.makedirs(error_dir, exist_ok=True)
                shutil.copy(f, error_dir / f.name)
                logger.error(f"ERRO VALIDACAO SCHEMA: {f.name} --- {e}")
                logger.info(f"COPIANDO {f.name} PARA {error_dir}")
        except Exception as e:
            logger.error(f"ERRO AO PROCESSAR: {f.name} --- {e}")
