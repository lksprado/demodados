import os
import re
import shutil
from pathlib import Path
from typing import Callable

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import DeputadoSchema
from src.utils.extractors.https import make_http_request, response_to_json
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import TheEqualizer
from src.utils.transformers.json_parsers import (
    make_df_from_json_list,
    normalize_json_object,
)

## urls
URL_DEPUTADOS_ATUAIS = (
    "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome"
)
URL_DEPUTADO = "https://dadosabertos.camara.leg.br/api/v2/deputados/"

## pastas
PASTA_PARAMETROS = "src/params/"
PASTA_LANDING_DEPUTADOS = "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/deputados_detalhes/"
PASTA_BRONZE_DEPUTADOS = (
    "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/deputados_detalhes/"
)
PASTA_ERROS = "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/erros/"

## arquivos
ARQUIVO_IDS_JSON = os.path.join(PASTA_PARAMETROS, "id_deputados.json")
ARQUIVO_IDS_CSV = os.path.join(PASTA_PARAMETROS, "id_deputados.csv")


def obter_ids_deputados_atuais():
    data = make_http_request(URL_DEPUTADOS_ATUAIS)
    response_to_json(data, PASTA_PARAMETROS, "id_deputados.json")
    df_ids = make_df_from_json_list(ARQUIVO_IDS_JSON)
    df_ids.to_csv(ARQUIVO_IDS_CSV, sep=";")


def make_request_from_ids(
    source_file: Path,
    url_base: Callable[[str], str],
    output_dir: Path,
    filename: Callable[[str], str],
    logger,
):
    """Le arquivo onde estao IDs para criar URL de requisicao"""
    output_dir = Path(output_dir)

    try:
        df_ids = pd.read_csv(source_file, sep=";")
        id_list = df_ids["id"].to_list()
    except Exception as e:
        logger.error(f"❌ Erro ao ler arquivo de IDs: {source_file}")
        return

    for id in id_list:
        try:
            url = url_base(id)
            data = make_http_request(url)

            if data:
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = filename(id)
                response_to_json(data, output_dir, output_file)
            else:
                logger.warning(f"Sem resposta da API na url: {url}")
        except Exception as e:
            logger.error(f"❌ Erro ao processar: {url} --- {e}")


def transform_json_to_csv(
    input_dir: Path, output_dir: Path, filename: Callable[[str], str], logger
):
    input_dir = Path(input_dir)
    for f in input_dir.iterdir():
        try:
            dep_id = re.match(r"^\d+", f).group()
            data = normalize_json_object(f, "dados")
            if not data.empty:
                os.makedirs(output_dir, exist_ok=True)
                output_file = filename(dep_id)
                file_destination = os.path.join(output_dir, output_file)
                data.to_csv(
                    file_destination,
                    sep=";",
                    index=False,
                )
            else:
                logger.warning(f"Objeto json retornou vazio:{f}")
        except Exception as e:
            logger.error(f"❌ Erro ao processar: {f} --- {e}")


def validate_load_to_db(
    input_dir: Path, db_table_name: str, cols_sanitize, logger, error_folder
):
    input_dir = Path(input_dir)
    error_folder = Path(error_folder)
    ## VALIDACAO E CARGA
    for f in input_dir.iterdir():
        try:
            df = pd.read_csv(f, sep=";")
            df = (
                TheEqualizer(df)
                .sanitize_columns_names()
                .not_sanitize_columns_values(cols=cols_sanitize)
                .df
            )
            try:
                validated_df = DeputadoSchema.validate(df)
                PostgreSQLManager.send_to_db(
                    df=validated_df, table_name=db_table_name, filename=f
                )
            except SchemaError as e:
                os.makedirs(error_folder, exist_ok=True)
                shutil.move(f, error_folder)
                logger.error(f"❌ Erro ao validar: {f} --- {e}")
        except Exception as e:
            logger.error(f"❌ Erro ao processar: {f} --- {e}")


def pipeline_parlamento_deputados_raw(extraction=False, transforme=False, load=False):
    """
    1. Le arquivo com ID deputados
    2. Faz requisicao http e salva json
    3. Parseia json e salva em csv
    4. Le csv faz limpeza basica
    5. Valida dataframe conforme schema
    6. Carga na camada broze como tabela raw
    """
    logger = logger_setting("pipeline_parlamento_deputados_raw")

    if extraction == True:
        url = lambda id: f"{URL_DEPUTADO}{id}"
        file = lambda id: f"{id}_deputado.json"
        make_request_from_ids(
            ARQUIVO_IDS_CSV, url, PASTA_LANDING_DEPUTADOS, filename=file, logger=logger
        )

    if transforme == True:
        file = lambda id: f"{id}_deputado.csv"
        transform_json_to_csv(
            PASTA_LANDING_DEPUTADOS,
            PASTA_BRONZE_DEPUTADOS,
            filename=file,
            logger=logger,
        )

    if load == True:
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
        validate_load_to_db(
            input_dir=PASTA_BRONZE_DEPUTADOS,
            db_table_name="parlamento_deputados_raw",
            cols_sanitize=cols_to_not_sanitize_values,
            error_folder=PASTA_ERROS,
            logger=logger,
        )

    logger.info("Concluido Pipeline de Deputados na Camara")
    logger.info("-" * 75)


if __name__ == "__main__":
    pipeline_parlamento_deputados_raw(load=True)
