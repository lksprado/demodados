import os
import re
from datetime import datetime

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.camara_deputados.schema import DeputadoSchema
from src.utils.extractors.https import make_http_request, response_to_json
from src.utils.loaders.postgres import send_to_db
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
PASTA_LANDING_DEPUTADOS = "data/landing/camara/deputados_detalhes/"
PASTA_BRONZE_DEPUTADOS = "data/bronze/camara/deputados_detalhes/"

## arquivos
ARQUIVO_IDS_JSON = os.path.join(PASTA_PARAMETROS, "id_deputados.json")
ARQUIVO_IDS_CSV = os.path.join(PASTA_PARAMETROS, "id_deputados.csv")


def obter_ids_deputados_atuais():
    data = make_http_request(URL_DEPUTADOS_ATUAIS)
    response_to_json(data, PASTA_PARAMETROS, "id_deputados.json")
    df_ids = make_df_from_json_list(ARQUIVO_IDS_JSON)
    df_ids.to_csv(ARQUIVO_IDS_CSV, sep=";")


def pipeline_camara_deputados_raw():
    """
    1. Le arquivo com ID deputados
    2. Faz requisicao http e salva json
    3. Parseia json e salva em csv
    4. Le csv faz limpeza basica
    5. Valida dataframe conforme schema
    6. Carga na camada broze como tabela raw
    """
    logger = logger_setting("pipeline_camara__deputados_raw")

    logger.info("Iniciando Pipeline de Deputados na Camara")

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

    ## EXTRACAO
    try:
        df_ids = pd.read_csv(ARQUIVO_IDS_CSV, sep=";")
        id_list = df_ids["id"].to_list()
    except Exception as e:
        logger.error(f"❌ Erro ao ler arquivo de IDs: {ARQUIVO_IDS_CSV}")
        logger.exception(e)
        return

    for id in id_list:
        try:
            url = f"{URL_DEPUTADO}{id}"
            data = make_http_request(url)

            if data:
                response_to_json(data, PASTA_LANDING_DEPUTADOS, f"{id}_deputado.json")

                logger.info(f"✅ Dados salvos para deputado ID: {id}")
            else:
                logger.warning(f"⚠️ Nenhuma resposta da API para ID: {id}")
        except Exception as e:
            logger.error(f"❌ Falha ao processar ID: {id}")
            logger.exception(e)

    ## TRANSFORMACAO
    files = os.listdir(PASTA_LANDING_DEPUTADOS)

    for dep in files:
        dep_id = re.match(r"^\d+", dep).group()
        file_path = os.path.join(PASTA_LANDING_DEPUTADOS, dep)
        data = normalize_json_object(file_path, "dados")
        data.to_csv(
            f"{PASTA_BRONZE_DEPUTADOS}{dep_id}_deputado.csv",
            sep=";",
            index=False,
        )

    ## VALIDACAO E CARGA
    for f in os.listdir(PASTA_BRONZE_DEPUTADOS):
        filename = os.path.join(PASTA_BRONZE_DEPUTADOS, f)
        try:
            df = pd.read_csv(filename, sep=";")
            df = (
                TheEqualizer(df)
                .sanitize_columns_names()
                .not_sanitize_columns_values(cols=cols_to_not_sanitize_values)
                .df
            )

            validated_df = DeputadoSchema.validate(df)
            send_to_db(validated_df, "camara__deputados_raw", f)
            logger.info(f"Arquivo validado e enviado: {f}")

        except SchemaError as e:
            logger.error(f"❌ Validação falhou para: {f}")
            logger.exception(e)

        except Exception as e:
            logger.error(f"❌ Erro inesperado ao processar: {f}")
            logger.exception(e)

    logger.info("Concluido Pipeline de Deputados na Camara")
    logger.info("-" * 75)


if __name__ == "__main__":
    pipeline_camara_deputados_raw()
