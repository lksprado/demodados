import csv
import json
import os
import re
from datetime import datetime

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.camara_deputados.schema import (
    DeputadosRadarSchema,
    GovernismoDeputadoSchema,
)
from src.utils.extractors.https import make_http_request, response_to_json
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import TheEqualizer
from src.utils.transformers.json_parsers import (
    make_df_from_json_list,
    normalize_json_object,
)

URL_GOVERNISMO = "https://radar.congressoemfoco.com.br/api/governismo?casa=camara"
# URL BASE EXEMPLO PARA DETALHES DOS DEPUTADOS
# https://radar.congressoemfoco.com.br/api/parlamentares/1204526/info
URL_DEPUTADOS_BASE = "https://radar.congressoemfoco.com.br/api/parlamentares"

PASTA_LANDING_RADAR_CONGRESSO = "data/landing/camara/radar_congresso/"
PASTA_LANDING_RADAR_CONGRESSO_DEPUTADOS = (
    "data/landing/camara/radar_congresso/radar_detalhes_deputados/"
)
PASTA_BRONZE_RADAR_CONGRESSO = "data/bronze/camara/radar_congresso/"

ARQUIVO_GOVERNISMO_JSON = os.path.join(
    PASTA_LANDING_RADAR_CONGRESSO, "deputados_governismo.json"
)
ARQUIVO_ID_DEPUTADOS_CSV = "src/params/radar_congresso_ids.csv"
ARQUIVO_SCHEMAS_TXT = "data/bronze/camara/radar_congresso/schema_cols/todas_colunas_id_voz_deputado_detalhes.txt"

TAB_RADARCONGRESSO__GOVERNISMO_DEPUTADOS_RAW = (
    "radarcongresso__governismo_deputados_raw"
)


def pipeline_radarcongresso_governismo_deputados():
    logger = logger_setting("pipeline_radarcongresso__governismo_deputados_raw")
    logger.info("Iniciando Pipeline de Governismo dos Deputados na Camara")

    try:
        data = make_http_request(URL_GOVERNISMO)
        if data:
            response_to_json(
                data, PASTA_LANDING_RADAR_CONGRESSO, "deputados_governismo.json"
            )
            logger.info(f"✅ Dados salvos em: {PASTA_LANDING_RADAR_CONGRESSO}")
        else:
            logger.warning(f"⚠️ Nenhuma resposta da API")
            return
    except Exception as e:
        logger.error(
            f"❌ Falha ao processar arquivo em: {PASTA_LANDING_RADAR_CONGRESSO}"
        )
        logger.exception(e)
    try:
        with open(f"{ARQUIVO_GOVERNISMO_JSON}", "r") as f:
            df = json.load(f)
        df = pd.DataFrame(df)
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
        logger.info(f"✅ Dados transformados com sucesso")
        try:
            GovernismoDeputadoSchema.validate(df_long)
            logger.info("✅ Dados validados com Pandera com sucesso")
            pg = PostgreSQLManager()
            pg.send_to_db(
                df=df_long,
                table_name=TAB_RADARCONGRESSO__GOVERNISMO_DEPUTADOS_RAW,
                filename="deputados_gorvernismo.json",
            )
            logger.info("✅ Dados carregados com sucesso")
            logger.info("✅ Concluido Pipeline de Deputados na Camara")
            logger.info("-" * 75)
        except SchemaError as e:
            logger.error(
                "❌ Erro de schema Pandera na validação do DataFrame antes da carga"
            )
            logger.exception(e)
            return
    except Exception as e:
        logger.error(
            f"❌ Falha ao processar arquivo em: {PASTA_LANDING_RADAR_CONGRESSO}"
        )
        logger.exception(e)


##########################################################################################################################
##########################################################################################################################
##########################################################################################################################


def get_radarcongresso_id_deputados():
    with open(ARQUIVO_GOVERNISMO_JSON, "r") as f:
        data_json = json.load(f)

    ids = list(set(data_json["parlamentares"].keys()))

    with open(ARQUIVO_ID_DEPUTADOS_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id_radarcongresso"])
        for id in ids:
            writer.writerow([id])
    print(f"Arquivo de ID de deputados salvo em {ARQUIVO_ID_DEPUTADOS_CSV}")


def pipeline_radarcongresso_deputados_detalhes():
    logger = logger_setting("pipeline_radarcongresso__deputados_raw")
    logger.info("Iniciando Pipeline de Governismo dos Deputados na Camara")

    url = URL_DEPUTADOS_BASE
    df_id = pd.read_csv("src/params/radar_congresso_ids.csv")
    id_ls = df_id.iloc[:, 0].to_list()
    for id in id_ls:
        data_json = make_http_request(f"{url}/{id}/info")
        if data_json:
            response_to_json(
                data_json,
                PASTA_LANDING_RADAR_CONGRESSO_DEPUTADOS,
                f"{id}_voz_deputado_detalhes.json",
            )
            df = pd.json_normalize(data_json)
            df = TheEqualizer(df).sanitize_columns_names().df
            df.to_csv(
                f"{PASTA_BRONZE_RADAR_CONGRESSO}{id}_voz_deputado_detalhes.csv",
                sep=";",
                index=False,
            )

    pg = PostgreSQLManager()
    for file in os.listdir(PASTA_BRONZE_RADAR_CONGRESSO):
        df = pd.read_csv(
            os.path.join(PASTA_BRONZE_RADAR_CONGRESSO, file),
            sep=";",
            encoding="utf-8",
            dtype=str,
        )

    with open(f"{ARQUIVO_SCHEMAS_TXT}", "r", encoding="utf-8") as f:
        linhas = f.read().splitlines()
        for col in linhas:
            if col not in df.columns:
                df[col] = None

    DeputadosRadarSchema.validate(df)

    pg.send_to_db(df, "radarcongresso__deputados_raw", how="append", filename=file)


if __name__ == "__main__":
    # pipeline_radarcongresso_governismo_deputados()
    # pipeline_radarcongresso_deputados_detalhes()
    # get_radarcongresso_id_deputados()
    pass
