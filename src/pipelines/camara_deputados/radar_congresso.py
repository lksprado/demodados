import json
import os
import re
from datetime import datetime

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.camara_deputados.schema import GovernismoDeputadoSchema
from src.utils.extractors.https import make_http_request, response_to_json
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import TheEqualizer
from src.utils.transformers.json_parsers import (
    make_df_from_json_list,
    normalize_json_object,
)

URL = "https://radar.congressoemfoco.com.br/api/governismo?casa=camara"
PASTA_LANDING_RADAR_CONGRESSO = "data/landing/camara/radar_congresso/"
PASTA_BRONZE_RADAR_CONGRESSO = "data/bronze/camara/radar_congresso/"

TAB_RADARCONGRESSO__GOVERNISMO_DEPUTADOS_RAW = (
    "radarcongresso__governismo_deputados_raw"
)


def pipeline_radarcongresso_governismo_deputados():
    logger = logger_setting("pipeline_radarcongresso__governismo_deputados_raw")
    logger.info("Iniciando Pipeline de Governismo dos Deputados na Camara")

    try:
        data = make_http_request(URL)
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
        with open(
            f"{PASTA_LANDING_RADAR_CONGRESSO}deputados_governismo.json", "r"
        ) as f:
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


if __name__ == "__main__":
    pipeline_radarcongresso_governismo_deputados()
