import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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


@dataclass(frozen=True)
class PipelineConfig:
    """Contrato imutável de configuração do pipeline."""

    url_base: str
    landing_dir: Path | str
    bronze_dir: Path | str
    error_dir: Path | str
    parameter_file: str
    db_table: str

    def __post_init__(self):
        # Normaliza diretórios para Path, mesmo se vierem como string dos dicts atuais
        # Faz normalizacoes/validacoes derivadas dos campos recebidos
        # object.__setattr__ permite ajustar campos
        object.__setattr__(self, "landing_dir", Path(self.landing_dir))
        object.__setattr__(self, "bronze_dir", Path(self.bronze_dir))
        object.__setattr__(self, "error_dir", Path(self.error_dir))
        object.__setattr__(self, "parameter_file", Path(self.parameter_file))

    # Conveniência para garantir diretórios antes de usar
    def ensure_dirs(self) -> None:
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)


PIPELINE_CONFIG_PRD = {
    "parameter_file": "src/params/id_deputados.csv",
    "url_base": "https://dadosabertos.camara.leg.br/api/v2/deputados/",
    "landing_dir": "./data/landing/camara/deputados/",
    "bronze_dir": "./data/bronze/camara/deputados/",
    "error_dir": "./data/error/camara/deputados/",
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

CFG_PRD = PipelineConfig(**PIPELINE_CONFIG_PRD)


def obter_ids_deputados_atuais():
    """Funcao auxiliar para obter todos deputados atuais"""
    data = make_http_request(
        "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome"
    )
    response_to_json(data, "src/params/", "id_deputados.json")
    df_ids = make_df_from_json_list("src/params/id_deputados.json")
    df_ids.to_csv("src/params/id_deputados.csv", sep=";")


def extraction(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    cfg.ensure_dirs()

    try:
        df_ids = pd.read_csv(cfg.parameter_file, sep=";")
        id_list = df_ids["id"].to_list()
    except Exception as e:
        logger.error(f"ERRO AO LER ARQUIVO DE IDS: {cfg.parameter_file}")
        raise

    for id in id_list:
        try:
            url = f"{cfg.url_base}{id}"
            data = make_http_request(url, log=logger)

            if data:
                output_file = f"{id}_deputado.json"
                response_to_json(data, cfg.landing_dir, output_file)
            else:
                logger.warning(f"RESPOSTA VAZIA: {url}")
        except Exception as e:
            logger.error(f"ERRO REQUISICAO {url} --- {e}")


def transform_deputado(
    cfg: PipelineConfig, cols_to_sanitize: list, log: Optional[logging.Logger] = None
):
    logger = log or logging.getLogger(__name__)
    cfg.ensure_dirs()

    for f in cfg.landing_dir.iterdir():
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

                output_file = f"{dep_id}_deputado.csv"

                file_destination = cfg.bronze_dir / output_file
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


def validate_load_to_db(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)

    dataframes = []

    ## VALIDACAO E CARGA
    for f in cfg.bronze_dir.iterdir():
        try:
            df = pd.read_csv(f, sep=";")
            try:
                validated_df = DeputadoSchema.validate(df)
                dataframes.append(validated_df)
            except SchemaError as e:
                cfg.error_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(f, cfg.error_dir / f.name)
                logger.error(f"ERRO VALIDACAO SCHEMA: {f.name} --- {e}")
                logger.info(f"COPIANDO {f.name} PARA {cfg.error_dir}")
                return

        except Exception as e:
            logger.error(f"ERRO AO PROCESSAR: {f.name} --- {e}")

    if not dataframes:
        logger.warning("NENHUM CSV VALIDO PARA CARGA.")
        return

    all_dataframes = pd.concat(dataframes, ignore_index=True)

    PostgreSQLManager.send_to_db(
        df=all_dataframes,
        table_name=cfg.db_table,
        how="replace",
        filename="",
        log=logger,
    )


def pipeline_camara_deputados(extract=False, transform=False, load=False):
    logger = logger_setting("pipeline_camara_deputados")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extract:
        try:
            extraction(CFG_PRD, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transform:
        try:
            transform_deputado(CFG_PRD, cols_to_not_sanitize_values, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load:
        try:
            validate_load_to_db(CFG_PRD, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


if __name__ == "__main__":
    pipeline_camara_deputados(True, True, True)
