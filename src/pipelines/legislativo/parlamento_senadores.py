"""
Pipeline Senadores
Extrai dados de https://legis.senado.leg.br/dadosabertos/api-docs/swagger-ui/index.html

Fluxo:
1) extract_*: baixa JSON da API do Site e salva em data/landing.
2) transform_*: normaliza e salva CSV em data/bronze.
3) load_*: valida com Pandera e insere na tabela Postgres correspondente.

Requisitos:
- Conexão Postgres configurada em PostgreSQLManager
- Schemas Pandera: ParlamentarRadarSchema, GovernismoSchema
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import SenadoresRadarSchema
from src.utils.extractors.https import HttpJsonExtractor
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.logger import logger_setting
from src.utils.transformers.cleaning import ColumnSanitizer


### DATACLASS REDUZ CODIGO BOILERPLATE
@dataclass(frozen=True)
class PipelineConfig:
    """Contrato imutável de configuração do pipeline."""

    url_base: str
    landing_dir: Path | str
    landing_file: str
    bronze_dir: Path | str
    bronze_file: str
    db_table: str

    def __post_init__(self):
        # Normaliza diretórios para Path, mesmo se vierem como string dos dicts atuais
        # Faz normalizacoes/validacoes derivadas dos campos recebidos
        # object.__setattr__ permite ajustar campos
        object.__setattr__(self, "landing_dir", Path(self.landing_dir))
        object.__setattr__(self, "bronze_dir", Path(self.bronze_dir))

    # Atalhos (não mutam o objeto)
    # Property transforma um método em atribudo calculado
    # Utiliza paths finais ao inves de ficar criando dentro dos metodos
    @property
    def landing_path(self) -> Path:
        return self.landing_dir / self.landing_file

    @property
    def bronze_path(self) -> Path:
        return self.bronze_dir / self.bronze_file

    # Conveniência para garantir diretórios antes de usar
    def ensure_dirs(self) -> None:
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)


# https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4

PIPELINE_SENADORES_CONFIG_PRD = {
    "url_base": "https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4",
    "landing_dir": "./data/landing/senado/senadores/",
    "landing_file": "senado_senadores.json",
    "bronze_dir": "./data/bronze/senado/senadores/",
    "bronze_file": "senado_senadores.csv",
    "db_table": "parlamento_senadores_raw",
}

CFG_SEN = PipelineConfig(**PIPELINE_SENADORES_CONFIG_PRD)

cols = [
    "identificacaoparlamentar_urlfotoparlamentar",
    "identificacaoparlamentar_urlpaginaparlamentar",
    "mandato_suplentes_suplente",
    "mandato_exercicios_exercicio",
    "identificacaoparlamentar_telefones_telefone",
    "identificacaoparlamentar_emailparlamentar",
    "identificacaoparlamentar_telefones_telefone",
    "identificacaoparlamentar_urlpaginaparticular",
]


def extraction(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    cfg.ensure_dirs()
    extractor = HttpJsonExtractor(
        url=cfg.url_base,
        output_dir=cfg.landing_dir,  # Path
        filename_fn=cfg.landing_file,  # se sua classe aceita str aqui
        logger=logger,
    )
    extractor.fetch_and_save()


def transform_parlamentares(
    cfg: PipelineConfig, cols_to_sanitize=None, log: Optional[logging.Logger] = None
):
    logger = log or logging.getLogger(__name__)
    cfg.ensure_dirs()

    with open(cfg.landing_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(
        data,
        record_path=["ListaParlamentarEmExercicio", "Parlamentares", "Parlamentar"],
        sep=".",
    )

    df = (
        ColumnSanitizer(df)
        .sanitize_columns_names()
        .not_sanitize_columns_values(cols=cols_to_sanitize)
        .df
    )

    df.to_csv(cfg.bronze_path, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_path}")


def load_parlamentares(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    validate_and_load_to_db(
        csv_file=cfg.bronze_path,
        table=cfg.db_table,
        schema=SenadoresRadarSchema,
        file=str(cfg.bronze_path),
        logger=logger,
    )


def validate_and_load_to_db(csv_file, table, schema, file, logger: logging.Logger):
    df = pd.read_csv(csv_file, sep=";")
    try:
        validated_df = schema.validate(df)
    except SchemaError as e:
        logger.error(f"ERRO DE SCHEMA --- {e}")
        return

    PostgreSQLManager.send_to_db(
        df=validated_df, table_name=table, filename=file, log=logger
    )


def pipeline_senado_senadores(extract=False, transform=False, load=False):
    logger = logger_setting("pipeline_senado_senadores_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extract:
        try:
            extraction(CFG_SEN, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transform:
        try:
            transform_parlamentares(CFG_SEN, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load:
        try:
            load_parlamentares(CFG_SEN, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


if __name__ == "__main__":

    pipeline_senado_senadores(transform=True, load=True)
