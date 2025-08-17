"""
Pipeline Radar Congresso (parlamentares e governismo).
Extrai dados de https://radar.congressoemfoco.com.br/

Fluxo:
1) extract_*: baixa JSON da API do Radar Congresso e salva em data/landing.
2) transform_*: normaliza e salva CSV em data/bronze.
3) load_*: valida com Pandera e insere na tabela Postgres correspondente.

Requisitos:
- Conexão Postgres configurada em PostgreSQLManager
- Schemas Pandera: ParlamentarRadarSchema, GovernismoSchema
"""

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import GovernismoSchema, ParlamentarRadarSchema
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

CFG_PARL = PipelineConfig(**PIPELINE_PARLAMENTARES_CONFIG_PRD)
CFG_GOV_DEP = PipelineConfig(**PIPELINE_GOVERNISMO_DEPUTADOS_CONFIG_PRD)
CFG_GOV_SEN = PipelineConfig(**PIPELINE_GOVERNISMO_SENADORES_CONFIG_PRD)


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


def transform_parlamentares(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    cfg.ensure_dirs()

    df = pd.read_json(cfg.landing_path, dtype=str)  # Path funciona direto
    df = ColumnSanitizer(df).sanitize_columns_names().df

    df.to_csv(cfg.bronze_path, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_path}")


def load_parlamentares(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    validate_and_load_to_db(
        csv_file=cfg.bronze_path,
        table=cfg.db_table,
        schema=ParlamentarRadarSchema,
        file=str(cfg.bronze_path),
        logger=logger,
    )


def transform_governismo(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
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
    cfg.ensure_dirs()

    with cfg.landing_path.open("r") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)

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
            # Extrai "YYYY-MM-DD" do nome da coluna trimestral vinda da API
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", c)
            if date_match:
                date = date_match.group(0)  # Captura a data
            new_col_name = f"{date}"
            cols_dict[c] = new_col_name
        df_parlamentares = df_parlamentares.rename(columns=cols_dict)
        cols_renamed = list(cols_dict.values())

        # Converte wide->long
        df_long = df_parlamentares.melt(
            id_vars=["id", "afavor", "n", "total"],
            value_vars=cols_renamed,
            var_name="trimestre",
            value_name="perc_governismo",
        )

        df_long.to_csv(cfg.bronze_path, sep=";", index=False)
        logger.info(f"CSV SALVO EM: {cfg.bronze_path}")
    except Exception as e:
        logger.error(f"ERRO AO TRANSFORMAR {cfg.landing_file} --- {e} ")
        raise


def load_governismo(cfg: PipelineConfig, log: Optional[logging.Logger] = None):
    logger = log or logging.getLogger(__name__)
    validate_and_load_to_db(
        csv_file=cfg.bronze_path,
        table=cfg.db_table,
        schema=GovernismoSchema,
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


##########################################################
## EXECUTORES PARA A PRIMEIRA CARGA


def pipeline_radar_parlamentares(extract=False, transform=False, load=False):
    logger = logger_setting("pipeline_radar_paralmentares_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extract:
        try:
            extraction(CFG_PARL, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transform:
        try:
            transform_parlamentares(CFG_PARL, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load:
        try:
            load_parlamentares(CFG_PARL, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


def pipeline_radar_governismo_deputados(extract=False, transform=False, load=False):
    logger = logger_setting("pipeline_radar_governismo_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extract:
        try:
            extraction(CFG_GOV_DEP, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transform:
        try:
            transform_governismo(CFG_GOV_DEP, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load:
        try:
            load_governismo(CFG_GOV_DEP, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


def pipeline_radar_governismo_senadores(extract=False, transform=False, load=False):
    logger = logger_setting("pipeline_radar_governismo_raw")
    logger.info("-" * 100)
    logger.info("Iniciando pipeline")
    if extract:
        try:
            extraction(CFG_GOV_SEN, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if transform:
        try:
            transform_governismo(CFG_GOV_SEN, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    if load:
        try:
            load_governismo(CFG_GOV_SEN, logger)
        except Exception as e:
            logger.error(f"ALGO DEU ERRADO --- {e}")
    logger.info("Finalizado pipeline")
    logger.info("-" * 100)


if __name__ == "__main__":
    pipeline_radar_governismo_deputados(True, True, True)
    pipeline_radar_governismo_senadores(True, True, True)
    pipeline_radar_parlamentares(True, True, True)
    pass
