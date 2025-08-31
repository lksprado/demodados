"""
Pipeline Radar Congresso (parlamentares e governismo).
Extrai dados de https://radar.congressoemfoco.com.br/

Fluxo das Classes:
1) extract: baixa JSON da API do Radar Congresso e salva em data/landing.
2) transform: normaliza e salva CSV em data/bronze.
3) validate: valida conforme schema pandera
3) load: trunca e faz carga na tabela raw Postgres correspondente.

Requisitos:
- Conexão Postgres configurada em PostgreSQLManager
- Schemas Pandera: ParlamentarRadarSchema, GovernismoSchema
"""

import json
import logging
import re
from typing import Optional

import pandas as pd
from pandera import DataFrameModel
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import GovernismoSchema, ParlamentarRadarSchema
from src.utils.pipeline_cfg import GenericETL
from src.utils.transformers.cleaning import ColumnSanitizer

# HELP DE TRANSFORMACAO


def transform_governismo(
    df: pd.DataFrame, log: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """Funcao auxiliar para transformacao especifica de tabelas de governismo

    Args:
        df (pd.DataFrame): dataframe

    Raises:
        ValueError: Verifica se existe coluna parlamentares
        ValueError: Verifica se coluna parlamentares nao esta fazia
        ValueError: Verifica se existem colunas dos trimestres

    Returns:
        pd.DataFrame: _description_
    """
    logger = log or logging.getLogger(__name__)
    if df.empty:
        logger.warning("DATAFRAME VAZIO!")

    parlamentares = df["parlamentares"]
    df_parlamentares = pd.json_normalize(parlamentares)
    df_parlamentares = df_parlamentares.dropna(subset=["id"]).reset_index()
    cols_to_keep = [
        col
        for col in df_parlamentares.columns
        if "trimestral" not in col or "total" in col
    ]
    df_parlamentares = df_parlamentares[cols_to_keep]
    cols_to_rename = [col for col in df_parlamentares.columns if "trimestral" in col]
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
    return df_long


class RadarCongressoParlamentares(GenericETL):
    """ETL para endpoint de lista de parlamentares (deputados+senadores).

    Args:
        cfg_digt: dicionario de configuracao
        schema: validacao Pandera
        log: opcional
    """

    def __init__(self, cfg_dict, schema: DataFrameModel, log=None):
        self.schema = schema
        super().__init__(cfg_dict, log)

    def extract(self):
        super().generic_extraction()

    def transform(self):
        try:
            df = pd.read_json(self.cfg.landing_filepath, dtype=str)
            df = ColumnSanitizer(df).sanitize_columns_names().df
            df.to_csv(self.cfg.bronze_filepath, sep=";", index=False)
            self.logger.info(f"CSV SALVO EM: {self.cfg.bronze_filepath}")
            return df
        except:
            raise (
                self.logger.error(
                    f"ERRO AO TRANSFORMAR ARQUIVO --- {self.cfg.landing_filepath}"
                )
            )

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            validated_df = self.schema.validate(df)
            return validated_df
        except SchemaError as e:
            self.logger.error(f"ERRO DE SCHEMA --- {e}")
            raise

    def load(self, df: pd.DataFrame):
        self.loader.truncate_table(table_name=self.cfg.db_table)

        self.loader.send_to_db(
            df=df,
            table_name=self.cfg.db_table,
            filename=self.cfg.bronze_file,
            how="append",
        )


class RadarGovernismoETL(GenericETL):
    """ETL para endpoint de governismo de parlamentares (deputados+senadores).

    Args:
        cfg_digt: dicionario de configuracao
        schema: validacao Pandera
        log: opcional
    """

    def __init__(self, cfg_dict: dict, schema, log: Optional[logging.Logger] = None):
        self.schema = schema
        super().__init__(cfg_dict, log)

    def extract(self):
        # 1 request -> 1 arquivo (landing)
        self.generic_extraction()

    def transform(self) -> pd.DataFrame:
        self.logger.info("Iniciando Transformação (Governismo)")
        try:
            with self.cfg.landing_filepath.open("r") as f:
                raw = json.load(f)

            df = pd.DataFrame(raw)
            df = transform_governismo(df)
            df = ColumnSanitizer(df).sanitize_columns_names().df
            df.to_csv(self.cfg.bronze_filepath, sep=";", index=False)
            self.logger.info(f"CSV salvo em: {self.cfg.bronze_filepath}")
            return df
        except Exception:
            self.logger.error(
                f"Erro ao transformar arquivo: {self.landing_filepath}",
                exc_info=True,
            )

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            return self.schema.validate(df)
        except SchemaError:
            self.logger.error("Erro de schema (Governismo)", exc_info=True)

    def load(self, df: pd.DataFrame):
        self.loader.truncate_table(table_name=self.cfg.db_table, log=self.logger)

        self.loader.send_to_db(
            df=df,
            table_name=self.cfg.db_table,
            filename=self.cfg.bronze_file,
            how="append",
            log=self.logger,
        )


if __name__ == "__main__":

    # 1) Parlamentares
    cfg_parlamentares = {
        "url_base": "https://radar.congressoemfoco.com.br/api/busca-parlamentar",
        "landing_dir": "./data/raw/radar_congresso/parlamentares/",
        "landing_file": "radar_parlamentares.json",
        "bronze_dir": "./data/bronze/radar_congresso/parlamentares/",
        "bronze_file": "radar_parlamentares.csv",
        "db_table": "radar_parlamentares_raw",
    }

    etl_parl = RadarCongressoParlamentares(
        cfg_parlamentares, schema=ParlamentarRadarSchema
    )
    etl_parl.run_pipeline(E=False, T=True, V=True, L=True)

    # 2) Governismo – Deputados (Câmara)
    cfg_gov_dep = {
        "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=camara",
        "landing_dir": "./data/raw/radar_congresso/governismo/",
        "landing_file": "radar_governismo_deputados.json",
        "bronze_dir": "./data/bronze/radar_congresso/governismo/",
        "bronze_file": "radar_governismo_deputados.csv",
        "db_table": "radar_governismo_deputados_raw",
    }
    etl_gov_dep = RadarGovernismoETL(cfg_gov_dep, schema=GovernismoSchema)
    etl_gov_dep.run_pipeline(E=False, T=True, V=True, L=True)

    # 3) Governismo – Senadores (Senado)
    cfg_gov_sen = {
        "url_base": "https://radar.congressoemfoco.com.br/api/governismo?casa=senado",
        "landing_dir": "./data/raw/radar_congresso/governismo/",
        "landing_file": "radar_governismo_senadores.json",
        "bronze_dir": "./data/bronze/radar_congresso/governismo/",
        "bronze_file": "radar_governismo_senadores.csv",
        "db_table": "radar_governismo_senadores_raw",
    }
    etl_gov_sen = RadarGovernismoETL(cfg_gov_sen, schema=GovernismoSchema)
    etl_gov_sen.run_pipeline(E=False, T=True, V=True, L=True)
