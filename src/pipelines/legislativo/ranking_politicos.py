"""
Pipeline Ranking Politicos.
Extrai dados de https://politicos.org.br/en/Ranking

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

import pandas as pd

from src.pipelines.legislativo.schema import ParlamentaresRankingSchema
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer
from src.utils.transformers.json_parsers import make_df_from_json_list

logger = logging.getLogger("Pipeline: raw_ranking_deputados")


def transform_parlamentares(df, cfg: PipelineConfig) -> pd.DataFrame:
    # 1) Ler e normalizar nomes
    df_new = make_df_from_json_list(cfg.landing_filepath, list_key="data")
    df_new = ColumnSanitizer(df_new).sanitize_columns_names().df

    def _extract_register(d):
        if not isinstance(d, dict):
            return None
        reg = d.get("register")
        if reg is not None:
            return reg
        text = d.get("otherInformations") or d.get("otherinformations") or ""
        m = re.search(r"\d+", text)
        return m.group(0) if m else None

    df_new["parliamentarianregister"] = pd.to_numeric(
        df_new["parliamentarian"].apply(_extract_register), errors="coerce"
    ).astype("Int64")
    df_new["position"] = df_new["parliamentarian"].apply(lambda d: d.get("position"))

    # 3) Seleção final (só o que existir)
    cols_to_keep = [
        "id",
        "parliamentarianid",
        "year",
        "scorepresence",
        "scoresavequota",
        "scoresavequotapercentage",
        "scoreprocess",
        "scoreinternal",
        "scoreprivileges",
        "scorewastage",
        "scoretotal",
        "scoreranking",
        "scorerankingbyposition",
        "scorerankingbyparty",
        "scorerankingbystate",
        "scorerankingbypositionbystate",
        "parliamentarianstatecount",
        "parliamentarianpositionstatecount",
        "active",
        "link",
        "parliamentarianregister",
        "position",
    ]
    df_new = df_new[[c for c in cols_to_keep if c in df_new.columns]]

    cols = ["link"]

    df_new = ColumnSanitizer(df_new).not_sanitize_columns_values(cols=cols).df

    df_new.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")
    return df_new


def run_ranking_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        transform_fn=transform_parlamentares,
        validate_fn=None,
        load_fn=None,
        validator=ParlamentaresRankingSchema,
        log=logger,
    )

    etl.extract()
    df = etl.transform()
    df = etl.validate(df)
    etl.load(df)


if __name__ == "__main__":
    PIPELINE_RANKING_PRD = {
        "url_base": "https://apirest2.politicos.org.br/api/parliamentarianranking?Include=Parliamentarian.State&Include=Parliamentarian.Party&Include=Parliamentarian.Organ&Include=Parliamentarian&take=700&StatusId=1&OrderBy=scoreRanking&Year=2025",
        "landing_dir": "./data/raw/ranking/parlamentares/",
        "landing_file": "ranking_parlamentares.json",
        "bronze_dir": "./data/bronze/ranking/parlamentares/",
        "bronze_file": "ranking_parlamentares.csv",
        "db_table": "raw_ranking_parlamentares",
    }
    run_ranking_pipeline(PipelineConfig(**PIPELINE_RANKING_PRD))
