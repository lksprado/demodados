"""
Pipeline — Ranking Políticos

Extrai, transforma, valida e carrega o ranking de parlamentares a partir da API:
https://apirest2.politicos.org.br/api/parliamentarianranking

Fluxo:
1) extract (opcional neste run): baixa o JSON em data/landing.
2) transform_parlamentares(cfg): normaliza o JSON, enriquece campos e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (ParlamentaresRankingSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: ParlamentaresRankingSchema.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import logging

from src.pipelines.legislativo.schema import ParlamentarRankingSchema
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer
from src.utils.transformers.json_parsers import normalize_json_object

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_ranking_parlamentares")


def transform_parlamentares(cfg: PipelineConfig):
    df_new = normalize_json_object(cfg.landing_filepath, key="data")
    df_new = ColumnSanitizer(df_new).sanitize_columns_names().df
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
        "parliamentariancount",
        "parliamentarianpositioncount",
        "parliamentarianpositionstatecount",
        "parliamentarianstatecount",
        "scorerankingbypositionbystate",
        "parliamentarianstaffmaxyear",
        "parliamentarianstaffamountused",
        "parliamentarianquotamaxyear",
        "parliamentarianquotatotal",
        "active",
        "link",
        "parliamentarian_name",
        "parliamentarian_email",
        "parliamentarian_position",
        "parliamentarian_otherinformations",
        "parliamentarian_profession",
        "parliamentarian_academic",
        "parliamentarian_register",
        "parliamentarian_phone",
        "parliamentarian_instagram",
        "parliamentarian_twitter",
        "parliamentarian_facebook",
        "parliamentarian_youtube",
    ]
    df_new = df_new[[c for c in cols_to_keep if c in df_new.columns]]

    cols_no_clean = [
        "link",
        "parliamentarian_email",
        "parliamentarian_otherinformations",
        "parliamentarian_instagram",
        "parliamentarian_twitter",
        "parliamentarian_facebook",
        "parliamentarian_youtube",
    ]

    df_new = ColumnSanitizer(df_new).not_sanitize_columns_values(cols=cols_no_clean).df

    df_new.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")

    return df_new


def run_ranking_pipeline(cfg: PipelineConfig) -> None:
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        validator=ParlamentarRankingSchema,
        log=logger,
    )

    etl.extract()
    transform_parlamentares(cfg)
    etl.validate()
    pg = PostgreSQLManager()
    pg.execute_query(query="DROP TABLE raw.raw_ranking_parlamentares CASCADE")
    etl.load()


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
    ## python -m src.pipelines.legislativo.ranking_parlamentares
