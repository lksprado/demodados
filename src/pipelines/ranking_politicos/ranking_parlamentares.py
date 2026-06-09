"""
Pipeline — Ranking Políticos

Extrai, transforma, valida e carrega o ranking de parlamentares a partir da API:
https://apirest2.politicos.org.br/api/parliamentarianranking

Fluxo:
1) etl.extract(): baixa o JSON em data/landing usando o extractor genérico.
2) transform_parlamentares(cfg): normaliza o JSON, seleciona colunas e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (ParlamentarRankingSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: ParlamentarRankingSchema.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import logging
from pathlib import Path

from ...utils.loaders.postgres import PostgreSQLManager
from ...utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ...utils.transformers.cleaning import ColumnSanitizer
from ...utils.transformers.json_parsers import normalize_json_object
from ..legislativo.schema import ParlamentarRankingSchema

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_ranking_parlamentares")

_CONFIG_FILE = Path(__file__).parent / "ranking_politicos_config.yml"

cols_to_keep = [
    "id",
    "parliamentarianid",
    "parliamentarian_state_prefix",
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
    "parliamentarian_datebirth",
    "parliamentarian_register",
    "parliamentarian_phone",
    "parliamentarian_instagram",
    "parliamentarian_twitter",
    "parliamentarian_facebook",
    "parliamentarian_youtube",
]

cols_to_not_sanitize_values = [
    "link",
    "parliamentarian_email",
    "parliamentarian_otherinformations",
    "parliamentarian_instagram",
    "parliamentarian_twitter",
    "parliamentarian_facebook",
    "parliamentarian_youtube",
]


def transform_parlamentares(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    df = normalize_json_object(cfg.landing_filepath, key="data")
    df = ColumnSanitizer(df).sanitize_columns_names().df
    df = df[[c for c in cols_to_keep if c in df.columns]]
    df = (
        ColumnSanitizer(df)
        .not_sanitize_columns_values(cols=cols_to_not_sanitize_values)
        .df
    )

    df.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig) -> None:
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
    config = load_source_config(_CONFIG_FILE, source="parlamentares", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.ranking_politicos.ranking_parlamentares
