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

import pandas as pd

from src.pipelines.legislativo.schema import SenadoresRadarSchema
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("Pipeline: parlamento_senadores_raw")

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


def transform_senadores(df, cfg: PipelineConfig):

    with open(cfg.landing_filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(
        data,
        record_path=["ListaParlamentarEmExercicio", "Parlamentares", "Parlamentar"],
        sep=".",
    )

    df = (
        ColumnSanitizer(df)
        .sanitize_columns_names()
        .not_sanitize_columns_values(cols=cols)
        .df
    )

    df.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")
    return df


def run_senadores_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        transform_fn=transform_senadores,
        validate_fn=None,
        load_fn=None,
        validator=SenadoresRadarSchema,
        log=logger,
    )

    etl.extract()
    df = etl.transform()
    df = etl.validate(df)
    etl.load(df)


if __name__ == "__main__":

    PIPELINE_SENADORES_CONFIG_PRD = {
        "url_base": "https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4",
        "landing_dir": "./data/raw/senado/senadores/",
        "landing_file": "senado_senadores.json",
        "bronze_dir": "./data/bronze/senado/senadores/",
        "bronze_file": "senado_senadores.csv",
        "db_table": "parlamento_senadores_raw",
    }

    run_senadores_pipeline(PipelineConfig(**PIPELINE_SENADORES_CONFIG_PRD))
