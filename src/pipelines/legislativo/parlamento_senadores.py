"""
Pipeline — Senadores (Senado Federal)

Extrai, transforma, valida e carrega dados de senadores a partir da API:
https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4

Fluxo:
1) extract_senadores(cfg): baixa o JSON em data/landing (opcional neste run).
2) transform_senadores(cfg): normaliza o JSON, sanitiza colunas e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (SenadoresRadarSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: SenadoresRadarSchema.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import json
import logging

import pandas as pd

from ...utils.pipeline_cfg import GenericETL, PipelineConfig
from ...utils.transformers.cleaning import ColumnSanitizer
from .schema import SenadorSchema

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

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


def transform_senadores(cfg: PipelineConfig):

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


def run_senadores_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        validator=SenadorSchema,
        log=logger,
    )

    # etl.extract()
    transform_senadores(cfg)
    # etl.validate()
    etl.load()


if __name__ == "__main__":

    PIPELINE_SENADORES_CONFIG_PRD = {
        "url_base": "https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4",
        "landing_dir": "./data/raw/senado/senadores/",
        "landing_file": "parlamento_senadores.json",
        "bronze_dir": "./data/bronze/senado/senadores/",
        "bronze_file": "parlamento_senadores.csv",
        "db_table": "raw_parlamento_senadores",
    }

    run_senadores_pipeline(PipelineConfig(**PIPELINE_SENADORES_CONFIG_PRD))
