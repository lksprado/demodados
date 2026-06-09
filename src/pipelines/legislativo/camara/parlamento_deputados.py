"""
Pipeline — Deputados (Câmara dos Deputados)

Extrai, transforma, valida e carrega dados detalhados de deputados a partir da API:
https://dadosabertos.camara.leg.br/api/v2/deputados/{id}

Fluxo:
1) extract_deputados(cfg): baixa um JSON por deputado em data/landing (opcional neste run).
2) transform_deputados(cfg): normaliza os JSONs, consolida e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (DeputadoSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: DeputadoSchema.
- PipelineConfig com: parameter_file (IDs), url_base, landing_dir, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (INFO). Em Airflow, remova basicConfig e use o logger do Airflow.
- O separador do CSV bronze é ';' e deve ser consistente em transform/validate/load.
"""

import logging
from functools import partial
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.json_parsers import normalize_json_object
from ..schema import DeputadoSchema

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_parlamento_deputados")

_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"

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


def extract_deputados(cfg: PipelineConfig, workers: int = 1):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)
    df_ids = pd.read_csv(cfg.parameter_filepath)
    ids_list = df_ids["id"].drop_duplicates().to_list()

    tasks = [
        (f"{cfg.url_base}{dep_id}", f"{dep_id}_deputado.json") for dep_id in ids_list
    ]
    extractor.fetch_and_save_many(tasks, cfg.landing_dir, workers=workers)


def transform_deputados(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = normalize_json_object(f, "dados")
            if not data.empty:
                df = (
                    ColumnSanitizer(data)
                    .sanitize_columns_names()
                    .not_sanitize_columns_values(cols=cols_to_not_sanitize_values)
                    .df
                )
                dataframes.append(df)

        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=partial(extract_deputados, workers=4),
        load_fn=None,
        validator=DeputadoSchema,
        log=logger,
    )

    etl.extract()
    transform_deputados(cfg)
    etl.validate()
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="deputados", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.camara.parlamento_deputados
