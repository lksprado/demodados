"""
Pipeline — Senadores (Senado Federal)

Extrai, transforma, valida e carrega dados de senadores em exercício a partir da API:
https://legis.senado.leg.br/dadosabertos/senador/lista/atual?v=4

Fluxo:
1) etl.extract(): baixa o JSON em data/landing usando o extractor genérico.
2) transform_senadores(cfg): normaliza o JSON, sanitiza colunas e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (SenadorSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: SenadorSchema.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_parlamento_senadores")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"

cols_to_not_sanitize_values = [
    "identificacaoparlamentar_urlfotoparlamentar",
    "identificacaoparlamentar_urlpaginaparlamentar",
    "mandato_suplentes_suplente",
    "mandato_exercicios_exercicio",
    "identificacaoparlamentar_telefones_telefone",
    "identificacaoparlamentar_emailparlamentar",
    "identificacaoparlamentar_urlpaginaparticular",
]


def extract_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)
    current = 58
    previous = current - 1

    extractor.fetch_and_save(
        url=f"{cfg.url_base}",
        output_dir=cfg.landing_dir,
        filename=f"{previous}_{current}_senado_senadores.json",
    )


def transform_senadores(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for json_file in cfg.landing_dir.glob("*.json"):
        with open(json_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        df = pd.json_normalize(
            data,
            record_path=["ListaParlamentarEmExercicio", "Parlamentares", "Parlamentar"],
            sep=".",
        )

        df = (
            ColumnSanitizer(df)
            .sanitize_columns_names()
            .not_sanitize_columns_values(cols=cols_to_not_sanitize_values)
            .df
        )
        dataframes.append(df)

    df_final = pd.concat(dataframes, ignore_index=True)

    df_final.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_legislatura,
        load_fn=None,
        validator=None,
        log=logger,
    )

    etl.extract()
    transform_senadores(cfg)
    etl.validate()
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="senadores", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_senadores
