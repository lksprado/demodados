"""
Pipeline — Senadores (Senado Federal)

Extrai, transforma, valida e carrega dados de senadores de uma determinada legislatura a partir da API:
https://legis.senado.leg.br/dadosabertos/senador/lista/legislatura/

Fluxo:
1) etl.extract(): baixa o JSON em data/landing usando o extractor genérico.
2) transform_senadores(cfg): normaliza o JSON, sanitiza colunas e salva CSV em data/bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
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


def range_extract_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    for leg in range(50, 59):
        extractor.fetch_and_save(
            url=f"{cfg.url_base}{leg}?exercicio=S&v=4",
            output_dir=cfg.landing_dir,
            filename=f"{leg}_senadores_legislatura.json",
        )


def extract_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)
    current = 58

    extractor.fetch_and_save(
        url=f"{cfg.url_base}?idLegislatura={current}&ordem=ASC&ordenarPor=nome",
        output_dir=cfg.landing_dir,
        filename=f"{current}_senadores_legislatura.json",
    )


def transform_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            with open(f, "r", encoding="utf-8") as fp:
                raw = json.load(fp)

                df = pd.json_normalize(
                    raw,
                    record_path=[
                        "ListaParlamentarLegislatura",
                        "Parlamentares",
                        "Parlamentar",
                    ],
                    sep=".",
                )

            df = ColumnSanitizer(df).sanitize_columns_names().df
            dataframes.append(df)

        except Exception as e:
            logger.error(f"Erro ao transformar {f}: {e}", exc_info=True)
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_legislatura,
        load_fn=None,
        validator=None,
        log=logger,
    )

    # etl.extract()
    transform_legislatura(cfg)
    # etl.validate()
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="legislatura", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_legislatura
