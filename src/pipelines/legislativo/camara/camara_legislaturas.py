"""
Pipeline — Legislaturas (Câmara dos Deputados)

Extrai, transforma e carrega a lista de deputados por legislatura a partir da API:
https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura={id}

Fluxo:
1) full_extract_legislatura(cfg): baixa um JSON por legislatura em data/landing (opcional neste run).
2) transform_legislatura(cfg): normaliza os JSONs, consolida e salva CSV em data/bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- PipelineConfig com: url_base, landing_dir, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (INFO). Em Airflow, remova basicConfig e use o logger do Airflow.
- O separador do CSV bronze é ';' e deve ser consistente em transform/validate/load.
- Config carregada via camara_config.yml; altere env= para trocar entre local e airflow.
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
    force=True,
)

logger = logging.getLogger("Pipeline: raw_parlamento_legislatura")

_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def range_extract_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    for leg in range(51, 57):
        extractor.fetch_and_save(
            url=f"{cfg.url_base}?idLegislatura={leg}&ordem=ASC&ordenarPor=nome",
            output_dir=cfg.landing_dir,
            filename=f"{leg}_deputados_legislatura.json",
        )


def extract_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)
    current = 57

    extractor.fetch_and_save(
        url=f"{cfg.url_base}?idLegislatura={current}&ordem=ASC&ordenarPor=nome",
        output_dir=cfg.landing_dir,
        filename=f"{current}_deputados_legislatura.json",
    )


def transform_legislatura(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            with open(f, "r", encoding="utf-8") as fp:
                raw = json.load(fp)

            dados = raw.get("dados", [])
            if not dados:
                continue

            uri_self = next(
                (
                    lnk["href"]
                    for lnk in raw.get("links", [])
                    if lnk.get("rel") == "self"
                ),
                None,
            )

            df = pd.json_normalize(dados, sep=".")
            df["url_link"] = uri_self
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

    etl.extract()
    transform_legislatura(cfg)
    # etl.validate()
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="legislatura", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.camara.parlamento_legislaturas
