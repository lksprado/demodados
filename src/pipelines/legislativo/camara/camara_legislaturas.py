import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("raw_camara_legislaturas")

_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def range_extract_legislatura(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)

    for leg in range(51, 57):
        extractor.fetch_and_save(
            url=f"{cfg.url_base}?idLegislatura={leg}&ordem=ASC&ordenarPor=nome",
            output_dir=cfg.landing_dir,
            filename=f"{leg}_deputados_legislatura.json",
        )


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)
    current = 57

    extractor.fetch_and_save(
        url=f"{cfg.url_base}?idLegislatura={current}&ordem=ASC&ordenarPor=nome",
        output_dir=cfg.landing_dir,
        filename=f"{current}_deputados_legislatura.json",
    )


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
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
            logger.error(f"❌ Erro ao transformar {f}: {e}", exc_info=True)
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract,
        transform_fn=transform,
        load_fn=None,
        log=logger,
    )
    etl.run()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="legislatura", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.camara.parlamento_legislaturas
