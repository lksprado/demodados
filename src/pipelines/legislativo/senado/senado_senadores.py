import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("raw_senado_senadores")

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


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)
    current = 58
    previous = current - 1

    extractor.fetch_and_save(
        url=f"{cfg.url_base}",
        output_dir=cfg.landing_dir,
        filename=f"{previous}_{current}_senado_senadores.json",
    )


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
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
    logger.info(f"💾 CSV salvo em: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract,
        load_fn=None,
        log=logger,
    )

    etl.extract()
    transform(cfg)
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="senadores", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_senadores
