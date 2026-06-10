import logging
from functools import partial
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.json_parsers import normalize_json_object

logger = logging.getLogger("raw_camara_deputados")

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


def extract(cfg: PipelineConfig, workers: int = 1):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)
    df_ids = pd.read_csv(cfg.parameter_filepath)
    ids_list = df_ids["id"].drop_duplicates().to_list()

    tasks = [
        (f"{cfg.url_base}{dep_id}", f"{dep_id}_deputado.json") for dep_id in ids_list
    ]
    extractor.fetch_and_save_many(tasks, cfg.landing_dir, workers=workers)


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
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

        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=partial(extract, workers=4),
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
    config = load_source_config(_CONFIG_FILE, source="deputados", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.camara.parlamento_deputados
