import datetime
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.json_parsers import normalize_json_object

logger = logging.getLogger("raw_ranking_parlamentares")

_CONFIG_FILE = Path(__file__).parent / "ranking_politicos_config.yml"


def extract(cfg: PipelineConfig) -> Path:
    current_year = datetime.datetime.now().year
    HttpJsonExtractor(logger).fetch_and_save(
        url=cfg.url_base,
        output_dir=cfg.landing_dir,
        filename=f"ranking_senadores_{current_year}.json",
    )


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
    dataframes = []
    for json_file in cfg.landing_dir.glob("*.json"):
        df = normalize_json_object(json_file, key="items")
        df = ColumnSanitizer(df).sanitize_columns_names().df
        dataframes.append(df)

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg: PipelineConfig) -> None:
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
    config = load_source_config(_CONFIG_FILE, source="senadores", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.ranking_politicos.ranking_senadores
