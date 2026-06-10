import json
import logging
import re
from pathlib import Path

import pandas as pd

from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config

logger = logging.getLogger("raw_radar_governismo")

_CONFIG_FILE = Path(__file__).parent / "radar_congresso_config.yml"


def transform(cfg: PipelineConfig) -> Path:
    with cfg.landing_filepath.open("r") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)

    if df.empty:
        logger.warning("⚠️ Dataframe vazio.")

    try:
        parlamentares = df["parlamentares"]
        df_parlamentares = pd.json_normalize(parlamentares)
        df_parlamentares = df_parlamentares.dropna(subset=["id"]).reset_index()
        cols_to_keep = [
            col
            for col in df_parlamentares.columns
            if "trimestral" not in col or "total" in col
        ]
        df_parlamentares = df_parlamentares[cols_to_keep]
        cols_to_rename = [
            col for col in df_parlamentares.columns if "trimestral" in col
        ]
        cols_dict = {}
        for c in cols_to_rename:
            # Extrai "YYYY-MM-DD" do nome da coluna trimestral vinda da API
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", c)
            if date_match:
                date = date_match.group(0)
            new_col_name = f"{date}"
            cols_dict[c] = new_col_name
        df_parlamentares = df_parlamentares.rename(columns=cols_dict)
        cols_renamed = list(cols_dict.values())

        # Converte wide->long
        df_long = df_parlamentares.melt(
            id_vars=["id", "afavor", "n", "total"],
            value_vars=cols_renamed,
            var_name="trimestre",
            value_name="perc_governismo",
        )

        df_long.to_csv(cfg.bronze_filepath, sep=";", index=False)
        logger.info(f"💾 CSV salvo em: {cfg.bronze_filepath}")
    except Exception:
        logger.error(f"❌ Erro ao transformar {cfg.landing_file}", exc_info=True)
        raise


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
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
    config_dep = load_source_config(
        _CONFIG_FILE, source="governismo_deputados", env="local"
    )
    config_sen = load_source_config(
        _CONFIG_FILE, source="governismo_senadores", env="local"
    )
    run_pipeline(PipelineConfig(**config_dep))
    run_pipeline(PipelineConfig(**config_sen))
    # python -m src.pipelines.legislativo.radar_congresso.radar_governismo
