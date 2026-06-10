import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("raw_senado_status")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    parameter_df = pd.read_csv(cfg.parameter_filepath, sep=";")
    parameter_df = parameter_df.loc[
        parameter_df["total_votos"] >= 5000, ["sigla", "numero", "ano"]
    ].drop_duplicates()

    extractor = HttpJsonExtractor(logger)
    for _, row in parameter_df.iterrows():
        sigla = row["sigla"]
        numero = int(row["numero"])
        ano = int(row["ano"])

        filename = f"status_{sigla}_{numero}_{ano}.json"
        url = f"{cfg.url_base}?sigla={sigla}&numero={numero}&ano={ano}&v=1"

        data = extractor.make_http_request(url=url)
        if data:
            extractor.save_response(data, cfg.landing_dir, filename)

    logger.info(f"✅ Extracao completa em {cfg.landing_dir}")


def transform(cfg: PipelineConfig) -> Path:
    logger.info("🔄 Iniciando transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = pd.read_json(f)
            if not data.empty:
                df = ColumnSanitizer(data).sanitize_columns_names().df
                dataframes.append(df)
        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"💾 CSV salvo em: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig):
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
    config = load_source_config(_CONFIG_FILE, source="status", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_status
