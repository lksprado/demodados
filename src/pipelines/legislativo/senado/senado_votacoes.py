import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.json_parsers import normalize_json_object

logger = logging.getLogger("raw_senado_votacoes")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)

    for y in range(2001, 2027):
        logger.info(f"Extraindo {y}...")
        base_params = f"dataInicio={y}-01-01&dataFim={y}-12-31&v=1"

        data = extractor.make_http_request(f"{cfg.url_base}?{base_params}")
        if not data:
            logger.warning(f"⚠️ Sem dados para {y}.")
            continue
        extractor.save_response(data, cfg.landing_dir, f"{y}_senado_votacoes")


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            # JSON é um array na raiz — sem chave intermediária
            # pd.json_normalize achata informeLegislativo automaticamente (sep=".")
            data = normalize_json_object(f)
            if not data.empty:
                # votos é uma lista de dicts por linha; descartado aqui
                # (tratado em pipeline separado de votos individuais)
                if "votos" in data.columns:
                    data = data.drop(columns=["votos"])
                df = ColumnSanitizer(data).sanitize_columns_names().df
                dataframes.append(df)

        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue

    dfs = pd.concat(dataframes, ignore_index=True)

    # Remove quebras de linha embutidas em colunas de texto (ex: informelegislativo_texto)
    # para evitar ParserError no pd.read_csv posterior
    str_cols = dfs.select_dtypes(include="object").columns
    dfs[str_cols] = dfs[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True)
    )

    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)

    if cfg.output_param_file:
        id_col = "codigosessaovotacao"
        if id_col in dfs.columns:
            dfs[[id_col]].drop_duplicates().to_csv(
                cfg.output_param_filepath, index=False
            )
            logger.info(f"📄 IDs exportados para: {cfg.output_param_filepath}")
        else:
            logger.warning(f"⚠️ Coluna '{id_col}' nao encontrada - IDs nao exportados.")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract,
        transform_fn=transform,
        load_fn=None,
        log=logger,
    )

    # etl.extract()
    etl.transform()
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="votacoes", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_sessoes
