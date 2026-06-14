import logging
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.json_parsers import normalize_json_object

logger = logging.getLogger("raw_camara_votacoes")

_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def _get_last_page(links: list) -> int:
    for link in links:
        if link.get("rel") == "last":
            qs = parse_qs(urlparse(link["href"]).query)
            return int(qs.get("pagina", [1])[0])
    return 1


QUARTERS = [
    ("01-01", "03-31", "Q1"),
    ("04-01", "06-30", "Q2"),
    ("07-01", "09-30", "Q3"),
    ("10-01", "12-31", "Q4"),
]


def _current_quarter() -> tuple[int, str, str, str]:
    today = date.today()
    y = today.year
    q = (today.month - 1) // 3  # 0-based index into QUARTERS
    inicio, fim, label = QUARTERS[q]
    return y, inicio, fim, label


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao do quarter atual...")
    extractor = HttpJsonExtractor(logger)

    y, inicio, fim, label = _current_quarter()
    logger.info(f"Extraindo {y}-{label}...")
    base_params = (
        f"dataInicio={y}-{inicio}&dataFim={y}-{fim}"
        f"&itens=100&ordem=DESC&ordenarPor=dataHoraRegistro"
    )

    first_page_data = extractor.make_http_request(
        f"{cfg.url_base}?{base_params}&pagina=1"
    )
    if not first_page_data:
        logger.warning(f"⚠️ Sem dados para {y}-{label}.")
        return

    last_page = _get_last_page(first_page_data.get("links", []))
    logger.info(f"{y}-{label}: {last_page} pagina(s)")

    tasks = [
        (f"{cfg.url_base}?{base_params}&pagina={p}", f"votacoes_{y}_{label}_{p}.json")
        for p in range(1, last_page + 1)
    ]
    extractor.fetch_and_save_many(tasks, cfg.landing_dir)


def full_extract_votacoes(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)

    for y in range(2001, 2027):
        for inicio, fim, label in QUARTERS:
            logger.info(f"Extraindo {y}-{label}...")
            base_params = (
                f"dataInicio={y}-{inicio}&dataFim={y}-{fim}"
                f"&itens=100&ordem=DESC&ordenarPor=dataHoraRegistro"
            )

            first_page_data = extractor.make_http_request(
                f"{cfg.url_base}?{base_params}&pagina=1"
            )
            if not first_page_data:
                logger.warning(f"⚠️ Sem dados para {y}-{label}, pulando.")
                continue

            last_page = _get_last_page(first_page_data.get("links", []))
            logger.info(f"{y}-{label}: {last_page} pagina(s)")

            tasks = [
                (
                    f"{cfg.url_base}?{base_params}&pagina={p}",
                    f"votacoes_{y}_{label}_{p}.json",
                )
                for p in range(1, last_page + 1)
            ]
            extractor.fetch_and_save_many(tasks, cfg.landing_dir)


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = normalize_json_object(f, "dados")
            if not data.empty:
                df = ColumnSanitizer(data).sanitize_columns_names().df
                dataframes.append(df)

        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)

    if cfg.output_param_file:
        dfs[["id"]].drop_duplicates().to_csv(cfg.output_param_filepath, index=False)
        logger.info(f"📄 IDs exportados para: {cfg.output_param_filepath}")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract,
        transform_fn=transform,
        load_fn=None,
        log=logger,
    )
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="votacoes", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.camara.camara_votacoes
