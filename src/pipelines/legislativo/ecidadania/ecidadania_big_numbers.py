import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup as bs

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.html_parsers import make_bs_object

logger = logging.getLogger("raw_ecidadania_bignumbers")

_CONFIG_FILE = Path(__file__).parent / "ecidadania_config.yml"


def parser_big_numbers(soup_object: bs) -> pd.DataFrame:
    soup = soup_object
    container = soup.find("div", id="container-consulta-publica")
    if not container:
        logger.warning(
            'Elemento id="container-consulta-publica" nao encontrado. Retornando DataFrame vazio.'
        )
        return pd.DataFrame()

    boxes = container.select("div.box-est-cp > header")

    if len(boxes) < 3:
        logger.warning(
            "⚠️ Nao foi possivel identificar os 3 contadores. Retornando DataFrame vazio."
        )
        return pd.DataFrame()

    def parse_num(text):
        return int(text.replace(".", "").strip())

    total_proposicoes_votadas = parse_num(boxes[0].text)
    total_pessoas_votaram = parse_num(boxes[1].text)
    total_votos_registrados = parse_num(boxes[2].text)

    data_extracao = datetime.today().strftime("%Y-%m-%d")

    df = pd.DataFrame(
        [
            {
                "total_proposicoes_votadas": total_proposicoes_votadas,
                "total_pessoas_votaram": total_pessoas_votaram,
                "total_votos_registrados": total_votos_registrados,
                "dt_extracao": data_extracao,
            }
        ]
    )
    df.columns = df.columns.str.lower()
    return df


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    data_extracao = datetime.today().strftime("%Y-%m-%d")
    filename = f"big_numbers_{data_extracao}.csv"
    landing_file = cfg.landing_dir / filename
    extractor = HttpJsonExtractor(logger)
    resp = extractor.make_http_request_text(url=cfg.url_base)
    soup = make_bs_object(response=resp)
    df = parser_big_numbers(soup)
    df.to_csv(landing_file, sep=";", index=False)
    logger.info(f"✅ Extracao completa em {landing_file}")


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = pd.read_csv(f, sep=";", dtype=str)
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
    config = load_source_config(_CONFIG_FILE, source="big_numbers", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.ecidadania.ecidadania_big_numbers
