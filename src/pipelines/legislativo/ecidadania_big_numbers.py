import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup as bs

from src.utils.extractors.https import HttpJsonExtractor
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer
from src.utils.transformers.html_parsers import make_bs_object

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: ecidadania_bignumbers_raw")


def parser_big_numbers(soup_object: bs) -> pd.DataFrame:
    soup = soup_object
    container = soup.find("div", id="container-consulta-publica")
    if not container:
        logger.warning(
            """Elemento id="container-consulta-publica" não encontrado. Retornando DataFrame vazio"""
        )
        return pd.DataFrame()

    # encontra todas as divs que contêm os números
    boxes = container.select("div.box-est-cp > header")

    # Se não achar 3 números, retorna vazio
    if len(boxes) < 3:
        logger.warning(
            "Não foi possível identificar os 3 contadores. Retornando DataFrame vazio"
        )
        return pd.DataFrame()

    # Remove pontos de milhares, converte para int
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


def extraction_big_numbers(cfg: dict):
    data_extracao = datetime.today().strftime("%Y-%m-%d")
    filename = f"big_numbers_{data_extracao}.csv"
    landing_file = cfg.landing_dir / filename
    extractor = HttpJsonExtractor(logger)
    resp = extractor.make_http_request_text(url=cfg.url_base)
    soup = make_bs_object(response=resp)
    df = parser_big_numbers(soup)
    df.to_csv(landing_file, sep=";", index=False)
    logger.info(f"Extracao Completa em {landing_file}")


def transform_bignumbers(cfg: PipelineConfig) -> Path:
    data = []
    for f in cfg.landing_dir.iterdir():
        df = pd.read_csv(f, sep=";", dtype=str)
        df = ColumnSanitizer(df).sanitize_columns_names().df
        data.append(df)
    df = pd.concat(data, ignore_index=False)
    df.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")


def run_ecidadania_bignumbers_pipeline(cfg: dict):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extraction_big_numbers,
        load_fn=None,
        validator=None,
        log=logger,
    )

    # etl.extract()
    transform_bignumbers(cfg)
    etl.load()


if __name__ == "__main__":
    PIPELINE_ECIDADANIA_BIGNUMBER_CONFIG_PRD = {
        "url_base": "https://www12.senado.leg.br/ecidadania/principalmateria",
        "landing_dir": "./data/raw/senado/ecidadania/big_numbers",
        # "landing_file": None,
        "bronze_dir": "./data/bronze/senado/ecidadania/big_numbers",
        "bronze_file": "ecidadania_bignumbers_consolidado.csv",
        "db_table": "raw_ecidadania_bignumbers",
    }

    run_ecidadania_bignumbers_pipeline(
        PipelineConfig(**PIPELINE_ECIDADANIA_BIGNUMBER_CONFIG_PRD)
    )
