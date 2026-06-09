"""
Pipeline — Big Numbers (E-Cidadania / Senado Federal)

Extrai, transforma e carrega os contadores gerais da plataforma e-Cidadania:
https://www12.senado.leg.br/ecidadania/principalmateria

Fluxo:
1) extract_big_numbers(cfg): requisita a página HTML e salva CSV em data/landing.
2) transform_big_numbers(cfg): consolida os CSVs de landing em bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Observações:
- O script configura logging no entry-point (INFO). Em Airflow, remova basicConfig e use o logger do Airflow.
- O separador do CSV bronze é ';' e deve ser consistente em transform/validate/load.
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup as bs

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.html_parsers import make_bs_object

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_ecidadania_big_numbers")

_CONFIG_FILE = Path(__file__).parent / "ecidadania_config.yml"


def parser_big_numbers(soup_object: bs) -> pd.DataFrame:
    soup = soup_object
    container = soup.find("div", id="container-consulta-publica")
    if not container:
        logger.warning(
            """Elemento id="container-consulta-publica" não encontrado. Retornando DataFrame vazio"""
        )
        return pd.DataFrame()

    boxes = container.select("div.box-est-cp > header")

    if len(boxes) < 3:
        logger.warning(
            "Não foi possível identificar os 3 contadores. Retornando DataFrame vazio"
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


def extract_big_numbers(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    data_extracao = datetime.today().strftime("%Y-%m-%d")
    filename = f"big_numbers_{data_extracao}.csv"
    landing_file = cfg.landing_dir / filename
    extractor = HttpJsonExtractor(logger)
    resp = extractor.make_http_request_text(url=cfg.url_base)
    soup = make_bs_object(response=resp)
    df = parser_big_numbers(soup)
    df.to_csv(landing_file, sep=";", index=False)
    logger.info(f"Extracao Completa em {landing_file}")


def transform_big_numbers(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = pd.read_csv(f, sep=";", dtype=str)
            if not data.empty:
                df = ColumnSanitizer(data).sanitize_columns_names().df
                dataframes.append(df)
        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"CSV SALVO EM: {cfg.bronze_filepath}")


def run_pipeline(cfg: PipelineConfig):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_big_numbers,
        load_fn=None,
        validator=None,
        log=logger,
    )

    etl.extract()
    transform_big_numbers(cfg)
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="big_numbers", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.ecidadania.ecidadania_big_numbers
