"""
Pipeline — votacoes (Câmara dos votacoes)

Extrai, transforma, valida e carrega dados detalhados de votacoes a partir da API:
https://dadosabertos.camara.leg.br/api/v2/votacoes/

Fluxo:
1) extract_votacoes(cfg): baixa um JSON por deputado em data/landing (opcional neste run).
2) transform_votacoes(cfg): normaliza os JSONs, consolida e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (votacoeschema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: votacoeschema.
- PipelineConfig com: parameter_file (IDs), url_base, landing_dir, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (INFO). Em Airflow, remova basicConfig e use o logger do Airflow.
- O separador do CSV bronze é ';' e deve ser consistente em transform/validate/load.
"""

import logging
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

from ...utils.extractors.https import HttpJsonExtractor
from ...utils.pipeline_cfg import GenericETL, PipelineConfig
from ...utils.transformers.cleaning import ColumnSanitizer
from ...utils.transformers.json_parsers import normalize_json_object

# from .schema import votacoeschema

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_parlamento_votacoes")

cols_to_not_sanitize_values = [
    # "uri",
    # "urlwebsite",
    # "redesocial",
    # "datanascimento",
    # "datafalecimento",
    # "ultimostatus_uri",
    # "ultimostatus_uripartido",
    # "ultimostatus_urlfoto",
    # "ultimostatus_email",
    # "ultimostatus_data",
    # "ultimostatus_gabinete_telefone",
    # "ultimostatus_gabinete_email",
    # "arquivo_origem",
    # "data_carga",
]


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


def extract_votacoes(cfg: PipelineConfig):
    logger.info("Iniciando Extracao do quarter atual...")
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
        logger.warning(f"Sem dados para {y}-{label}.")
        return

    last_page = _get_last_page(first_page_data.get("links", []))
    logger.info(f"{y}-{label}: {last_page} página(s)")

    dados = first_page_data.get("dados", [])
    if dados:
        extractor.save_response(
            {"dados": dados}, cfg.landing_dir, f"votacoes_{y}_{label}_1.json"
        )

    for p in range(2, last_page + 1):
        page_data = extractor.make_http_request(
            f"{cfg.url_base}?{base_params}&pagina={p}"
        )
        if page_data:
            page_dados = page_data.get("dados", [])
            if page_dados:
                extractor.save_response(
                    {"dados": page_dados},
                    cfg.landing_dir,
                    f"votacoes_{y}_{label}_{p}.json",
                )


def full_extract_votacoes(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    min_year = 2021
    max_year = 2026

    for y in range(min_year, max_year + 1):
        for inicio, fim, label in QUARTERS:
            logger.info(f"Extraindo {y}-{label}...")
            base_params = (
                f"dataInicio={y}-{inicio}&dataFim={y}-{fim}"
                f"&itens=100&ordem=DESC&ordenarPor=dataHoraRegistro"
            )

            pg1_file = f"votacoes_{y}_{label}_1.json"
            pg1_path = Path(cfg.landing_dir) / pg1_file

            first_page_data = extractor.make_http_request(
                f"{cfg.url_base}?{base_params}&pagina=1"
            )
            if not first_page_data:
                logger.warning(f"Sem dados para {y}-{label}, pulando.")
                continue

            last_page = _get_last_page(first_page_data.get("links", []))
            logger.info(f"{y}-{label}: {last_page} página(s)")

            if not pg1_path.exists():
                dados = first_page_data.get("dados", [])
                if dados:
                    extractor.save_response({"dados": dados}, cfg.landing_dir, pg1_file)
            else:
                logger.info(f"Já existe, pulando: {pg1_file}")

            for p in range(2, last_page + 1):
                output_file = f"votacoes_{y}_{label}_{p}.json"
                if (Path(cfg.landing_dir) / output_file).exists():
                    logger.info(f"Já existe, pulando: {output_file}")
                    continue

                page_data = extractor.make_http_request(
                    f"{cfg.url_base}?{base_params}&pagina={p}"
                )
                if page_data:
                    page_dados = page_data.get("dados", [])
                    if page_dados:
                        extractor.save_response(
                            {"dados": page_dados},
                            cfg.landing_dir,
                            output_file,
                        )


def transform_votacoes(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = normalize_json_object(f, "dados")
            if not data.empty:
                df = (
                    ColumnSanitizer(data).sanitize_columns_names()
                    # .not_sanitize_columns_values(cols=cols_to_not_sanitize_values)
                    .df
                )
                dataframes.append(df)

        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_votacoes_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=full_extract_votacoes,
        load_fn=None,
        validator=None,
        log=logger,
    )

    etl.extract()
    transform_votacoes(cfg)
    etl.validate()
    etl.load()


if __name__ == "__main__":
    PIPELINE_VOTACOES_PRD = {
        "url_base": "https://dadosabertos.camara.leg.br/api/v2/votacoes",
        "landing_dir": "./data/raw/camara/votacoes/",
        "bronze_dir": "./data/bronze/camara/votacoes/",
        "error_dir": "./data/error/camara/votacoes/",
        "bronze_file": "parlamento_votacoes.csv",
        "db_table": "raw_parlamento_votacoes",
    }

    run_votacoes_pipeline(PipelineConfig(**PIPELINE_VOTACOES_PRD))
    # python -m src.pipelines.legislativo.parlamento_votacoes
