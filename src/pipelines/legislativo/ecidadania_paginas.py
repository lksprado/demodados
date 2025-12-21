import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup as bs

from ...utils.extractors.https import HttpJsonExtractor
from ...utils.pipeline_cfg import GenericETL, PipelineConfig
from ...utils.transformers.cleaning import ColumnSanitizer
from ...utils.transformers.html_parsers import make_bs_object

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: ecidadania_paginas_raw")


def parser_paginas(soup_object: bs) -> pd.DataFrame:
    soup = soup_object

    container = soup.find("div", id="container-consulta-publica")

    if not container:
        logger.warning(
            """Elemento id="container-consulta-publica" não encontrado. Retornando DataFrame vazio"""
        )
        return pd.DataFrame()

    tipo_map = {
        "ECD": "EMENDA(S) DA CÂMARA DOS DEPUTADOS A PROJETO DE LEI DO SENADO",
        "EDS": "EMENDA(S) DA CÂMARA DOS DEPUTADOS A PROJETO DE DECRETO LEGISLATIVO",
        "MPV": "MEDIDA PROVISÓRIA",
        "PDL": "PROJETO DE DECRETO LEGISLATIVO",
        "PDS": "PROJETO DE DECRETO LEGISLATIVO (SF)",
        "PEC": "PROPOSTA DE EMENDA À CONSTITUIÇÃO",
        "PL": "PROJETO DE LEI",
        "PLC": "PROJETO DE LEI DA CÂMARA",
        "PLP": "PROJETO DE LEI COMPLEMENTAR",
        "PLS": "PROJETO DE LEI DO SENADO",
        "PLV": "PROJETO DE LEI DE CONVERSÃO (CN)",
        "PRS": "PROJETO DE RESOLUÇÃO DO SENADO",
        "SCD": "SUBSTITUTIVO DA CÂMARA DOS DEPUTADOS A PROJETO DE LEI DO SENADO",
        "SDS": "SUBSTITUTIVO DA CÂMARA DOS DEPUTADOS A PROJETO DE DECRETO LEGISLATIVO",
        "SUG": "SUGESTÃO",
    }

    data_extracao = datetime.today().strftime("%Y-%m-%d")

    results = []

    for item in container.find_all("div", class_="resumo-materia"):
        header = item.find("header")
        section = item.find("section")
        figure = item.find("figure", class_="grafico-consulta-publica")

        titulo_tag = header.find("a") if header else None
        descritivo_tag = section.find("a") if section else None

        titulo = titulo_tag.get_text(strip=True) if titulo_tag else None
        descritivo = descritivo_tag.get_text(strip=True) if descritivo_tag else None

        href = (
            titulo_tag["href"] if titulo_tag and titulo_tag.has_attr("href") else None
        )
        full_link = (
            f"https://www12.senado.leg.br/ecidadania/{href.lstrip('/')}"
            if href
            else None
        )

        tipo_proposicao = None
        if titulo:
            sigla = titulo.split()[0]
            tipo_proposicao = tipo_map.get(sigla, "DESCONHECIDO")

        votos_sim = votos_nao = 0
        if figure:
            spans = figure.select("header span")
            if len(spans) >= 2:
                try:
                    votos_sim = int(spans[0].text.replace(".", "").strip())
                    votos_nao = int(spans[1].text.replace(".", "").strip())
                except ValueError:
                    pass

        results.append(
            {
                "dt_extracao": data_extracao,
                "sigla": titulo.split(" ", 1)[0],
                "numero": titulo.split(" ", 1)[1].split("/")[0],
                "ano": titulo.split(" ", 1)[1].split("/")[1],
                "titulo": titulo,
                "tipo_proposicao": tipo_proposicao,
                "descritivo": descritivo,
                "votos_sim": votos_sim,
                "votos_nao": votos_nao,
                "link": full_link,
            }
        )
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    return df


def extraction_paginas(cfg: dict):
    data_extracao = datetime.today().strftime("%Y-%m-%d")
    x = 1
    for x in range(1, 146):
        logger.info(f"Extraindo pagina {x}...")
        filename = f"consultas_publicas_{data_extracao}_page_{x}.csv"
        landing_file = cfg.landing_dir / filename
        extractor = HttpJsonExtractor(logger)
        resp = extractor.make_http_request_text(url=f"{cfg.url_base}{x}")
        soup = make_bs_object(response=resp)
        df = parser_paginas(soup)
        df.to_csv(landing_file, sep=";", index=False)
        x += 1
    logger.info(f"Extracao Completa em {landing_file}")


def transform_paginas(cfg: PipelineConfig) -> Path:
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            data = pd.read_csv(f, sep=";")
            if not data.empty:
                df = ColumnSanitizer(data).sanitize_columns_names().df
                dataframes.append(df)
                df["total_votos"] = df["votos_sim"] + df["votos_nao"]

        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_ecidadania_mais_votados_pipeline(cfg: dict):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extraction_paginas,
        load_fn=None,
        validator=None,
        log=logger,
    )

    # etl.extract()
    transform_paginas(cfg)
    etl.load()


if __name__ == "__main__":
    PIPELINE_ECIDADANIA_PAGINAS_CONFIG_PRD = {
        "url_base": "https://www12.senado.leg.br/ecidadania/principalmateria?p=",
        "landing_dir": "./data/raw/senado/ecidadania/paginas",
        # "landing_file": None,
        "bronze_dir": "./data/bronze/senado/ecidadania/paginas",
        "bronze_file": "ecidadania_paginas_consolidado.csv",
        "db_table": "raw_ecidadania_paginas",
    }

    run_ecidadania_mais_votados_pipeline(
        PipelineConfig(**PIPELINE_ECIDADANIA_PAGINAS_CONFIG_PRD)
    )
