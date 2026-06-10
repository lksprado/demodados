import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup as bs

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.html_parsers import make_bs_object

logger = logging.getLogger("raw_ecidadania_paginas")

_CONFIG_FILE = Path(__file__).parent / "ecidadania_config.yml"

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


def parser_paginas(soup_object: bs) -> pd.DataFrame:
    soup = soup_object
    container = soup.find("div", id="container-consulta-publica")

    if not container:
        logger.warning(
            '⚠️ Elemento id="container-consulta-publica" nao encontrado. Retornando DataFrame vazio.'
        )
        return pd.DataFrame()

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


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    data_extracao = datetime.today().strftime("%Y-%m-%d")
    extractor = HttpJsonExtractor(logger)
    for x in range(1, 146):
        logger.info(f"Extraindo pagina {x}...")
        filename = f"consultas_publicas_{data_extracao}_page_{x}.csv"
        landing_file = cfg.landing_dir / filename
        resp = extractor.make_http_request_text(url=f"{cfg.url_base}{x}")
        soup = make_bs_object(response=resp)
        df = parser_paginas(soup)
        df.to_csv(landing_file, sep=";", index=False)
    logger.info(f"✅ Extracao completa em {cfg.landing_dir}")


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
    dfs["total_votos"] = pd.to_numeric(
        dfs["votos_sim"], errors="coerce"
    ) + pd.to_numeric(dfs["votos_nao"], errors="coerce")
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
    config = load_source_config(_CONFIG_FILE, source="paginas", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.ecidadania.ecidadania_paginas
