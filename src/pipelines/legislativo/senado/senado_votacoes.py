"""
Pipeline — Senadores (Senado Federal)

Extrai, transforma, valida e carrega dados sessões a partir da API:
https://legis.senado.leg.br/dadosabertos/senador/lista/legislatura/

Fluxo:
1) etl.extract(): baixa o JSON em data/landing usando o extractor genérico.
2) transform_senadores(cfg): normaliza o JSON, sanitiza colunas e salva CSV em data/bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- PipelineConfig com: url_base, landing_dir, landing_file, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (nível INFO). Em Airflow, não use basicConfig; use o logger do Airflow.
- O CSV bronze usa separador ';' e deve ser lido com o mesmo sep em validate/load.
"""

import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer
from ....utils.transformers.json_parsers import normalize_json_object

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logging.getLogger("Pipeline: raw_senado_votacoes")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"


def full_extract_votacoes(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    for y in range(2001, 2027):
        logger.info(f"Extraindo {y}...")
        base_params = f"dataInicio={y}-01-01&dataFim={y}-12-31&v=1"

        data = extractor.make_http_request(f"{cfg.url_base}?{base_params}")
        if not data:
            logger.warning(f"Sem dados para {y}.")
            continue
        extractor.save_response(data, cfg.landing_dir, f"{y}_senado_votacoes")


def transform_votacoes(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
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

        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
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
            logger.info(f"IDs exportados para: {cfg.output_param_filepath}")
        else:
            logger.warning(f"Coluna '{id_col}' não encontrada — IDs não exportados.")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=full_extract_votacoes,
        load_fn=None,
        validator=None,
        log=logger,
    )

    # etl.extract()
    transform_votacoes(cfg)
    # etl.validate()
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="votacoes", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_sessoes
