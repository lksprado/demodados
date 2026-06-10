"""
Pipeline — Orientação de Bancada por Votação (Senado Federal)

Extrai e transforma a orientação de voto de cada bancada/liderança
por votação plenária, a partir da API do Senado.

Fluxo:
1) full_extract_orientacao(cfg): baixa JSONs em data/landing/votos_orientacao/.
2) transform_votos_orientacao(cfg): explode orientacoesLideranca,
   gerando uma linha por partido por votação, salva CSV em data/bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,
)

logger = logging.getLogger("Pipeline: raw_senado_votos_orientacao")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"

_PARENT_COLS = [
    "codigoVotacaoSve",
    "siglaTipoMateria",
    "numeroMateria",
    "anoMateria",
    "dataInicioVotacao",
    "dataTerminoVotacao",
    "descricaoVotacao",
    "qtdVotosSim",
    "qtdVotosNao",
    "qtdVotosAbstencao",
]


def full_extract_orientacao(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    for y in range(2001, 2027):
        logger.info(f"Extraindo {y}...")
        base_params = f"{y}0101/{y}1231?v=1"

        data = extractor.make_http_request(f"{cfg.url_base}{base_params}")
        if not data:
            logger.warning(f"Sem dados para {y}.")
            continue
        extractor.save_response(
            data, cfg.landing_dir, f"{y}_senado_votacoes_orientacao"
        )


def transform_votos_orientacao(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao de Orientacoes...")

    dataframes = []
    for f in cfg.landing_dir.iterdir():
        if f.suffix != ".json":
            continue
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)

            rows = []
            for votacao in data.get("votacoes", []):
                orientacoes = votacao.get("orientacoesLideranca") or []
                parent = {k: votacao.get(k) for k in _PARENT_COLS}
                for orientacao in orientacoes:
                    rows.append({**parent, **orientacao})

            if rows:
                df = ColumnSanitizer(pd.DataFrame(rows)).sanitize_columns_names().df
                dataframes.append(df)

        except Exception as e:
            logger.error(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    if not dataframes:
        logger.warning("Nenhum dado de orientacoes encontrado.")
        return

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"Orientacoes salvas em: {cfg.bronze_filepath} ({len(dfs)} linhas)")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=full_extract_orientacao,
        load_fn=None,
        validator=None,
        log=logger,
    )

    # etl.extract()
    transform_votos_orientacao(cfg)
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="votos_orientacao", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_votos_orientacao
