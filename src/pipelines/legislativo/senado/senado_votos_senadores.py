"""
Pipeline — Votos Individuais por Senador (Senado Federal)

Lê os mesmos JSONs de landing do pipeline senado_votacoes e expande
o array 'votos', gerando uma linha por senador por votação.

Fluxo:
1) Sem extração própria — consome landing_dir de votacoes.
2) transform_votos_senadores(cfg): explode votos, salva CSV em data/bronze.
3) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,
)

logger = logging.getLogger("Pipeline: raw_senado_votos_senadores")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"

_PARENT_COLS = [
    "codigoSessaoVotacao",
    "codigoSessao",
    "dataSessao",
    "identificacao",
    "sigla",
    "numero",
    "ano",
    "resultadoVotacao",
]


def transform_votos_senadores(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao de Votos...")

    # Os JSONs ficam na landing de votacoes, não neste subpath
    votacoes_landing = cfg.landing_dir.parent / "votacoes"

    dataframes = []
    for f in votacoes_landing.iterdir():
        if not f.suffix == ".json":
            continue
        try:
            with open(f, "r", encoding="utf-8") as fh:
                records = json.load(fh)

            rows = []
            for votacao in records:
                votos = votacao.get("votos") or []
                parent = {k: votacao.get(k) for k in _PARENT_COLS}
                for voto in votos:
                    rows.append({**parent, **voto})

            if rows:
                df = ColumnSanitizer(pd.DataFrame(rows)).sanitize_columns_names().df
                dataframes.append(df)

        except Exception as e:
            logger.error(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    if not dataframes:
        logger.warning("Nenhum dado de votos encontrado.")
        return

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"Votos salvos em: {cfg.bronze_filepath} ({len(dfs)} linhas)")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        validator=None,
        log=logger,
    )

    transform_votos_senadores(cfg)
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="votos_senadores", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_votos_senadores
