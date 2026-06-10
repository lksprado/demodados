import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("raw_senado_votos_senadores")

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


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao de votos...")

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
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue

    if not dataframes:
        logger.warning("⚠️ Nenhum dado de votos encontrado.")
        return

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"💾 Votos salvos em: {cfg.bronze_filepath} ({len(dfs)} linhas)")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=None,
        load_fn=None,
        log=logger,
    )

    transform(cfg)
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="votos_senadores", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_votos_senadores
