import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logger = logging.getLogger("raw_senado_votos_orientacao")

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


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpJsonExtractor(logger)

    for y in range(2001, 2027):
        logger.info(f"Extraindo {y}...")
        base_params = f"{y}0101/{y}1231?v=1"

        data = extractor.make_http_request(f"{cfg.url_base}{base_params}")
        if not data:
            logger.warning(f"⚠️ Sem dados para {y}.")
            continue
        extractor.save_response(
            data, cfg.landing_dir, f"{y}_senado_votacoes_orientacao"
        )


def transform(cfg: PipelineConfig):
    logger.info("🔄 Iniciando transformacao de orientacoes...")

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
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue

    if not dataframes:
        logger.warning("⚠️ Nenhum dado de orientacoes encontrado.")
        return

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)
    logger.info(f"💾 Orientacoes salvas em: {cfg.bronze_filepath} ({len(dfs)} linhas)")


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract,
        load_fn=None,
        log=logger,
    )

    # etl.extract()
    transform(cfg)
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="votos_orientacao", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_votos_orientacao
