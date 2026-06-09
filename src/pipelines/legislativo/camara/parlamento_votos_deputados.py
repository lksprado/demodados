"""
Pipeline — Deputados (Câmara dos Deputados)

Extrai, transforma, valida e carrega dados detalhados de deputados a partir da API:
https://dadosabertos.camara.leg.br/api/v2/deputados/{id}

Fluxo:
1) extract_deputados(cfg): baixa um JSON por deputado em data/landing (opcional neste run).
2) transform_deputados(cfg): normaliza os JSONs, consolida e salva CSV em data/bronze.
3) etl.validate(): reabre o CSV bronze e valida com Pandera (DeputadoSchema).
4) etl.load(): insere o CSV bronze na tabela Postgres definida em cfg.db_table.

Requisitos:
- PostgreSQL acessível e configurado no PostgreSQLManager.
- Schema Pandera: DeputadoSchema.
- PipelineConfig com: parameter_file (IDs), url_base, landing_dir, bronze_dir, bronze_file, db_table.

Observações:
- O script configura logging no entry-point (INFO). Em Airflow, remova basicConfig e use o logger do Airflow.
- O separador do CSV bronze é ';' e deve ser consistente em transform/validate/load.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from ....utils.extractors.https import HttpJsonExtractor
from ....utils.logger import logger_setting
from ....utils.pipeline_cfg import GenericETL, PipelineConfig, load_source_config
from ....utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,
)

logger = logger_setting(
    "Pipeline: raw_parlamento_votos_deputados", log_file="logs/parlamento_votos.log"
)

_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"
_SEM_DADOS_FILE = "sem_dados_id_votacao.csv"


def extract_votos(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    todos_ids = pd.read_csv(cfg.parameter_filepath)[["id"]].astype(str)

    ids_landing = pd.DataFrame(
        {
            "id": [
                f.stem.removesuffix("_votos_deputados")
                for f in cfg.landing_dir.iterdir()
                if f.suffix == ".json"
            ]
        }
    )

    sem_dados_path = cfg.parameter_dir / _SEM_DADOS_FILE
    ids_sem_dados = (
        pd.read_csv(sem_dados_path)[["id"]].astype(str)
        if sem_dados_path.exists()
        else pd.DataFrame(columns=["id"])
    )

    ja_processados = pd.concat(
        [ids_landing, ids_sem_dados], ignore_index=True
    ).drop_duplicates()

    pendentes = (
        todos_ids.merge(ja_processados, on="id", how="left", indicator=True)
        .query('_merge == "left_only"')["id"]
        .tolist()
    )

    logger.info(
        f"{len(pendentes)} votações pendentes "
        f"(total: {len(todos_ids)}, já extraídas: {len(ids_landing)}, sem dados: {len(ids_sem_dados)})."
    )

    for vot_id in pendentes:
        url = f"{cfg.url_base}{vot_id}/votos"
        data = extractor.make_http_request(url)
        if data and data.get("dados"):
            extractor.save_response(
                data, cfg.landing_dir, f"{vot_id}_votos_deputados.json"
            )
        elif data is not None:
            logger.warning(
                f"Sem dados para votacao {vot_id}, registrando para ignorar nas próximas runs."
            )
            write_header = not sem_dados_path.exists()
            with open(sem_dados_path, "a", encoding="utf-8", newline="") as f:
                if write_header:
                    f.write("id\n")
                f.write(f"{vot_id}\n")


def transform_votos(cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            with open(f, "r", encoding="utf-8") as fp:
                raw = json.load(fp)

            dados = raw.get("dados", [])
            if not dados:
                continue

            uri_self = next(
                (
                    lnk["href"]
                    for lnk in raw.get("links", [])
                    if lnk.get("rel") == "self"
                ),
                None,
            )

            df = pd.json_normalize(dados, sep=".")
            df["url_votos"] = uri_self
            df = ColumnSanitizer(df).sanitize_columns_names().df
            dataframes.append(df)

        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_votos,
        load_fn=None,
        validator=None,
        log=logger,
    )

    # etl.extract()
    transform_votos(cfg)
    # etl.validate()
    etl.load()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="votos_deputados", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.parlamento_votos_deputados
