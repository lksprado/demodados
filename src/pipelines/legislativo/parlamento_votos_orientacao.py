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

from ...utils.extractors.https import HttpJsonExtractor
from ...utils.logger import logger_setting
from ...utils.pipeline_cfg import GenericETL, PipelineConfig
from ...utils.transformers.cleaning import ColumnSanitizer

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    force=True,  # garante que qualquer configuração anterior seja sobrescrita
)

logger = logger_setting(
    "Pipeline: raw_parlamento_votacoes_orientacao",
    log_file="logs/parlamento_orientacao_votos.log",
)


_PARAMS_DIR = Path(__file__).parents[2] / "params"


def _sem_dados_path(cfg: PipelineConfig):
    return _PARAMS_DIR / f"ids_sem_dados_{cfg.landing_dir.name}.csv"


def extract_votos(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)

    all_ids = pd.read_csv(cfg.parameter_file, sep=";")["id"].astype(str).tolist()

    ids_existentes = {
        f.stem.removesuffix("_votacoes_orientacao")
        for f in cfg.landing_dir.iterdir()
        if f.suffix == ".json"
    }

    sem_dados_file = _sem_dados_path(cfg)
    ids_sem_dados = (
        set(pd.read_csv(sem_dados_file)["id"].astype(str).tolist())
        if sem_dados_file.exists()
        else set()
    )

    ids_novos = [
        i for i in all_ids if i not in ids_existentes and i not in ids_sem_dados
    ]
    logger.info(
        f"{len(ids_novos)} novas votações para extrair "
        f"(total: {len(all_ids)}, existentes: {len(ids_existentes)}, sem dados: {len(ids_sem_dados)})."
    )

    for vot_id in ids_novos:
        url = f"{cfg.url_base}{vot_id}/orientacoes"
        output_file = f"{vot_id}_votacoes_orientacao.json"
        data = extractor.make_http_request(url)
        if data and data.get("dados"):
            extractor.save_response(data, cfg.landing_dir, output_file)
        elif data is not None:
            logger.warning(
                f"Sem dados para votacao {vot_id}, registrando para ignorar nas próximas runs."
            )
            write_header = not sem_dados_file.exists()
            with open(sem_dados_file, "a", encoding="utf-8", newline="") as f:
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


def run_deputados_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_votos,
        load_fn=None,
        validator=None,
        log=logger,
    )

    etl.extract()
    transform_votos(cfg)
    # etl.validate()
    etl.load()


if __name__ == "__main__":
    PIPELINE_votacoes_orientacao_PRD = {
        "parameter_file": "./src/params/full_votos_deputados_id.csv",
        "url_base": "https://dadosabertos.camara.leg.br/api/v2/votacoes/",
        "landing_dir": "/media/lucas/Files/2.Projetos/0.mylake/raw/demodados/camara/votacoes_orientacao",
        "bronze_dir": "/media/lucas/Files/2.Projetos/0.mylake/bronze/demodados/camara/votacoes_orientacao",
        "error_dir": "./data/error/camara/votacoes_orientacao/",
        "bronze_file": "parlamento_votacoes_orientacao.csv",
        "db_table": "raw_parlamento_votacoes_orientacao",
    }

    run_deputados_pipeline(PipelineConfig(**PIPELINE_votacoes_orientacao_PRD))
    # python -m src.pipelines.legislativo.parlamento_votacoes_orientacao
