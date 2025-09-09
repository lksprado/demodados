import logging
import re

import pandas as pd
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import DeputadoSchema
from src.utils.extractors.https import HttpJsonExtractor
from src.utils.loaders.postgres import PostgreSQLManager
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer
from src.utils.transformers.json_parsers import normalize_json_object

logger = logging.getLogger("Pipeline: parlamento_deputados_raw")

cols_to_not_sanitize_values = [
    "uri",
    "urlwebsite",
    "redesocial",
    "datanascimento",
    "datafalecimento",
    "ultimostatus_uri",
    "ultimostatus_uripartido",
    "ultimostatus_urlfoto",
    "ultimostatus_email",
    "ultimostatus_data",
    "ultimostatus_gabinete_telefone",
    "ultimostatus_gabinete_email",
    "arquivo_origem",
    "data_carga",
]


#### HELPER
#### EXTRACT RECURSIVO
def extract_deputados(cfg: PipelineConfig):
    logger.info("Iniciando Extracao...")
    extractor = HttpJsonExtractor(logger)
    df_ids = pd.read_csv(cfg.parameter_file)
    for dep_id in df_ids["id"].to_list():
        url = f"{cfg.url_base}{dep_id}"
        output_file = f"{dep_id}_deputado.json"
        data = extractor.make_http_request(url)
        if data:
            extractor.save_response(data, cfg.landing_dir, output_file)
    return None


def transform_deputados(df, cfg: PipelineConfig):
    logger.info("Iniciando Transformacao...")
    dataframes = []
    for f in cfg.landing_dir.iterdir():
        try:
            dep_id = re.match(r"^\d+", f.name).group()
            data = normalize_json_object(f, "dados")
            if not data.empty:
                df = (
                    ColumnSanitizer(data)
                    .sanitize_columns_names()
                    .not_sanitize_columns_values(cols=cols_to_not_sanitize_values)
                    .df
                )
                output_file = f"{dep_id}_deputado.csv"
                file_destination = cfg.bronze_dir / output_file
                df.to_csv(file_destination, sep=";", index=False)
                logger.info(f"CSV SALVO EM: {file_destination}")
                dataframes.append(df)
        except Exception as e:
            print(f"ERRO AO TRANSFORMAR {f} --- {e}")
            continue

    if not dataframes:
        raise RuntimeError("NENHUM DATAFRAME ENCONTRADO")

    return pd.concat(dataframes, ignore_index=True)


def run_deputados_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract_deputados,
        transform_fn=transform_deputados,
        validate_fn=None,
        load_fn=None,
        validator=DeputadoSchema,
        log=logger,
    )

    etl.extract()
    df = etl.transform()
    df = etl.validate(df)
    etl.load(df)


if __name__ == "__main__":
    cfg = PipelineConfig(
        parameter_file="./src/params/id_deputados.csv",
        url_base="https://dadosabertos.camara.leg.br/api/v2/deputados/",
        landing_dir="./data/landing/camara/deputados/",
        bronze_dir="./data/bronze/camara/deputados/",
        error_dir="./data/error/camara/deputados/",
        db_table="parlamento_deputados_raw",
    )

    run_deputados_pipeline(cfg)
