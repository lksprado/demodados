import shutil
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.utils.logger_cfg import logger
from sqlalchemy import create_engine


def execute_query(query: str):
    postgres_hook = PostgresHook(postgres_conn_id="demodados_dw")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()


def send_csv_df_to_db(file_path: Path, table_name, schema, how="replace"):
    """Envia um Dataframe Pandas para camada raw interpretando como texto.

    Args:
        dataframe (pd.DataFrame): Dados
        table_name (_type_): Tabela no BD
        schema (_type_): Schema no DB
        how (str, optional): _description_. Defaults to 'replace'.
    """
    logger.info(f"Iniciando carga na tabela {schema}.{table_name}")
    postgres_hook = PostgresHook(postgres_conn_id="demodados_dw")
    engine = create_engine("postgresql+psycopg2://", creator=postgres_hook.get_conn)
    df = pd.read_csv(file_path, dtype=str)
    try:
        df.to_sql(table_name, engine, if_exists=how, index=False, schema=schema)
    except:
        raise logger.error(f"Falha na carga de Dataframe em: {schema}.{table_name}")


def move_files_after_loading(staging_dir: Path, bronze_dir: Path):
    staging_dir = Path(staging_dir)
    bronze_dir = Path(bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)  # garante destino
    # Lista apenas arquivos JSON
    json_files = list(staging_dir.glob("*.json"))

    if not json_files:
        logger.info("Nenhum arquivo JSON encontrado para mover.")
        return 0

    for file in json_files:
        dst_path = bronze_dir / file.name
        shutil.move(str(file), str(dst_path))  # move precisa de str ou Path
        logger.info(f"Movido {file.name} para Bronze")

    remaining = len(list(staging_dir.glob("*.json")))
    logger.info(
        f"{len(json_files)} arquivos movidos. {remaining} arquivos restantes em {staging_dir}"
    )

    csv_files = list(staging_dir.glob("*.csv"))
    if not csv_files:
        logger.info("Nenhum arquivo CSV encontrado para remover.")
        return 0

    for f in csv_files:
        f.unlink()  # remove o arquivo
        logger.info(f"Arquivo removido: {f.name}")


def export_table_to_csv(output_dir: Path, filename, query):
    output_dir = Path(output_dir)
    postgres_hook = PostgresHook(postgres_conn_id="demodados_dw")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df_export = pd.DataFrame(rows, columns=colnames)
    file_dest = output_dir / filename
    df_export.to_csv(file_dest, index=False)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Tabela exportada em csv para: {output_dir}{filename}")
