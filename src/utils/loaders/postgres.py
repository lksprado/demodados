import logging
import os
from datetime import datetime
from logging import NullHandler
from typing import Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())


class PostgreSQLManager:
    def __init__(
        self,
        db_name=None,
        db_user=None,
        db_password=None,
        db_host=None,
        db_port=None,
        connection=None,  # <- conexão externa (psycopg2)
        engine=None,  # <- engine externa (sqlalchemy)
        log: Optional[logging.Logger] = None,
    ):
        self.external_connection = connection
        self.external_engine = engine
        self.engine = engine

        if (
            not self.external_connection
            and not self.external_engine
            and not self.check_environment_variables()
            and db_name is None
            and db_user is None
            and db_password is None
            and db_host is None
            and db_port is None
        ):
            raise ValueError("CREDENCIAIS DO BANCO NAO FORNECIDAS.")

        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_port = db_port or os.getenv("DB_PORT")

        self.logger = log or logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _connect(self):
        if self.external_connection:
            self.logger.debug("Usando conexao externa (injetada)")
            return self.external_connection

        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            self.logger.debug("Conexao ok.")
            return connection
        except psycopg2.Error as e:
            self.logger.error(f"❌ Erro ao conectar: {e}", exc_info=True)
            return None

    def alchemy(self):
        if self.external_engine:
            self.logger.debug("Usando engine externa (injetada)")
            return self.external_engine

        self.engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        return self.engine

    @staticmethod
    def check_environment_variables():
        logger = logging.getLogger(__name__)
        if (
            not os.getenv("DB_NAME")
            or not os.getenv("DB_USER")
            or not os.getenv("DB_PASSWORD")
            or not os.getenv("DB_HOST")
        ):
            logger.error(
                "❌ Variaveis de ambiente para conexao nao estao configuradas corretamente"
            )
            return False
        else:
            logger.debug("Variaveis de ambiente OK")
            return True

    def send_df_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        how="replace",
        filename=None,
    ):
        """
        Envia um DataFrame para uma tabela no schema **raw** do PostgreSQL.
        """
        logger = self.logger
        if filename:
            df["arquivo_origem"] = filename
        df["data_carga"] = datetime.now()

        engine = self.alchemy()
        try:
            df.to_sql(
                name=table_name, con=engine, schema="raw", if_exists=how, index=False
            )
            logger.info(f"✅ Dados inseridos em raw.{table_name}")

        except Exception as e:
            logger.error(f"❌ Erro ao inserir no banco: {e}", exc_info=True)
            raise

    def send_csv_to_db(
        self,
        csv_path,
        table_name: str,
        filename=None,
        sep: str = ";",
        chunksize: int = 50_000,
        how: str = "replace",
    ):
        """Carrega um CSV grande para **raw.<table_name>** em blocos (streaming).

        Le e insere o CSV em chunks de ``chunksize`` linhas em vez de materializar
        o arquivo inteiro em memoria, evitando OOM com bronzes grandes. O primeiro
        bloco usa ``how`` (replace por padrao) e os demais fazem append, mantendo a
        inferencia de tipos por coluna (consistente com ``send_df_to_db``).
        """
        logger = self.logger
        engine = self.alchemy()
        total = 0
        first = True
        try:
            for chunk in pd.read_csv(csv_path, sep=sep, chunksize=chunksize):
                if filename:
                    chunk["arquivo_origem"] = filename
                chunk["data_carga"] = datetime.now()
                # Mantem o INSERT abaixo do limite de 65535 parametros do Postgres.
                rows_per_insert = max(1, 60_000 // max(1, chunk.shape[1]))
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema="raw",
                    if_exists=how if first else "append",
                    index=False,
                    method="multi",
                    chunksize=rows_per_insert,
                )
                total += len(chunk)
                first = False

            if first:
                # CSV sem linhas (so cabecalho): cria a tabela vazia mesmo assim.
                header = pd.read_csv(csv_path, sep=sep, nrows=0)
                if filename:
                    header["arquivo_origem"] = pd.Series(dtype="object")
                header["data_carga"] = pd.Series(dtype="datetime64[ns]")
                header.to_sql(
                    name=table_name,
                    con=engine,
                    schema="raw",
                    if_exists=how,
                    index=False,
                )

            logger.info(f"✅ {total} linhas inseridas em raw.{table_name}")
        except Exception as e:
            logger.error(f"❌ Erro ao inserir CSV no banco: {e}", exc_info=True)
            raise

    def execute_query(self, query: str):
        logger = self.logger
        try:
            if self.engine:
                with self.engine.connect() as conn:
                    conn.execute(text(query))  # <- usa SQLAlchemy
                    logger.info("✅ Query executada com sucesso")
            else:
                # fallback para psycopg2
                connection = self._connect()
                cursor = connection.cursor()
                cursor.execute(query)
                cursor.close()
                connection.commit()
                connection.close()
                logger.info("✅ Query executada com sucesso")
        except Exception as e:
            logger.error(f"❌ Erro ao executar query: {e}", exc_info=True)
            raise

    def fetchone(self, query: str):
        try:
            if self.engine:
                # SQLAlchemy modo
                with self.engine.connect() as conn:
                    result = conn.execute(text(query)).fetchone()
                    return result
            else:
                # fallback para psycopg2
                conn = self._connect()
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                conn.close()
                return result
        except Exception as e:
            self.logger.error(f"❌ Erro no fetchone: {e}", exc_info=True)
            raise


def psyco_test():
    import psycopg2

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
    print("✅ Conectado com sucesso")
    conn.close()


if __name__ == "__main__":
    pg = PostgreSQLManager()
    connect = pg._connect()
    # psyco_test()
