import logging
import os
from datetime import datetime
from typing import Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # take environment variables


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

        if log is None:
            logging.basicConfig(
                format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO,
            )
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = log

    def _connect(self):
        if self.external_connection:
            self.logger.debug("USANDO CONEXÃO EXTERNA (injetada)")
            return self.external_connection

        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            self.logger.debug("CONEXAO OK.")
            return connection
        except psycopg2.Error as e:
            self.logger.error(f"ERRO AO CONECTAR: {e}")
            return None

    def execute_query(self, query):
        try:
            connection = self._connect()
            if connection:
                cursor = connection.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                connection.commit()
                connection.close()
                return result
            else:
                self.logger.error("ERRO AO CONECTAR")
                return None
        except psycopg2.Error as e:
            self.logger.error(f"ERRO AO EXECUTAR QUERY: {e}")
            return None

    def execute_insert(self, query, values):
        try:
            connection = self._connect()
            if connection:
                cursor = connection.cursor()
                cursor.execute(query, values)
                connection.commit()
                cursor.close()
                connection.close()
                self.logger.debug("INSERT OK")
            else:
                self.logger.error("ERRO AO CONECTAR")
        except psycopg2.Error as e:
            self.logger.error(f"ERRO NO INSERT --- {e}")

    def alchemy(self):
        if self.external_engine:
            self.logger.debug("USANDO ENGINE EXTERNA (injetada)")
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
                "VARIAVEIS DE AMBIENTE PARA CONEXAO NAO ESTAO CONFIGURADAS CORRETAMENTE"
            )
            return False
        else:
            logger.debug("VARIAVEIS DE AMBIENTE OK")
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
            logger.info(f"✅ DADOS INSERIDOS EM raw.{table_name}")

        except Exception as e:
            logger.error(f"❌ ERRO AO INSERIR NO BANCO: {e}", exc_info=True)
            raise

    def execute_query(self, query: str):
        logger = self.logger

        try:
            connection = self._connect()  # usa self.external_connection se tiver
            cursor = connection.cursor()
            cursor.execute(query)
            cursor.close()
            connection.commit()
            connection.close()
            logger.info(f"QUERY EXECUTADA COM SUCESSO")

        except Exception as e:
            logger.error(f"❌ ERRO AO EXECUTAR QUERY: {e}", exc_info=True)

    def fetchone(self, query: str):
        try:
            with self.engine.connect() as conn:
                result = conn.exec_driver_sql(query).fetchone()
            return result
        except Exception as e:
            logging.error(f"❌ ERRO NO fetchone: {e}", exc_info=True)
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
