import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # take environment variables


class PostgreSQLManager:
    def __init__(
        self, db_name=None, db_user=None, db_password=None, db_host=None, db_port=None
    ):

        if (
            not self.check_environment_variables
            and db_name is None
            and db_user is None
            and db_password is None
            and db_host is None
            and db_port is None
        ):
            raise ValueError("As credenciais do Banco não foram fornecidas.")

        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_port = db_port or os.getenv("DB_PORT")

    def _connect(self):
        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            print("Conexão bem-sucedida ao banco de dados PostgreSQL.")
            return connection
        except psycopg2.Error as e:
            print(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
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
                print("Não foi possível estabelecer a conexão com o banco de dados.")
                return None
        except psycopg2.Error as e:
            print(f"Erro ao executar a consulta SQL: {e}")
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
                print("Inserção bem-sucedida.")
            else:
                print("Não foi possível estabelecer a conexão com o banco de dados.")
        except psycopg2.Error as e:
            print(f"Erro ao executar a inserção SQL: {e}")

    @staticmethod
    def check_environment_variables():
        if (
            not os.getenv("DB_NAME")
            or not os.getenv("DB_USER")
            or not os.getenv("DB_PASSWORD")
            or not os.getenv("DB_HOST")
        ):
            print("As variáveis de ambiente do banco não estão configuradas.")
            return False
        else:
            print("Variáveis de ambiente para o Banco foram configuradas corretamente.")
            return True

    def alchemy(self):
        self.engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        return self.engine

    @staticmethod
    def send_to_db(df, table_name, how="append", filename=None):
        """
        Envia um DataFrame para uma tabela no schema 'bronze' do PostgreSQL.

        Args:
            df (pd.DataFrame): DataFrame a ser inserido no banco de dados.
            table_name (str): Nome da tabela de destino.
            how (str, optional): Modo de inserção no banco. Pode ser 'replace', 'append' ou 'fail'. Default é 'append'.
            filename (str, optional): Nome do arquivo de origem para registrar no campo 'arquivo_origem'.

        Comportamento:
            - Adiciona as colunas 'data_carga' e, se fornecido, 'arquivo_origem'.
            - Utiliza SQLAlchemy via PostgreSQLManager.
            - Insere os dados na tabela especificada no schema 'bronze'.

        Returns:
            None. Os dados são persistidos no banco de dados.

        Raises:
            Exibe erro no console se falhar na operação de inserção.
        """
        try:
            pg = PostgreSQLManager()
            connection = pg.alchemy()

            if filename:
                df["arquivo_origem"] = os.path.basename(filename)

            df["data_carga"] = datetime.now()

            df.to_sql(table_name, connection, schema="raw", if_exists=how, index=False)
            print(f"✅ {filename} Dados inseridos em {table_name}")

        except Exception as e:
            print(f"❌ Erro ao inserir no banco: {e}")


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
