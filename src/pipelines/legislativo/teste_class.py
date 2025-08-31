import pandas as pd
from pandera import DataFrameModel
from pandera.errors import SchemaError

from src.pipelines.legislativo.schema import GovernismoSchema, ParlamentarRadarSchema
from src.utils.pipeline_cfg import GenericETL, PipelineConfig
from src.utils.transformers.cleaning import ColumnSanitizer


class RadarCongressoParlamentares(GenericETL):

    def __init__(self, cfg_dict, schema: DataFrameModel, log=None):
        self.schema = schema
        super().__init__(cfg_dict, log)

    def extract(self):
        super().generic_extraction()

    def transform(self):
        try:
            df = pd.read_json(self.cfg.landing_filepath, dtype=str)
            df = ColumnSanitizer(df).sanitize_columns_names().df
            df.to_csv(self.cfg.bronze_filepath, sep=";", index=False)
            self.logger.info(f"CSV SALVO EM: {self.cfg.bronze_filepath}")
            return df
        except:
            raise (
                self.logger.error(
                    f"ERRO AO TRANSFORMAR ARQUIVO --- {self.cfg.landing_filepath}"
                )
            )

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            validated_df = self.schema.validate(df)
            return validated_df
        except SchemaError as e:
            self.logger.error(f"ERRO DE SCHEMA --- {e}")
            raise

    def load(self, df: pd.DataFrame):
        try:
            self.loader.send_to_db(
                df=df, table_name=self.db_table, filename=self.bronze_file
            )
        except Exception as e:
            self.logger.warning(
                f"ERROR NA CARGA PARA TABELA --- {self.db_table} --- {e}"
            )


if __name__ == "__main__":
    cfg_dicionario = {
        "url_base": "https://radar.congressoemfoco.com.br/api/busca-parlamentar",
        "landing_dir": "./data/teste/radar_congresso/parlamentares/",
        "bronze_dir": "./data/teste_bronze/radar_congresso/parlamentares/",
        "landing_file": "teste.json",
        "bronze_file": "teste.csv",
        "db_table": "radar_teste_raw",
    }

    etl = RadarCongressoParlamentares(cfg_dicionario, schema=ParlamentarRadarSchema)
    etl.run_pipeline(False, True, True, True)
