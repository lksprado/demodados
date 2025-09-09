import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from pandera import DataFrameModel
from pandera.errors import SchemaError

from src.utils.extractors.https import HttpJsonExtractor
from src.utils.loaders.postgres import PostgreSQLManager


@dataclass
class PipelineConfig:
    """Contrato para configuração do pipeline.
    Forneça um dicionário contendo:
    Args:
        landing_dir: diretorio arquivos bruto
        bronze_dir: diretorio pos transformacao
        error_dir: diretorio fallback se houver
        parameter_file: arquivo para parametrizar
        db_table: nome tabela banco de dados
    """

    landing_dir: Path | str
    bronze_dir: Path | str | None = None
    db_table: str | None = None
    url_base: str | None = None
    error_dir: Path | str = None
    landing_file: str | None = None
    bronze_file: str | None = None
    parameter_file: str | None = None
    criar_dirs: bool = True

    def __post_init__(self):
        # Normaliza diretórios para Path, mesmo se vierem como string dos dicts atuais
        # Faz normalizacoes/validacoes derivadas dos campos recebidos
        self.landing_dir = Path(self.landing_dir)
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        if self.bronze_dir:
            self.bronze_dir = Path(self.bronze_dir)
            self.bronze_dir.mkdir(parents=True, exist_ok=True)
        if self.error_dir:
            self.error_dir = Path(self.error_dir)
            self.error_dir.mkdir(parents=True, exist_ok=True)
        # Deriva bronze_file se não vier no config
        if self.bronze_file is None and self.landing_file:
            self.bronze_file = Path(self.landing_file).with_suffix(".csv").name

        if self.criar_dirs:
            self.ensure_dirs()

    # Conveniência para garantir diretórios antes de usar
    def ensure_dirs(self) -> None:
        """Garante diretórios Landing e Bronze"""
        if not self.landing_dir.exists():
            self.landing_dir.mkdir(parents=True, exist_ok=True)

        if self.bronze_dir is not None:
            if not self.bronze_dir.exists():
                self.bronze_dir.mkdir(parents=True, exist_ok=True)

    # @property
    # É um decorator do Python que transforma uma função (método) em um atributo “calculado”.
    # Você acessa como se fosse um campo (obj.algo), mas por trás ele roda uma função.
    # Vantagens
    # usar obj.bronze_filepath em vez de obj.get_bronze_filepath()
    # valores que dependem de outros (ex.: dir + file ➜ path).
    @property
    def landing_filepath(self) -> Path:
        if not self.landing_dir:
            raise ValueError("landing_dir não configurado na PipelineConfig.")
        if not self.landing_file:
            raise ValueError("landing_file não configurado na PipelineConfig.")
        return self.landing_dir / self.landing_file

    @property
    def bronze_filepath(self) -> Path:
        if not self.bronze_dir:
            raise ValueError("bronze_dir não configurado na PipelineConfig.")
        if not self.bronze_file:
            raise ValueError("bronze_file não configurado na PipelineConfig.")
        return Path(self.bronze_dir) / self.bronze_file


class GenericETL:
    """Template para ET(v)L
    Args:
        cfg_dict: Dicionário de configuração com PipelineConfig
    """

    def __init__(
        self,
        cfg: dict,
        extract_fn: Callable = None,
        transform_fn: Callable = None,
        validate_fn: Callable = None,
        load_fn: Callable = None,
        validator: DataFrameModel = None,
        log: Optional[logging.Logger] = None,
    ):
        self.cfg = cfg
        self.extract_fn = extract_fn
        self.transform_fn = transform_fn
        self.validate_fn = validate_fn
        self.load_fn = load_fn
        self.validator = validator
        self.loader = PostgreSQLManager()

        if log is None:
            logging.basicConfig(
                format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO,
            )
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = log

    def generic_extraction(self):
        """Extracao mais basica de 1 URL para 1 arquivo"""
        self.logger.info("Iniciando Extracao...")
        extractor = HttpJsonExtractor(self.logger)
        extractor.fetch_and_save(
            url=self.cfg.url_base,
            output_dir=self.cfg.landing_dir,
            filename=self.cfg.landing_file,
        )

    def extract(self):
        if self.extract_fn:
            return self.extract_fn(self.cfg)
        else:
            return self.generic_extraction()

    def transform(self, df=None):
        self.logger.info("Iniciando Transformacao...")
        if not self.transform_fn:
            raise NotImplementedError("Nenhum transformer definido")
        result = self.transform_fn(df, self.cfg)
        if result is None:
            raise ValueError("Transform function must return a DataFrame, got None")
        return result

    def generic_validator(self, df):
        self.logger.info("Iniciando Validacao...")
        try:
            self.logger.info("Validacao OK")
            return self.validator.validate(df)
        except SchemaError as e:
            self.logger.error(f"ERRO DE SCHEMA: {e}", exc_info=True)
            raise

    def validate(self, df):
        if self.validate_fn:
            self.logger.info("Validacao OK")
            return self.validate_fn(df)
        elif self.validator:
            return self.generic_validator(df)
        else:
            raise NotImplementedError("Nenhum validator definido")

    def generic_loader(self, df):
        self.logger.info("Iniciando Carga...")
        try:
            self.loader.truncate_table(table_name=self.cfg.db_table, log=self.logger)
        finally:
            self.loader.send_df_to_db(
                df=df,
                table_name=self.cfg.db_table,
                filename=self.cfg.bronze_file,
                how="append",
                log=self.logger,
            )

    def load(self, df):
        if self.load_fn:
            return self.load_fn(df, self.cfg)
        else:
            return self.generic_loader(df)
