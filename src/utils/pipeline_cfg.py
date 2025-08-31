import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Type

from src.utils.extractors.https import HttpJsonExtractor
from src.utils.loaders.postgres import PostgreSQLManager


@dataclass
class PipelineConfig:
    """Contrato para configuração do pipeline. \n
    Forneça um dicionário contendo: \n
    Args:
        landing_dir: <diretorio arquivos bruto>
        bronze_dir: <diretorio pos transformacao>
        error_dir: <diretorio fallback se houver>
        parameter_file: <arquivo para parametrizar>
        db_table: <nome tabela banco de dados >
    """

    landing_dir: Path | str
    bronze_dir: Path | str | None = None
    db_table: str | None = None
    url_base: str | None = None
    error_dir: Path | str = None
    landing_file: str | None = None
    bronze_file: str | None = None
    parameter_file: str | None = None

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

    # Conveniência para garantir diretórios antes de usar
    def ensure_dirs(self) -> None:
        """Garante diretórios"""
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


class GenericETL(ABC):
    """Template para ET(v)L
    Args:
        cfg_dict: Dicionário de configuração com PipelineConfig
    """

    def __init__(self, cfg_dict: dict, log: Optional[logging.Logger] = None):

        if log is None:
            logging.basicConfig(
                format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO,
            )
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = log

        self.cfg = PipelineConfig(**cfg_dict)
        ## CONVENIENCIA DAS VARIAVEIS
        self.landing_dir = self.cfg.landing_dir
        self.bronze_dir = self.cfg.bronze_dir
        self.db_table = self.cfg.db_table
        self.url_base = self.cfg.url_base
        self.error_dir = self.cfg.error_dir
        self.landing_file = self.cfg.landing_file
        self.bronze_file = self.cfg.bronze_file
        self.parameter_file = self.cfg.parameter_file
        self.extractor = HttpJsonExtractor(log=self.logger)
        self.loader = PostgreSQLManager()

        # GARANTE DIRETÓRIOS
        self.cfg.ensure_dirs()

    def generic_extraction(self):
        """Extracao+salvo mais basica de 1 URL para 1 arquivo"""
        self.extractor.fetch_and_save(
            url=self.url_base, output_dir=self.landing_dir, filename=self.landing_file
        )

    @abstractmethod
    def extract(self):
        raise NotImplementedError("Execute method must be overridden in subclasses")

    @abstractmethod
    def transform(self):
        raise NotImplementedError("Execute method must be overridden in subclasses")

    @abstractmethod
    def validate(self, df):
        raise NotImplementedError("Execute method must be overridden in subclasses")

    @abstractmethod
    def load(self):
        raise NotImplementedError("Execute method must be overridden in subclasses")

    def run_pipeline(
        self, E: bool = False, T: bool = False, V: bool = False, L: bool = False
    ):
        df = None
        if E:
            try:
                self.logger.info("Iniciando Extracao")
                self.extract()
            except Exception as e:
                self.logger.error(f"Problema na Extracao --- {e}")
        if T:
            try:
                self.logger.info("Iniciando Transformacao")
                df = self.transform()
            except Exception as e:
                self.logger.error(f"Problema na Transformacao --- {e}")
        if V:
            try:
                self.logger.info("Iniciando Validacao")
                df = self.validate(df)
            except Exception as e:
                self.logger.error(f"Problema na Validacao --- {e}")
        if L:
            try:
                self.logger.info("Iniciando Carga")
                self.load(df)
            except Exception as e:
                self.logger.error(f"Problema na Carga --- {e}")
