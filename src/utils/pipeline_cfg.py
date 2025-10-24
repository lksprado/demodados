import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from pandera.errors import SchemaError
from pandera.pandas import DataFrameModel

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
    def __init__(
        self,
        cfg: PipelineConfig,
        extract_fn: Callable[[PipelineConfig], Path] | None = None,
        load_fn: Callable[[PipelineConfig], None] | None = None,
        validator: DataFrameModel | None = None,
        log: Optional[logging.Logger] = None,
    ):
        self.cfg = cfg
        self.extract_fn = extract_fn
        self.load_fn = load_fn
        self.validator = validator
        self.logger = log or logging.getLogger(self.__class__.__name__)

    # --- EXTRACT ---
    def generic_extraction(self) -> Path:
        self.logger.info("Iniciando Extração...")
        extractor = HttpJsonExtractor(self.logger)
        extractor.fetch_and_save(
            url=self.cfg.url_base,
            output_dir=self.cfg.landing_dir,
            filename=self.cfg.landing_file,
        )
        return self.cfg.landing_filepath

    def extract(self) -> Path:
        if self.extract_fn is not None:
            return self.extract_fn(self.cfg)  # custom usa cfg
        return self.generic_extraction()  # genérico sem args

    # --- VALIDATE ---
    def validate(self) -> Path:
        if not self.validator:
            self.logger.info("Sem validador; pulando validação.")
            return self.cfg.bronze_filepath

        self.logger.info(f"Validando {self.cfg.bronze_filepath}...")
        df = pd.read_csv(
            self.cfg.bronze_filepath, sep=";"
        )  # opcional: sep/encoding do cfg
        try:
            self.validator.validate(df)
            self.logger.info("Validação OK")
            return self.cfg.bronze_filepath
        except SchemaError as e:
            self.logger.error(f"ERRO DE SCHEMA: {e}", exc_info=True)
            raise

    # --- LOAD ---
    def generic_loader(self) -> None:
        self.logger.info(f"Carga de {self.cfg.bronze_filepath} -> {self.cfg.db_table}")
        df = pd.read_csv(
            self.cfg.bronze_filepath, sep=";"
        )  # opcional: sep/encoding do cfg
        PostgreSQLManager().send_df_to_db(
            df=df,
            table_name=self.cfg.db_table,
            filename=self.cfg.bronze_filepath.name,
            how="replace",
        )
        self.logger.info("Carga concluída")

    def load(self) -> None:
        if self.load_fn is not None:
            # loader custom totalmente dirigido por cfg
            self.load_fn(self.cfg)
        else:
            # fallback usa o método que já conhece self.cfg
            self.generic_loader()
