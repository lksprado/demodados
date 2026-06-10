import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from pandera.errors import SchemaError
from pandera.pandas import DataFrameModel

from ..utils.extractors.https import HttpJsonExtractor
from ..utils.loaders.postgres import PostgreSQLManager


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
    subpath: str = None
    error_dir: Path | str = None
    landing_file: str | None = None
    bronze_file: str | None = None
    parameter_dir: Path | str | None = None
    parameter_file: str | None = None
    output_param_dir: Path | str | None = None
    output_param_file: str | None = None
    criar_dirs: bool = True

    def __post_init__(self):
        # Normaliza diretórios para Path, mesmo se vierem como string dos dicts atuais
        # Faz normalizacoes/validacoes derivadas dos campos recebidos
        self.landing_dir = (
            Path(self.landing_dir) / self.subpath
            if self.subpath
            else Path(self.landing_dir)
        )
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        if self.bronze_dir:
            self.bronze_dir = (
                Path(self.bronze_dir) / self.subpath
                if self.subpath
                else Path(self.bronze_dir)
            )
            self.bronze_dir.mkdir(parents=True, exist_ok=True)
        if self.error_dir:
            self.error_dir = (
                Path(self.error_dir) / self.subpath
                if self.subpath
                else Path(self.error_dir)
            )
            self.error_dir.mkdir(parents=True, exist_ok=True)
        if self.parameter_dir:
            self.parameter_dir = Path(self.parameter_dir)
        if self.output_param_dir:
            self.output_param_dir = Path(self.output_param_dir)
            self.output_param_dir.mkdir(parents=True, exist_ok=True)

        # Resolve template {date} em landing_file e bronze_file
        today = datetime.today().strftime("%Y-%m-%d")
        if self.landing_file:
            self.landing_file = self.landing_file.format(date=today)
        if self.bronze_file:
            self.bronze_file = self.bronze_file.format(date=today)

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

    @property
    def parameter_filepath(self) -> Path:
        if not self.parameter_dir:
            raise ValueError("parameter_dir não configurado na PipelineConfig.")
        if not self.parameter_file:
            raise ValueError("parameter_file não configurado na PipelineConfig.")
        return Path(self.parameter_dir) / self.parameter_file

    @property
    def output_param_filepath(self) -> Path:
        if not self.output_param_dir:
            raise ValueError("output_param_dir não configurado na PipelineConfig.")
        if not self.output_param_file:
            raise ValueError("output_param_file não configurado na PipelineConfig.")
        return Path(self.output_param_dir) / self.output_param_file


def load_source_config(config_path: str, source: str, env: str) -> dict:
    """Monta dict compatível com PipelineConfig a partir de YAML com sections environments/sources.

    O YAML deve ter a estrutura:
        environments:
          local:
            base_raw: ...
            base_bronze: ...
            base_parameters: ...  # opcional
          airflow:
            ...
        sources:
          <nome>:
            base_url: ...
            subpath: ...      # opcional
            bronze_file: ...  # opcional
            landing_file: ... # opcional
            db_table: ...
            parameter_dir: ... # opcional
            parameter_file: ... # opcional nome do arquivo; combinado com base_parameters do env
    """

    import yaml

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    env_cfg = cfg["environments"][env]
    src_cfg = cfg["sources"][source]

    result: dict = {
        "landing_dir": env_cfg["base_raw"],
        "bronze_dir": env_cfg.get("base_bronze"),
        "parameter_dir": env_cfg.get("base_parameters"),
        "url_base": src_cfg.get("base_url"),
        "bronze_file": src_cfg.get("bronze_file"),
        "landing_file": src_cfg.get("landing_file"),
        "subpath": src_cfg.get("subpath"),
        "db_table": src_cfg.get("db_table"),
        "parameter_file": src_cfg.get("parameter_file"),
        "output_param_dir": env_cfg.get("base_parameters"),
        "output_param_file": src_cfg.get("output_param_file"),
    }

    return {k: v for k, v in result.items() if v is not None}


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
        df = pd.read_csv(self.cfg.bronze_filepath, sep=";", low_memory=False)
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
