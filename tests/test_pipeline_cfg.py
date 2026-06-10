import json
from pathlib import Path

import pytest

from src.utils.pipeline_cfg import PipelineConfig


def test_pipeline_cfg_normalizes_paths_and_derives_bronze_file(tmp_path: Path):
    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"

    cfg = PipelineConfig(
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
        landing_file="data.json",
        bronze_file=None,
        criar_dirs=True,
    )

    # Dirs created and normalized to Path
    assert cfg.landing_dir == landing_dir
    assert cfg.bronze_dir == bronze_dir
    assert landing_dir.exists()
    assert bronze_dir.exists()

    # bronze_file is derived from landing_file with .csv
    assert cfg.bronze_file == "data.csv"

    # Property paths combine dir + file
    assert cfg.landing_filepath == landing_dir / "data.json"
    assert cfg.bronze_filepath == bronze_dir / "data.csv"


def test_pipeline_cfg_resolves_date_template(tmp_path: Path):
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        landing_file="data_{date}.json",
        bronze_file="data_{date}.csv",
        criar_dirs=True,
    )

    assert cfg.landing_file == f"data_{today}.json"
    assert cfg.bronze_file == f"data_{today}.csv"
    assert cfg.landing_filepath == tmp_path / "landing" / f"data_{today}.json"


def test_pipeline_cfg_derives_bronze_with_date(tmp_path: Path):
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        landing_file="data_{date}.json",
        criar_dirs=False,
    )

    assert cfg.bronze_file == f"data_{today}.csv"


def test_pipeline_cfg_missing_fields_raise():
    cfg = PipelineConfig(landing_dir="/tmp/ld", criar_dirs=False)

    with pytest.raises(ValueError):
        _ = cfg.landing_filepath

    cfg.landing_file = "x.json"
    # Now landing_filepath works
    assert isinstance(cfg.landing_filepath, Path)

    # bronze not configured
    with pytest.raises(ValueError):
        _ = cfg.bronze_filepath

    cfg.bronze_dir = Path("/tmp/brz")
    cfg.bronze_file = "x.csv"
    assert isinstance(cfg.bronze_filepath, Path)
