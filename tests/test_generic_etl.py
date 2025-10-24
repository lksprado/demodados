from pathlib import Path

import pandas as pd
import pytest

from src.utils.pipeline_cfg import GenericETL, PipelineConfig


def test_generic_extraction_calls_http_extractor(monkeypatch, tmp_path: Path):
    called = {}

    class FakeExtractor:
        def __init__(self, logger):
            called["logger_name"] = getattr(logger, "name", None)

        def fetch_and_save(self, url, output_dir, filename):
            called["url"] = url
            called["output_dir"] = Path(output_dir)
            called["filename"] = filename

    # Replace HttpJsonExtractor in GenericETL module scope
    import src.utils.pipeline_cfg as pipeline_cfg_module

    monkeypatch.setattr(pipeline_cfg_module, "HttpJsonExtractor", FakeExtractor)

    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        url_base="https://api.example/test",
        landing_file="data.json",
        bronze_file="data.csv",
    )

    etl = GenericETL(cfg=cfg)
    etl.generic_extraction()

    assert called["url"] == "https://api.example/test"
    assert called["output_dir"] == cfg.landing_dir
    assert called["filename"] == cfg.landing_file


def test_extract_uses_custom_function(tmp_path: Path):
    markers = {"ran": False}

    def custom_extract(c):
        markers["ran"] = True
        assert isinstance(c, PipelineConfig)
        return "ok"

    cfg = PipelineConfig(landing_dir=tmp_path / "ld", bronze_dir=tmp_path / "brz")
    etl = GenericETL(cfg=cfg, extract_fn=custom_extract)
    result = etl.extract()

    assert markers["ran"] is True
    assert result == "ok"


def test_transform_without_function_raises(tmp_path: Path):
    cfg = PipelineConfig(landing_dir=tmp_path / "ld")
    etl = GenericETL(cfg=cfg, transform_fn=None)
    with pytest.raises(NotImplementedError):
        etl.transform()


def test_transform_with_function_returns_df(tmp_path: Path):
    cfg = PipelineConfig(landing_dir=tmp_path / "ld")

    def tx(df, _cfg):
        return pd.DataFrame({"a": [1, 2]})

    etl = GenericETL(cfg=cfg, transform_fn=tx)
    out = etl.transform()
    assert isinstance(out, pd.DataFrame)
    assert list(out.columns) == ["a"]


def test_generic_validator_calls_validate():
    class FakeValidator:
        def __init__(self):
            self.called = False

        def validate(self, df):
            self.called = True
            return "validated"

    val = FakeValidator()
    etl = GenericETL(cfg={}, validator=val)
    res = etl.validate(pd.DataFrame({"x": [1]}))
    assert res == "validated"
    assert val.called is True


def test_generic_loader_uses_default_postgres_manager(monkeypatch, tmp_path: Path):
    sent = {}

    class FakePg:
        def send_df_to_db(self, df, table_name, filename, how):
            sent["table_name"] = table_name
            sent["filename"] = filename
            sent["how"] = how
            sent["rows"] = len(df)

    import src.utils.pipeline_cfg as pipeline_cfg_module

    # Patch the class used inside generic_loader
    monkeypatch.setattr(pipeline_cfg_module, "PostgreSQLManager", lambda: FakePg())

    cfg = PipelineConfig(
        landing_dir=tmp_path / "ld",
        bronze_dir=tmp_path / "brz",
        bronze_file="file.csv",
        db_table="my_table",
    )
    etl = GenericETL(cfg=cfg)
    df = pd.DataFrame({"x": [1, 2, 3]})
    etl.generic_loader(df)

    assert sent == {
        "table_name": "my_table",
        "filename": "file.csv",
        "how": "replace",
        "rows": 3,
    }


def test_load_uses_custom_load_fn(tmp_path: Path):
    called = {"args": None}

    def custom_load(df, cfg):
        called["args"] = (len(df), cfg.db_table)
        return "loaded"

    cfg = PipelineConfig(
        landing_dir=tmp_path / "ld",
        bronze_dir=tmp_path / "brz",
        db_table="t",
    )
    etl = GenericETL(cfg=cfg, load_fn=custom_load)
    out = etl.load(pd.DataFrame({"x": [1, 2]}))

    assert out == "loaded"
    assert called["args"] == (2, "t")
