from pathlib import Path

import pandas as pd

from src.utils.transformers.json_parsers import make_df_from_json_list


def test_make_df_from_json_list_success(tmp_path: Path):
    data = {"data": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]}
    fp = tmp_path / "sample.json"
    fp.write_text(__import__("json").dumps(data), encoding="utf-8")

    df = make_df_from_json_list(str(fp), list_key="data")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2


def test_make_df_from_json_list_missing_key(tmp_path: Path):
    data = {"other": []}
    fp = tmp_path / "sample.json"
    fp.write_text(__import__("json").dumps(data), encoding="utf-8")

    df = make_df_from_json_list(str(fp), list_key="data")
    # Returns empty DataFrame when key missing
    assert df.empty
