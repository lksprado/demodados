from pathlib import Path

import pandas as pd

from src.pipelines.legislativo.ranking_politicos import transform_parlamentares
from src.utils.pipeline_cfg import PipelineConfig


def _sample_input() -> dict:
    # Minimal structure to exercise the transformation
    return {
        "data": [
            {
                "id": 10,
                "parliamentarianid": "P-1",
                "year": 2025,
                "scoretotal": 99.5,
                "link": "https://example.test/Parlamentar?id=123",
                "parliamentarian": {
                    "register": None,
                    "otherInformations": "Reg: 123",
                    "position": "DEPUTADO",
                },
            },
            {
                "id": 11,
                "parliamentarianid": "P-2",
                "year": 2025,
                "scoretotal": 88.0,
                "link": "https://example.test/Parlamentar?id=456",
                "parliamentarian": {
                    "register": 456,
                    "otherInformations": None,
                    "position": "SENADOR",
                },
            },
        ]
    }


def test_transform_parlamentares_creates_bronze_csv_and_columns(tmp_path: Path):
    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"
    landing_dir.mkdir()
    bronze_dir.mkdir()

    landing_file = landing_dir / "ranking_parlamentares.json"
    bronze_file = "ranking_parlamentares.csv"

    # write input json
    landing_file.write_text(__import__("json").dumps(_sample_input()), encoding="utf-8")

    cfg = PipelineConfig(
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
        landing_file=landing_file.name,
        bronze_file=bronze_file,
        criar_dirs=False,
    )

    df = transform_parlamentares(None, cfg)

    # CSV written
    out_path = bronze_dir / bronze_file
    assert out_path.exists()

    # Validate content basics
    assert isinstance(df, pd.DataFrame)
    # Required columns produced by transform
    for col in [
        "id",
        "parliamentarianid",
        "year",
        "scoretotal",
        "parliamentarianregister",
        "position",
        "link",
    ]:
        assert col in df.columns

    # Check parliamentarianregister extraction logic
    # First row uses number from otherInformations (123), second from register (456)
    regs = df["parliamentarianregister"].astype("Int64").tolist()
    assert 123 in regs and 456 in regs

    # 'link' must remain not sanitized (original case preserved)
    assert df.loc[df.index[0], "link"].startswith("https://example.test/")
