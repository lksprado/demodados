import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
import json
import re

import pandas as pd

from src.utils.transformers.cleaning import ColumnSanitizer


def make_df_from_json_list(filepath: str, list_key: str) -> pd.DataFrame:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if list_key not in data:
        print(f"Chave '{list_key}' não encontrada no JSON.")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(data[list_key])
        return df
    except (ValueError, TypeError) as e:
        print(f"Erro ao criar DataFrame: {e}")
        return pd.DataFrame()


def normalize_json_object(filepath: str, key: str = None) -> pd.DataFrame:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if key:
        data = data.get(key, {})

    # if not isinstance(data, dict):
    #     print(f"❌ Estrutura inesperada: esperada dict, veio {type(data)}")
    #     return pd.DataFrame()

    try:
        df = pd.json_normalize(data, sep=".")
        return df
    except Exception as e:
        print(f"Erro ao normalizar JSON: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    filepath = "local_setup/data/teste/ranking_teste.json"
    df_new = make_df_from_json_list(filepath=filepath, list_key="data")
    df_new = ColumnSanitizer(df_new).sanitize_columns_names().df

    df_new["parliamentarianregister"] = (
        df_new["parliamentarian"]
        .apply(
            lambda d: (
                (
                    d.get("register")
                    or (
                        re.findall(
                            r"\d+",
                            (
                                d.get("otherInformations")
                                or d.get("otherinformations")
                                or ""
                            ),
                        )
                        or [None]
                    )[-1]
                )
                if isinstance(d, dict)
                else None
            )
        )
        .astype("Int64")
    )
    df_new["position"] = df_new["parliamentarian"].apply(lambda d: d.get("position"))

    # 3) Seleção final (só o que existir)
    cols_to_keep = [
        "id",
        "parliamentarianid",
        "year",
        "scorepresence",
        "scoresavequota",
        "scoresavequotapercentage",
        "scoreprocess",
        "scoreinternal",
        "scoreprivileges",
        "scorewastage",
        "scoretotal",
        "scoreranking",
        "scorerankingbyposition",
        "scorerankingbyparty",
        "scorerankingbystate",
        "scorerankingbypositionbystate",
        "parliamentarianstatecount",
        "parliamentarianpositionstatecount",
        "active",
        "link",
        "parliamentarianregister",
        "position",
    ]
    df_new = df_new[[c for c in cols_to_keep if c in df_new.columns]]

    df_new.to_csv("local_setup/data/teste/ranking_teste.csv", sep=";", index=False)
    pass
