import json
import os
import re
import pandas as pd


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

    if not isinstance(data, dict):
        print(f"❌ Estrutura inesperada: esperada dict, veio {type(data)}")
        return pd.DataFrame()

    try:
        df = pd.json_normalize(data, sep=".")
        return df
    except Exception as e:
        print(f"Erro ao normalizar JSON: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    pass