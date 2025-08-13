import os
from pathlib import Path

import pandas as pd
import pandera as pa


def infer_the_schema(df, folder, name):
    schema = pa.infer_schema(df)
    with open(f"{folder}{name}.py", "w", encoding="utf-8") as f:
        f.write(schema.to_script())
    print(f"Schema salvo em: {folder}{name}")


def get_all_columns_from_csv_folder(
    csv_folder_path: Path,
    output_folder_for_schema: Path,
    output_filename: str,
    csv_sep=";",
):
    """
    Lê todos os arquivos CSV de uma pasta, extrai e unifica os nomes das colunas de cada arquivo,
    e salva o conjunto de colunas únicas em um arquivo de texto, uma coluna por linha.

    Parâmetros:
        csv_folder_path (Path): Caminho para a pasta onde estão os arquivos CSV.
        output_folder_for_schema (Path): Caminho para a pasta onde o arquivo de saída será salvo.
        output_filename (str): Nome do arquivo de saída que irá conter as colunas.
        csv_sep (str, opcional): Separador dos arquivos CSV. Padrão é ';'.

    Observações:
        - Apenas arquivos com extensão '.csv' serão considerados.
        - O arquivo de saída será sobrescrito se já existir.
        - O arquivo final conterá uma coluna por linha.
    """
    os.makedirs(output_folder_for_schema, exist_ok=True)
    output_file_destination = os.path.join(output_folder_for_schema, output_filename)
    cols_set = set()
    for file in os.listdir(csv_folder_path):
        csv_file_path = os.path.join(csv_folder_path, file)
        if os.path.isfile(csv_file_path) and file.endswith(".csv"):
            df_cols_list = pd.read_csv(csv_file_path, sep=csv_sep, nrows=0).columns
            cols_set.update(df_cols_list)

    col_list_unique = list(cols_set)

    with open(output_file_destination, "w") as f:
        for col in col_list_unique:
            f.write(f"{col}\n")
    print(f"Arquivo txt com coluna salva em: {output_file_destination}")


if __name__ == "__main__":
    # folder = "data/bronze/camara/deputados_detalhes/"
    # name = "deputados_detalhes_schema"
    # df = pd.read_csv(
    #     "data/bronze/camara/deputados_detalhes/62881_deputado.csv", sep=";"
    # )
    # infer_the_schema(df, folder, name)
    folder = "data/bronze/camara/radar_congresso"
    schema = "data/bronze/camara/radar_congresso/schema_cols"
    final_filename = "todas_colunas_id_voz_deputado_detalhes.txt"
    get_all_columns_from_csv_folder(folder, schema, final_filename)
