import os
import re
from pathlib import Path


def change_extension_from_all_files(input_dir, new_extension, current_extension=None):
    """
    Muda a extensão de todos os arquivos em um diretório.

    Args:
        input_dir (str | Path): Diretório onde estão os arquivos.
        new_extension (str): Nova extensão (ex.: ".csv").
        current_extension (str, opcional): Extensão atual para filtrar (ex.: ".json").
    """
    input_dir = Path(input_dir)

    for file in input_dir.iterdir():
        if file.is_file():
            # Se tiver filtro de extensão, aplica
            if current_extension and file.suffix != current_extension:
                continue

            # Novo nome com a extensão trocada
            new_name = file.with_suffix(new_extension)
            file.rename(new_name)
            print(f"Renomeado: {file.name} → {new_name.name}")


if __name__ == "__main__":
    dir = "/media/lucas/Files/2.Projetos/0.mylake/landing/demodados/camara/radar_congresso/radar_detalhes_deputados"
    new_ext = ".json"
    change_extension_from_all_files(dir, new_extension=new_ext)
