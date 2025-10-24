import json
import logging
from pathlib import Path
from typing import Optional

import requests


class HttpJsonExtractor:
    """Faz requisicao HTTP com requests e/ou Salva em .json"""

    def __init__(self, log: Optional[logging.Logger] = None):
        if log is None:
            logging.basicConfig(
                format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO,
            )
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = log

    def make_http_request(
        self, url: str, method: str = "GET", **kwargs
    ) -> Optional[dict]:
        """Faz requisição HTTP e retorna JSON (ou None em caso de erro).

        Args:
            url (str): URL para requisicao
            method (str, optional): Metodo de requisicao. Defaults to "GET".
        Returns:
            Optional[dict]: Dicionario, JSON
        """
        default_headers = {"Accept": "application/json"}
        user_headers = kwargs.pop("headers", {})  # sobrescreve se o usuário passou algo
        headers = {**default_headers, **user_headers}
        try:
            response = requests.request(
                method=method, url=url, headers=headers, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except ValueError:
            self.logger.error(f"RESPOSTA NAO E JSON: {url}")
        except requests.RequestException as e:
            self.logger.error(f"ERRO NA REQUISICAO: {url} --- {e}")
        return None

    def save_response(self, json_data: dict, output_dir: Path | str, filename: str):
        """Salva um dicionario JSON (dict) em arquivo .json

        Args:
            json_data (dict): Objeto Json
            output_dir (Path): Diretorio de destino
            filename (Callable[[str], str]): Nome do arquivo
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if not filename.endswith(".json"):
            filename = filename + ".json"

        file_path = output_dir / filename
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=4, ensure_ascii=False)

        self.logger.info(f"JSON SALVO EM: {file_path}")

    def fetch_and_save(self, url: str, output_dir: Path | str, filename: str, **kwargs):
        """Faz requisicao e persiste em disco.

        Args:
            url (str): URL de requisicao
            output_dir (Path | str): Diretorio de destino
            filename (str): Nome arquivo
        """
        data = self.make_http_request(url, **kwargs)
        if data is not None:
            self.save_response(data, output_dir, filename)
        else:
            self.logger.warning(f"NENHUM DADO DE {url}")


if __name__ == "__main__":
    url = "https://apirest2.politicos.org.br/api/parliamentarianranking?Include=Parliamentarian.State&Include=Parliamentarian.Party&Include=Parliamentarian.Organ&Include=Parliamentarian&take=700&StatusId=1&OrderBy=scoreRanking&Year=2025"
    output_dir = "./local_setup/data/teste/"
    filename = "ranking_teste"
    extractor = HttpJsonExtractor()
    extractor.fetch_and_save(url=url, output_dir=output_dir, filename=filename)
