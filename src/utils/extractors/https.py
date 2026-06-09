import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import NullHandler
from pathlib import Path
from typing import Optional

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())


class HttpJsonExtractor:
    """Faz requisicao HTTP com requests e/ou Salva em .json"""

    def __init__(
        self,
        log: Optional[logging.Logger] = None,
        retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.logger = log or logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.default_headers = {"Accept": "application/json"}

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,  # espera 2s, 4s, 8s entre tentativas
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

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
        user_headers = kwargs.pop("headers", {})  # sobrescreve se o usuário passou algo
        headers = {**self.default_headers, **user_headers}
        try:
            response = self.session.request(
                method=method, url=url, headers=headers, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except ValueError:
            self.logger.error(f"RESPOSTA NAO E JSON: {url}")
        except requests.RequestException as e:
            self.logger.error(f"ERRO NA REQUISICAO: {url} --- {e}")
        return None

    def make_http_request_text(self, url: str) -> Response.text:
        """Faz Requisicao GET e retorna Response.text"""
        try:
            response = requests.get(url, timeout=10, headers=self.default_headers)
            response.raise_for_status()
            logger.info(f"{response} --- {url}")
            return response.text
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

    def fetch_and_save_many(
        self,
        tasks: list[tuple[str, str]],
        output_dir: Path | str,
        workers: int = 1,
    ) -> None:
        """Faz múltiplas requisições e persiste cada resultado em disco.

        Args:
            tasks: lista de (url, filename)
            output_dir: diretório de destino
            workers: número de threads (1 = sequencial, padrão)
        """

        def _one(url: str, filename: str) -> None:
            data = self.make_http_request(url)
            if data is not None:
                self.save_response(data, output_dir, filename)
            else:
                self.logger.warning(f"Sem dados de {url}")

        if workers == 1:
            for url, filename in tasks:
                _one(url, filename)
            return

        self.logger.info(
            f"Iniciando extração com {workers} threads ({len(tasks)} tarefas)..."
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, url, fn): fn for url, fn in tasks}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    self.logger.error(f"Erro em {futures[fut]}: {e}", exc_info=True)

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
