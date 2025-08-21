import json
import logging
from pathlib import Path
from typing import Callable, Optional

import requests


class HttpJsonExtractor:
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
        self,
        url: Callable[[str], str],
        method="GET",
        headers=None,
        params=None,
        data=None,
        json_data=None,
        timeout=10,
    ) -> Optional[dict]:
        """Faz requisicao e retorna response Json"""
        default_headers = {"Accept": "application/json"}
        if headers:
            default_headers.update(headers)

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=default_headers,
                params=params,
                data=data,
                json=json_data,
                timeout=timeout,
            )
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                self.logger.error(f"RESPOSTA NAO E JSON: {url}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"ERRO REQUISICAO: {url} --- {e}")

    def _save_response(
        self, json_data: dict, output_dir: Path, filename: Callable[[str], str]
    ):
        output_dir = Path(output_dir)

        if not output_dir.exists():
            output_dir.mkdir(parents=True)

        if not filename.endswith(".json"):
            filename = filename + ".json"

        if not json_data is None:
            file_path = output_dir / filename

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4, ensure_ascii=False)

            self.logger.info(f"JSON SALVO EM: {file_path}")
        else:
            self.logger.warning(f"JSON VAZIO")

    def fetch_and_save(
        self,
        url,
        output_dir,
        filename: Callable[[str], str],
        method="GET",
        headers=None,
        params=None,
        data=None,
        json_data=None,
        timeout=10,
    ):
        data = self.make_http_request(
            url=url,
            method=method,
            headers=headers,
            params=params,
            data=data,
            json_data=json_data,
            timeout=timeout,
        )

        self._save_response(json_data=data, output_dir=output_dir, filename=filename)


if __name__ == "__main__":
    url = "https://dadosabertos.camara.leg.br/api/v2/deputados/204379"
    output_dir = "./local_setup/data/teste/"
    filename = "teste"
    extractor = HttpJsonExtractor()
    extractor.fetch_and_save(url=url, output_dir=output_dir, filename=filename)
