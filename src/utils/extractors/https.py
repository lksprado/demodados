import json
import logging
import os
from pathlib import Path
from typing import Callable, Optional

import requests


class HttpJsonExtractor:
    def __init__(
        self,
        url: str,
        output_dir: Path,
        filename_fn: str,
        logger: Optional[logging.Logger] = None,
    ):
        self.url = url
        self.output_dir = Path(output_dir)
        self.filename_fn = filename_fn
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def _fetch(
        self,
        method="GET",
        headers=None,
        params=None,
        data=None,
        json_data=None,
        timeout=10,
    ) -> Optional[dict]:
        default_headers = {"Accept": "application/json"}
        if headers:
            default_headers.update(headers)

        try:
            response = requests.request(
                method=method,
                url=self.url,
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
                self.logger.error(f"RESPOSTA NAO E JSON: {self.url}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"ERRO REQUISICAO: {self.url} --- {e}")

    def _save(self, json_data: dict) -> Path:
        file_path = self.output_dir / self.filename_fn
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=4, ensure_ascii=False)
        self.logger.info(f"JSON SALVO EM: {file_path} ")
        return file_path

    def fetch_and_save(self, **kwargs) -> Path:
        data = self._fetch(**kwargs)
        if data is None:
            self.logger.error(f"SEM DADOS PARA SALVAR: {self.url}")
            raise
        return self._save(data)

    #####################################################################
    ## METODO AUXILIAR PARA MULTIPLAS REQUISICOES


def make_http_request(
    url: str,
    method: str = "GET",
    headers: dict = None,
    params: dict = None,
    data: dict = None,
    json_data: dict = None,
    log: Optional[logging.Logger] = None,
) -> dict | None:
    """
    Realiza uma requisição HTTP e retorna a resposta em formato JSON.

    Args:
        url (str): URL do endpoint da API.
        method (str, optional): Método HTTP (GET, POST, etc.). Default é "GET".
        headers (dict, optional): Cabeçalhos adicionais para a requisição.
        params (dict, optional): Parâmetros de query para a URL.
        data (dict, optional): Dados a serem enviados como payload (form-urlencoded).
        json_data (dict, optional): Dados a serem enviados como JSON.

    Returns:
        dict | None: Conteúdo da resposta em JSON se bem-sucedida, senão None.

    Obs:
        - Timeout fixado em 10 segundos.
        - Aceita apenas respostas JSON (application/json).
    """
    logger = log or logging.getLogger(__name__)

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
            timeout=10,
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError as e:
            logger.error(f"RESPOSTA NAO E JSON")

    except requests.exceptions.RequestException as e:
        logger.error(f"ERRO DE REQUISIÇÃO: {e}")
        return None


def response_to_json(
    response: dict,
    output_dir: Path,
    filename_fn: Callable[[str], str],
    log: Optional[logging.Logger] = None,
) -> None:
    """
    Salva uma resposta JSON em um arquivo local.

    Args:
        response (dict): Objeto JSON a ser salvo.
        path (str): Caminho do diretório onde o arquivo será salvo.
        filename (str): Nome do arquivo (ex: "saida.json").

    Returns:
        None. O arquivo é salvo em disco.

    Obs:
        - Cria o diretório se ele não existir.
        - Substitui o arquivo existente, se houver.
    """
    logger = log or logging.getLogger(__name__)
    output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / filename_fn

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(response, f, indent=4, ensure_ascii=False)

    logger.info(f"JSON SALVO EM: {file_path}")


if __name__ == "__main__":
    make_http_request("https://dadosabertos.camara.leg.br/api/v2/deputados/204379")
