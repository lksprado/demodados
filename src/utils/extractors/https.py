import json
import os

import requests


def make_http_request(
    url: str,
    method: str = "GET",
    headers: dict = None,
    params: dict = None,
    data: dict = None,
    json_data: dict = None,
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
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO DE REQUISIÇÃO: {e}")
        return None


def response_to_json(response: dict, path: str, filename: str) -> None:
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
    if not os.path.exists(path):
        os.makedirs(path)

    file_path = os.path.join(path, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(response, f, indent=4, ensure_ascii=False)

    print(f"✅ SUCESSO! JSON salvo em: {file_path}")


if __name__ == "__main__":
    pass
