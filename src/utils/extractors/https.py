import json
import os
import requests
from src.utils.logger import logger

logger = logger('')

def make_http_request(
    url, method="GET", headers=None, params=None, data=None, json_data=None
):

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
        print(f"ERROR! --- Request error: {e}")
        return None


def response_to_json(response: dict, path: str, filename: str):
    if not os.path.exists(path):
        os.makedirs(path)

    file_path = os.path.join(path, filename)

    with open(file_path, "w") as f:
        json.dump(response, f, indent=4, ensure_ascii=False)
        return print(f"SUCCESS! --- Json saved: {file_path}")


if __name__ == "__main__":
    pass
