from pathlib import Path

from bs4 import BeautifulSoup as bs
from requests import Response


def make_bs_object(input_file: Path = None, response: Response.text = None) -> bs:
    """Cria um objeto BeautifulSoup com 'html.parser'

    Args:
        input_file (Path, optional): Abre um arquivo. Defaults to None
        response (Response, optional): Recebe objeto Unicode de Request. Defaults to None.

    Returns:
        bs: Soup Object
    """
    if input_file is not None and response is None:
        with open(Path(input_file), "r", encoding="utf-8") as f:
            soup = bs(f.read(), "html.parser")
    elif input_file is None and response is not None:
        soup = bs(response, "html.parser")
    else:
        raise ValueError("Ambos parametros None")
    return soup
