import pandas as pd

from src.utils.extractors.https import HttpJsonExtractor


def obter_tipo_entes():
    """Obter tabela de tipos de entes"""
    url_base = "https://legis.senado.leg.br/dadosabertos/processo/entes"
    destination = (
        "/home/lucas/workspace/demodados/demodadosdw/seeds/raw_senado_tipos_entes.csv"
    )

    extractor = HttpJsonExtractor()
    data = extractor.make_http_request(
        url=url_base,
    )
    df = pd.DataFrame(data)

    df.to_csv(destination, sep=",", index=False)  ### <<< necessario para dbt seed
    print("Finalizado")


def obter_tipos_decisao():
    """Obter tabela de tipos de decisao"""
    url_base = "https://legis.senado.leg.br/dadosabertos/processo/tipos-decisao"
    destination = (
        "/home/lucas/workspace/demodados/demodadosdw/seeds/raw_senado_tipos_decisao.csv"
    )
    extractor = HttpJsonExtractor()
    data = extractor.make_http_request(
        url=url_base,
    )
    df = pd.DataFrame(data)

    df.to_csv(destination, sep=",", index=False)  ### <<< necessario para dbt seed
    print("Finalizado")


if __name__ == "__main__":
    obter_tipos_decisao()


if __name__ == "__main__":
    obter_tipo_entes()
    obter_tipos_decisao()
