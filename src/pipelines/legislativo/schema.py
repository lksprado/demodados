# schema_pandera.py
from typing import Optional

import pandera.pandas as pa  # Recomendação atual do Pandera
from pandera import DataFrameModel, Field
from pandera.typing import Series


class DeputadoSchema(DataFrameModel):
    """VALIDACAO DE API DA CAMARA DETALHES DOS DEPUTADOS"""

    id: Series[int] = Field(nullable=False)
    uri: Series[str] = Field(nullable=False)
    nomecivil: Series[str] = Field(nullable=False)
    cpf: Series[int] = Field(nullable=False)
    sexo: Series[str] = Field(nullable=True)
    urlwebsite: Series[str] = Field(nullable=True)
    redesocial: Series[str] = Field(nullable=True)
    datanascimento: Series[str] = Field(nullable=True)
    datafalecimento: Series[str] = Field(nullable=True)
    ufnascimento: Series[str] = Field(nullable=True)
    municipionascimento: Series[str] = Field(nullable=True)
    escolaridade: Series[str] = Field(nullable=True)
    ultimostatus_id: Series[int] = Field(nullable=False)
    ultimostatus_uri: Series[str] = Field(nullable=False)
    ultimostatus_nome: Series[str] = Field(nullable=False)
    ultimostatus_siglapartido: Series[str] = Field(nullable=True)
    ultimostatus_uripartido: Series[str] = Field(nullable=True)
    ultimostatus_siglauf: Series[str] = Field(nullable=True)
    ultimostatus_idlegislatura: Series[int] = Field(nullable=False)
    ultimostatus_urlfoto: Series[str] = Field(nullable=False)
    ultimostatus_email: Series[str] = Field(nullable=True)
    ultimostatus_data: Series[str] = Field(nullable=True)
    ultimostatus_nomeeleitoral: Series[str]
    ultimostatus_gabinete_nome: Series[str] = Field(nullable=True)
    ultimostatus_gabinete_predio: Series[str] = Field(nullable=True)
    ultimostatus_gabinete_sala: Series[str] = Field(nullable=True)
    ultimostatus_gabinete_andar: Series[str] = Field(nullable=True)
    ultimostatus_gabinete_telefone: Series[str] = Field(nullable=True)
    ultimostatus_gabinete_email: Series[str]
    ultimostatus_situacao: Series[str]
    ultimostatus_condicaoeleitoral: Series[str]
    ultimostatus_descricaostatus: Series[str] = Field(nullable=True)

    class Config:
        strict = (
            False  # O DataFrame deve conter exatamente as colunas declaradas no schema.
        )
        coerce = True  # Tenta converter os valores. Se algum valor não puder ser convertido, ele vira NaN ao invés de gerar erro.


class GovernismoSchema(DataFrameModel):
    """VALIDACAO DE RADAR CONGRESSO GOVERNISMO"""

    id: Series[int]
    afavor: Series[int]
    n: Series[int]
    total: Series[int]
    trimestre: Series[str]
    perc_governismo: Series[float] = pa.Field(nullable=True)

    class Config:
        strict = False
        coerce = True


class ParlamentarRadarSchema(DataFrameModel):
    """VALIDACAO DE RADAR CONGRESSO PARLAMENTARES"""

    idparlamentarvoz: Series[str] = Field(nullable=True)
    idparlamentar: Series[str] = Field(nullable=True)
    casa: Series[str] = Field(nullable=True)
    nomeeleitoral: Series[str] = Field(nullable=True)
    nomeprocessado: Series[str] = Field(nullable=True)
    uf: Series[str] = Field(nullable=True)
    ultima_legislatura: Series[str] = Field(nullable=True)
    emexercicio: Series[str] = Field(nullable=True)
    parlamentarpartido: Series[str] = Field(nullable=True)

    class Config:
        strict = False
        coerce = True


class SenadoresRadarSchema(DataFrameModel):
    """VALIDACAO SENADO"""

    identificacaoparlamentar_codigoparlamentar: Series[int] = Field(nullable=False)
    identificacaoparlamentar_codigopubliconalegatual: Series[int] = Field(
        nullable=False
    )
    identificacaoparlamentar_nomeparlamentar: Series[str] = Field(nullable=False)
    identificacaoparlamentar_nomecompletoparlamentar: Series[str] = Field(
        nullable=False
    )
    identificacaoparlamentar_sexoparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_formatratamento: Series[str] = Field(nullable=True)
    identificacaoparlamentar_urlfotoparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_urlpaginaparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_emailparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_telefones_telefone: Series[str] = Field(nullable=True)
    identificacaoparlamentar_siglapartidoparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_ufparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_bloco_codigobloco: Series[str] = Field(nullable=True)
    identificacaoparlamentar_bloco_nomebloco: Series[str] = Field(nullable=True)
    identificacaoparlamentar_bloco_nomeapelido: Series[str] = Field(nullable=True)
    identificacaoparlamentar_bloco_datacriacao: Series[str] = Field(nullable=True)
    identificacaoparlamentar_membromesa: Series[str] = Field(nullable=True)
    identificacaoparlamentar_membrolideranca: Series[str] = Field(nullable=True)
    mandato_codigomandato: Series[str] = Field(nullable=True)
    mandato_ufparlamentar: Series[str] = Field(nullable=True)
    mandato_primeiralegislaturadomandato_numerolegislatura: Series[str] = Field(
        nullable=True
    )
    mandato_primeiralegislaturadomandato_datainicio: Series[str] = Field(nullable=True)
    mandato_primeiralegislaturadomandato_datafim: Series[str] = Field(nullable=True)
    mandato_segundalegislaturadomandato_numerolegislatura: Series[str] = Field(
        nullable=True
    )
    mandato_segundalegislaturadomandato_datainicio: Series[str] = Field(nullable=True)
    mandato_segundalegislaturadomandato_datafim: Series[str] = Field(nullable=True)
    mandato_descricaoparticipacao: Series[str] = Field(nullable=True)
    mandato_suplentes_suplente: Series[str] = Field(nullable=True)
    mandato_exercicios_exercicio: Series[str] = Field(nullable=True)
    mandato_titular_descricaoparticipacao: Series[str] = Field(nullable=True)
    mandato_titular_codigoparlamentar: Series[str] = Field(nullable=True)
    mandato_titular_nomeparlamentar: Series[str] = Field(nullable=True)
    identificacaoparlamentar_urlpaginaparticular: Series[str] = Field(nullable=True)

    class Config:
        strict = False
        coerce = True
