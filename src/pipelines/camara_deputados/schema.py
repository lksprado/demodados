# schema_pandera.py
import pandera.pandas as pa  # Recomendação atual do Pandera
from pandera.typing import Series
from pandera import Field
import pandas as pd
from typing import Optional

class DeputadoSchema(pa.DataFrameModel):
    id: Series[int] = Field(nullable=False)
    uri: Series[str] = Field(nullable=False)
    nomecivil: Series[str]= Field(nullable=False)
    cpf: Series[int] = Field(nullable=False)
    sexo: Series[str]= Field(nullable=True)
    urlwebsite: Series[str] = Field(nullable=True)
    redesocial: Series[str] = Field(nullable=True)
    datanascimento: Series[str]= Field(nullable=True)
    datafalecimento: Series[str] = Field(nullable=True)
    ufnascimento: Series[str]= Field(nullable=True)
    municipionascimento: Series[str] = Field(nullable=True)
    escolaridade: Series[str] = Field(nullable=True)
    ultimostatus_id: Series[int] = Field(nullable=False)
    ultimostatus_uri: Series[str]= Field(nullable=False)
    ultimostatus_nome: Series[str]= Field(nullable=False)
    ultimostatus_siglapartido: Series[str]= Field(nullable=True)
    ultimostatus_uripartido: Series[str] = Field(nullable=True)
    ultimostatus_siglauf: Series[str] = Field(nullable=True)
    ultimostatus_idlegislatura: Series[int]= Field(nullable=False)
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
        strict = True
        coerce = True
