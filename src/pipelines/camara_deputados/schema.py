# schema_pandera.py
from typing import Optional

import pandas as pd
import pandera.pandas as pa  # Recomendação atual do Pandera
from pandera import DataFrameModel, Field
from pandera.typing import Series


class DeputadoSchema(DataFrameModel):
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


class GovernismoDeputadoSchema(DataFrameModel):
    id: Series[int]
    afavor: Series[int]
    n: Series[int]
    total: Series[int]
    trimestre: Series[str]
    perc_governismo: Series[float] = pa.Field(nullable=True)

    class Config:
        strict = False
        coerce = True
