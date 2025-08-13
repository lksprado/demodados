{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('camara','camara__deputados_raw') }}
),

renamed AS (
    SELECT
        id,
        nomecivil AS nome_civil,
        sexo,
        ufnascimento AS uf_nascimento,
        municipionascimento AS municipio_nascimento,
        escolaridade,
        ultimostatus_siglapartido AS partido_atual,
        ultimostatus_siglauf AS uf_atual,
        ultimostatus_idlegislatura AS id_legislatura_atual,
        ultimostatus_urlfoto AS foto_atual,
        ultimostatus_data AS posse_data,
        ultimostatus_nomeeleitoral AS nome_eleitoral_atual,
        ultimostatus_gabinete_nome AS numero_gabinete_sala_atual,
        ultimostatus_gabinete_predio AS numero_gabinete_predio_atual,
        ultimostatus_gabinete_andar AS numero_gabinete_andar_atual,
        ultimostatus_gabinete_telefone AS telefone_gabinete_atual,
        ultimostatus_gabinete_email AS email_gabinete_atual,
        ultimostatus_situacao AS situacao_atual,
        ultimostatus_condicaoeleitoral AS condicao_eleitoral_atual,
        data_carga,
        CASE
            WHEN length((cpf::TEXT)) = 9 THEN '00' || (cpf::TEXT)
            WHEN length((cpf::TEXT)) = 10 THEN '0' || (cpf::TEXT)
            ELSE cpf::TEXT
        END AS cpf,
        replace(replace(replace(redesocial, '[', ''), ']', ''), '''', '')
        AS redesociais,
        to_date(datanascimento, 'YYYY-MM-DD') AS data_nascimento,
        to_date(datafalecimento, 'YYYY-MM-DD') AS data_falecimento
    FROM source
),

redesocial_tratamento AS (
    SELECT
        id,
        trim(split_part(redesociais, ',', 1)) AS rede_1,
        trim(split_part(redesociais, ',', 2)) AS rede_2,
        trim(split_part(redesociais, ',', 3)) AS rede_3,
        trim(split_part(redesociais, ',', 4)) AS rede_4
    FROM renamed
),

redesocial_tratamento_2 AS (
    SELECT
        id,
        CASE
            WHEN rede_1 LIKE '%twitter%' THEN rede_1
            WHEN rede_2 LIKE '%twitter%' THEN rede_2
            WHEN rede_3 LIKE '%twitter%' THEN rede_3
            WHEN rede_4 LIKE '%twitter%' THEN rede_4
        END AS redesocial_x_twitter,
        CASE
            WHEN rede_1 LIKE '%facebook%' THEN rede_1
            WHEN rede_2 LIKE '%facebook%' THEN rede_2
            WHEN rede_3 LIKE '%facebook%' THEN rede_3
            WHEN rede_4 LIKE '%facebook%' THEN rede_4
        END AS redesocial_facebook,
        CASE
            WHEN rede_1 LIKE '%instagram%' THEN rede_1
            WHEN rede_2 LIKE '%instagram%' THEN rede_2
            WHEN rede_3 LIKE '%instagram%' THEN rede_3
            WHEN rede_4 LIKE '%instagram%' THEN rede_4
        END AS redesocial_instagram,
        CASE
            WHEN rede_1 LIKE '%youtube%' THEN rede_1
            WHEN rede_2 LIKE '%youtube%' THEN rede_2
            WHEN rede_3 LIKE '%youtube%' THEN rede_3
            WHEN rede_4 LIKE '%youtube%' THEN rede_4
        END AS redesocial_youtube
    FROM redesocial_tratamento
)

SELECT
    t1.id,
    t1.nome_civil,
    t1.nome_eleitoral_atual,
    t1.partido_atual,
    t1.email_gabinete_atual,
    t2.redesocial_x_twitter,
    t2.redesocial_instagram,
    t2.redesocial_facebook,
    t2.redesocial_youtube,
    t1.redesociais,
    t1.telefone_gabinete_atual,
    t1.cpf,
    t1.sexo,
    t1.data_nascimento,
    t1.data_falecimento,
    t1.uf_nascimento,
    t1.municipio_nascimento,
    t1.uf_atual,
    t1.escolaridade,
    t1.id_legislatura_atual,
    t1.posse_data,
    t1.numero_gabinete_predio_atual,
    t1.numero_gabinete_andar_atual,
    t1.numero_gabinete_sala_atual,
    t1.situacao_atual,
    t1.condicao_eleitoral_atual,
    t1.foto_atual,
    t1.data_carga
FROM renamed AS t1
INNER JOIN redesocial_tratamento_2 AS t2
    ON t1.id = t2.id
