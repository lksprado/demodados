WITH source AS (
    SELECT * FROM {{ source('radar','radarcongresso__deputados_raw')}}
),

renamed AS (
    SELECT
        id_parlamentar_voz::int AS id_deputado_radar,
        id_parlamentar::int AS id_deputado_congresso,
        telefone,
        email,
        transparenciaparlamentar_estrelas::int,
        upper(raca) AS raca
    FROM source
)

SELECT * FROM renamed
