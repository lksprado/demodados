WITH source AS (
    SELECT * FROM {{ source('radar','radarcongresso__deputados_raw')}}
),
renamed AS (
    SELECT 
        id_parlamentar_voz::int as id_deputado_radar,
        id_parlamentar::int as id_deputado_congresso,
        telefone,
        email,
        upper(raca) as raca,
        transparenciaparlamentar_estrelas:: int 
    FROM source
)
SELECT * FROM renamed