WITH source AS (
    SELECT *
    FROM {{ source('radar','radarcongresso__governismo_deputados_raw')}}
),

renamed AS (
    SELECT
        id::int AS id,
        afavor::int AS total_votos_favor_governo,
        n::int AS total_votos_contra_governo,
        total::int AS perc_governismo,
        date_trunc(
            'quarter', to_date(trimestre, 'YYYY-MM-DD')
        )::date AS trimestre,
        perc_governismo::int AS perc_governismo_trimestre,
        data_carga
    FROM source
)

SELECT * FROM renamed
