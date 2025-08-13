WITH
source AS (
    SELECT * FROM {{ ref('brz_radarcongresso__governismo_deputados')}}
),

source2 AS (
    SELECT * FROM {{ ref('brz_radarcongresso__deputados_detalhes')}}
),

renamed AS (
    SELECT
        t2.id_deputado_congresso AS id,
        trimestre,
        perc_governismo_trimestre,
        data_carga
    FROM
        source AS t1
    INNER JOIN source2 AS t2
        ON t1.id = t2.id_deputado_radar
    WHERE perc_governismo_trimestre IS NOT null
)

SELECT * FROM renamed
