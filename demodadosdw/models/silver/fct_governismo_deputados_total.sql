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
        t1.total_votos_favor_governo,
        t1.total_votos_contra_governo,
        t1.perc_governismo,
        t1.data_carga
    FROM
        source AS t1
    INNER JOIN source2 AS t2
        ON t1.id = t2.id_deputado_radar
    GROUP BY
        t2.id_deputado_congresso,
        total_votos_favor_governo,
        total_votos_contra_governo,
        perc_governismo,
        data_carga
)

SELECT * FROM renamed
