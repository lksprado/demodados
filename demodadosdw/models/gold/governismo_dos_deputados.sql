WITH
dim_dep AS (
    SELECT * FROM {{ ref('dim_deputados')}}
),

fct_gov_total AS (
    SELECT * FROM {{ ref('fct_governismo_deputados_total')}}
),

tab AS (
    SELECT
        t1.id,
        t1.nome_eleitoral_atual,
        t1.partido_atual,
        t2.perc_governismo
    FROM dim_dep AS t1
    INNER JOIN fct_gov_total AS t2
        ON t1.id = t2.id
)

SELECT * FROM tab
