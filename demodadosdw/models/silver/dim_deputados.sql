WITH
source AS (
    SELECT * FROM {{ ref('brz_camara__deputados')}}
)

SELECT * FROM source
