with 
source as (
    select * from {{ ref('brz_camara__deputados')}}
)
select * from source