with 
source as (
    select * from {{ ref('brz_radarcongresso__governismo_deputados')}}
),
source2 as (
    select * from {{ ref('brz_radarcongresso__deputados_detalhes')}}
),
renamed as (
select
t2.id_deputado_congresso as id,
trimestre,
perc_governismo_trimestre,
data_carga
FROM 
source t1
inner join source2 t2
on t1.id = t2.id_deputado_radar
where perc_governismo_trimestre is not null
)
select * from renamed