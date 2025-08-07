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
t1.total_votos_favor_governo,
t1.total_votos_contra_governo,
t1.perc_governismo,
t1.data_carga
FROM 
source t1
inner join source2 t2
on t1.id = t2.id_deputado_radar
GROUP BY 
t2.id_deputado_congresso,
total_votos_favor_governo,
total_votos_contra_governo,
perc_governismo,
data_carga
)
select * from renamed