with 
dim_dep as (
    select * from {{ ref('dim_deputados')}}
),
fct_gov_total as (
    select * from {{ ref('fct_governismo_deputados_trimestre')}}
),
tab as (
    select 
    t1.id,
    t1.nome_eleitoral_atual,
    t1.partido_atual,
    t2.trimestre,
    t2.perc_governismo_trimestre
    from dim_dep t1
    inner join fct_gov_total t2
    on t1.id = t2.id 
)
select * from tab