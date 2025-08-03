with calendario as (
    select 
        dateadd(day, seq4(), '1990-01-01') as data
    from table(generator(rowcount => 15000)) -- Gera datas até cerca de 2050
),
legislaturas as (
    select
        idlegislatura,
        datainicio,
        datafim,
        anoeleicao
    from {{ source('camara_raw', 'legislaturas') }}
),
dim_tempo as (
    select
        c.data,
        extract(year from c.data) as ano,
        extract(month from c.data) as mes,
        extract(day from c.data) as dia,
        extract(quarter from c.data) as trimestre,
        extract(dow from c.data) as dia_semana,
        l.idlegislatura,
        l.anoeleicao
    from calendario c
    left join legislaturas l
        on c.data between l.datainicio and l.datafim
)
select * from dim_tempo
order by data