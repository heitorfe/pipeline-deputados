{{
  config(
    materialized='table'
  )
}}

with tecad_categorias_seed as (
  select * from {{ source('raw', 'tecadCategorias') }}
),

tecad_termos_seed as (
  select * from {{ source('raw', 'tecadTermos') }}
),

categorias_dim as (
  select
    cast(codCategoria as integer) as categoria_id,
    categoria as categoria_nome,
    categoria as categoria_descricao,
    current_timestamp() as data_carga
  from tecad_categorias_seed
  where codCategoria is not null
),

termos_dim as (
  select
    cast(codTermo as integer) as termo_id,
    termo as termo_nome,
    categorias as categoria_id,
    termo as termo_descricao,
    current_timestamp() as data_carga
  from tecad_termos_seed
  where codTermo is not null
),

tecad_completo as (
  select
    t.termo_id,
    t.termo_nome,
    t.termo_descricao,
    t.categoria_id,
    c.categoria_nome,
    c.categoria_descricao,
    t.data_carga
  from termos_dim t
  left join categorias_dim c on t.categoria_id = c.categoria_nome
)

select * from tecad_completo
