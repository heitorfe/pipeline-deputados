{{
  config(
    materialized='table'
  )
}}

with legislaturas_seed as (
  select * from {{ ref('legislaturas') }}
),

legislaturas_dim as (
  select
    cast(idLegislatura as integer) as legislatura_id,
    uri as legislatura_uri,
    cast(dataInicio as date) as data_inicio,
    cast(dataFim as date) as data_fim,
    cast(anoEleicao as integer) as ano_eleicao,
    
    -- Calculated fields
    extract(year from cast(dataInicio as date)) as ano_inicio,
    extract(year from cast(dataFim as date)) as ano_fim,
    case 
      when cast(dataFim as date) >= current_date() then true 
      else false 
    end as eh_legislatura_atual,
    
    -- Duration in days
    datediff(day, cast(dataInicio as date), cast(dataFim as date)) as duracao_dias,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from legislaturas_seed
  where idLegislatura is not null
)

select * from legislaturas_dim
