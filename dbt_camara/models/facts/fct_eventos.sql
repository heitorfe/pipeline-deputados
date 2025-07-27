{{
  config(
    materialized='table'
  )
}}

with eventos_staging as (
  select * from {{ ref('stg_eventos') }}
),

fct_eventos as (
  select
    evento_id,
    evento_uri,
    data_hora_inicio,
    data_hora_fim,
    situacao,
    descricao,
    descricao_tipo,
    local_externo,
    local_camara_nome,
    
    -- Campos calculados
    extract(year from data_hora_inicio) as ano_evento,
    extract(month from data_hora_inicio) as mes_evento,
    extract(dayofweek from data_hora_inicio) as dia_semana,
    extract(hour from data_hora_inicio) as hora_inicio,
    
    -- Duração do evento
    case 
      when data_hora_fim is not null then 
        datediff(minute, data_hora_fim, data_hora_inicio)
      else null 
    end as duracao_minutos,
    
    -- Flags
    case when local_externo is not null and local_externo != '' then true else false end as eh_evento_externo,
    case when situacao = 'Encerrada' then true else false end as eh_encerrado,
    case when date(data_hora_inicio) = current_date() then true else false end as eh_hoje,
    case 
      when date(data_hora_inicio) between 
        dateadd(day, -7, current_date()) and current_date() 
      then true else false 
    end as eh_semana_atual,
    
    -- Metadata
    data_carga,
    fonte
    
  from eventos_staging
)

select * from fct_eventos
