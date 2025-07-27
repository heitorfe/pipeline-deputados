{{
  config(
    materialized='view'
  )
}}

with eventos_raw as (
  select
    cast(id as integer) as evento_id,
    uri as evento_uri,
    url_documento_pauta as url_documento_pauta,
    cast(data_hora_inicio as timestamp) as data_hora_inicio,
    cast(data_hora_fim as timestamp) as data_hora_fim,
    situacao,
    descricao,
    descricao_tipo as descricao_tipo,
    local_externo as local_externo,
    
    -- Local da Câmara fields
    local_camara_nome as local_camara_nome,
    -- "localCamara.predio" as local_camara_predio,
    -- "localCamara.sala" as local_camara_sala,
    -- "localCamara.andar" as local_camara_andar,
    
    -- Metadata
    current_timestamp() as data_carga,
    '{{ var("data_fonte", "api_camara") }}' as fonte
    
  from {{ source('camara_raw', 'eventos') }}
  where id is not null
)

select * from eventos_raw
