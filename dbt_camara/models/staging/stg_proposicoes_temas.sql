{{
  config(
    materialized='view'
  )
}}

with proposicoes_temas_raw as (
  select
    uri_proposicao as uri_proposicao,
    sigla_tipo as sigla_tipo,
    cast(numero as integer) as numero,
    cast(ano as integer) as ano,
    cast(cod_tema as integer) as cod_tema,
    tema,
    cast(relevancia as integer) as relevancia,
    
    -- Extract proposicao_id from URI using Snowflake syntax
    cast(regexp_substr(uri_proposicao, '[0-9]+$') as integer) as proposicao_id,
    
    -- Metadata
    current_timestamp() as data_carga,
    '{{ var("data_fonte", "api_camara") }}' as fonte
    
  from {{ source('camara_raw', 'proposicoes_temas') }}
  where uri_proposicao is not null and cod_tema is not null
)

select * from proposicoes_temas_raw
