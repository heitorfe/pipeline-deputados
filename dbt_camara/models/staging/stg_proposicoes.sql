{{
  config(
    materialized='view'
  )
}}

with proposicoes_raw as (
  select
    cast(id as integer) as proposicao_id,
    uri as proposicao_uri,
    sigla_tipo as sigla_tipo,
    cast(numero as integer) as numero,
    cast(ano as integer) as ano,
    cast(cod_tipo as integer) as cod_tipo,
    descricao_tipo as descricao_tipo,
    ementa,
    ementa_detalhada as ementa_detalhada,
    keywords,
    cast(data_apresentacao as timestamp) as data_apresentacao,
    uri_orgao_numerador as uri_orgao_numerador,
    uri_prop_anterior as uri_prop_anterior,
    uri_prop_principal as uri_prop_principal,
    uri_prop_posterior as uri_prop_posterior,
    url_inteiro_teor as url_inteiro_teor,
    -- urnFinal as urn_final,
    
    -- Ultimo Status fields
    cast(ultimo_status_data_hora as timestamp) as ultimo_status_data_hora,
    cast(ultimo_status_sequencia as integer) as ultimo_status_sequencia,
    ultimo_status_uri_relator as ultimo_status_uri_relator,
    cast(ultimo_status_id_orgao as integer) as ultimo_status_id_orgao,
    ultimo_status_sigla_orgao as ultimo_status_sigla_orgao,
    ultimo_status_uri_orgao as ultimo_status_uri_orgao,
    ultimo_status_regime as ultimo_status_regime,
    ultimo_status_descricao_tramitacao as ultimo_status_descricao_tramitacao,
    cast(ultimo_status_id_tipo_tramitacao as integer) as ultimo_status_id_tipo_tramitacao,
    ultimo_status_descricao_situacao as ultimo_status_descricao_situacao,
    cast(ultimo_status_id_situacao as integer) as ultimo_status_id_situacao,
    ultimo_status_despacho as ultimo_status_despacho,
    ultimo_status_apreciacao as ultimo_status_apreciacao,
    ultimo_status_url as ultimo_status_url,
    
    -- Metadata
    current_timestamp() as data_carga,
    '{{ var("data_fonte", "api_camara") }}' as fonte
    
  from {{ source('camara_raw', 'proposicoes') }}
  where id is not null
)

select * from proposicoes_raw
