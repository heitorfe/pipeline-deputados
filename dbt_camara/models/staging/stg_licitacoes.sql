{{
  config(
    materialized='view'
  )
}}

with licitacoes_raw as (
  select
    cast(id_licitacao as integer) as licitacao_id,
    cast(numero as integer) as numero,
    cast(ano as integer) as ano,
    num_processo as num_processo,
    cast(ano_processo as integer) as ano_processo,
    objeto,
    modalidade,
    tipo,
    situacao,
    cast(replace(vlr_estimado, ',', '.') as decimal(15,2)) as valor_estimado,
    cast(replace(vlr_contratado, ',', '.') as decimal(15,2)) as valor_contratado,
    cast(replace(vlr_pago, ',', '.') as decimal(15,2)) as valor_pago,
    cast(data_autorizacao as date) as data_autorizacao,
    cast(data_publicacao as date) as data_publicacao,
    -- cast(data_abertura as date) as data_abertura,
    cast(num_itens as integer) as num_itens,
    cast(num_unidades as integer) as num_unidades,
    cast(num_propostas as integer) as num_propostas,
    cast(num_contratos as integer) as num_contratos,

    -- Metadata
    current_timestamp() as data_carga,
    '{{ var("data_fonte", "api_camara") }}' as fonte
    
  from {{ source('camara_raw', 'licitacoes') }}
  where id_licitacao is not null
)

select * from licitacoes_raw
