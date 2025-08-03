{{
  config(materialized='view')
}}

with proposicoes_autores_raw as (
  select
    cast(id_proposicao as integer) as proposicao_id,
    uri_proposicao as uri_proposicao,
    cast(id_deputado_autor as integer) as deputado_autor_id,
    uri_autor as uri_autor,
    cast(cod_tipo_autor as integer) as cod_tipo_autor,
    tipo_autor as tipo_autor,
    nome_autor as nome_autor,
    sigla_partido_autor as sigla_partido_autor,
    uri_partido_autor as uri_partido_autor,
    sigla_uf_autor as sigla_uf_autor,
    cast(ordem_assinatura as integer) as ordem_assinatura,
    case 
      when proponente = 'true' then true
      when proponente = 'false' then false
      else null
    end as eh_proponente,
    
    current_timestamp() as data_carga,
    'api_camara' as fonte
    
  from {{ source('camara_raw', 'proposicoes_autores')}}
  where id_proposicao is not null 
    and id_deputado_autor is not null
)

select * from proposicoes_autores_raw
