{{
  config(
    materialized='table'
  )
}}

with frentes_seed as (
  select * from {{ ref('frentes') }}
),

frentes_dim as (
  select
    cast(id as integer) as frente_id,
    uri as frente_uri,
    titulo,
    cast(dataCriacao as date) as data_criacao,
    cast(idLegislatura as integer) as id_legislatura,
    telefone,
    email,
    keywords,
    cast(idSituacao as integer) as id_situacao,
    situacao,
    urlWebsite as url_website,
    urlDocumento as url_documento,
    
    -- Coordenador fields
    cast(coordenador_id as integer) as coordenador_id,
    coordenador_uri,
    coordenador_nome,
    coordenador_siglaPartido as coordenador_sigla_partido,
    coordenador_uriPartido as coordenador_uri_partido,
    coordenador_siglaUf as coordenador_sigla_uf,
    cast(coordenador_idLegislatura as integer) as coordenador_id_legislatura,
    coordenador_urlFoto as coordenador_url_foto,
    
    -- Calculated fields
    extract(year from cast(dataCriacao as date)) as ano_criacao,
    case when idSituacao = '1' then true else false end as eh_ativa,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from frentes_seed
  where id is not null
)

select * from frentes_dim
