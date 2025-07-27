{{
  config(
    materialized='table'
  )
}}

with orgaos_seed as (
  select * from {{ ref('orgaos') }}
),

orgaos_dim as (
  select
    uri as orgao_uri,
    sigla as sigla_orgao,
    apelido,
    nome as nome_orgao,
    nomePublicacao as nome_publicacao,
    cast(codTipoOrgao as integer) as cod_tipo_orgao,
    tipoOrgao as tipo_orgao,
    dataInicio as data_inicio_str,
    dataInstalacao as data_instalacao_str,
    dataFim as data_fim_str,
    dataFimOriginal as data_fim_original_str,
    cast(codSituacao as integer) as cod_situacao,
    descricaoSituacao as descricao_situacao,
    casa,
    sala,
    urlWebsite as url_website,
    
    -- Campos calculados simples
    case 
      when dataFim is null or trim(dataFim) = '' then true 
      else false 
    end as eh_ativo,
    
    -- Categorização por tipo
    case 
      when upper(tipoOrgao) like '%COMISSÃO%' or upper(tipoOrgao) like '%COMISSAO%' then 'COMISSAO'
      when upper(tipoOrgao) like '%PLENÁRIO%' or upper(tipoOrgao) like '%PLENARIO%' then 'PLENARIO'
      when upper(tipoOrgao) like '%MESA%' then 'MESA_DIRETORA'
      when upper(tipoOrgao) like '%CONSELHO%' then 'CONSELHO'
      else 'OUTROS'
    end as categoria_orgao,
    
    -- Flags
    case when sala is not null and trim(sala) != '' then true else false end as tem_sala_definida,
    case when urlWebsite is not null and trim(urlWebsite) != '' then true else false end as tem_website,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from orgaos_seed
  where uri is not null
)

select * from orgaos_dim
