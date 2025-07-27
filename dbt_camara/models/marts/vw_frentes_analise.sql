{{
  config(
    materialized='view'
  )
}}

with dim_frentes as (
  select * from {{ ref('dim_frentes') }}
),

frentes_deputados as (
  select * from {{ source('raw', 'frentesDeputados') }}
),

frentes_completa as (
  select
    f.frente_id,
    f.titulo,
    f.data_criacao,
    f.ano_criacao,
    f.id_legislatura,
    f.eh_ativa,
    f.coordenador_nome,
    f.coordenador_sigla_partido,
    f.coordenador_sigla_uf,
    f.telefone,
    f.email,
    f.url_website,
    
    -- Contagem de membros
    count(fd.deputado_id) as total_membros,
    
    -- Análise partidária
    count(distinct regexp_substr(fd.deputado_uriPartido, '[0-9]+$')) as total_partidos,
    
    -- Análise regional
    count(distinct fd.deputado_siglaUf) as total_estados,
    max(fd.deputado_siglaUf) as estado_predominante,
    
    -- Concatenação de estados
    listagg(distinct fd.deputado_siglaUf, ', ') within group (order by fd.deputado_siglaUf) as estados_participantes,
    
    -- Análise temporal
    extract(year from current_date()) - f.ano_criacao as anos_atividade,
    
    -- Categorização por tamanho
    case 
      when count(fd.deputado_id) <= 10 then 'PEQUENA'
      when count(fd.deputado_id) <= 50 then 'MEDIA'
      when count(fd.deputado_id) <= 100 then 'GRANDE'
      else 'MUITO_GRANDE'
    end as categoria_tamanho,
    
    -- Análise de diversidade
    case 
      when count(distinct regexp_substr(fd.deputado_uriPartido, '[0-9]+$')) >= 10 then 'ALTA_DIVERSIDADE'
      when count(distinct regexp_substr(fd.deputado_uriPartido, '[0-9]+$')) >= 5 then 'MEDIA_DIVERSIDADE'
      else 'BAIXA_DIVERSIDADE'
    end as diversidade_partidaria,
    
    case 
      when count(distinct fd.deputado_siglaUf) >= 15 then 'NACIONAL'
      when count(distinct fd.deputado_siglaUf) >= 8 then 'MULTI_REGIONAL'
      when count(distinct fd.deputado_siglaUf) >= 3 then 'REGIONAL'
      else 'LOCAL'
    end as abrangencia_geografica,
    
    -- Metadata
    current_timestamp() as data_analise
    
  from dim_frentes f
  left join frentes_deputados fd on f.frente_id = cast(fd.id as integer)
  group by
    f.frente_id, f.titulo, f.data_criacao, f.ano_criacao, f.id_legislatura,
    f.eh_ativa, f.coordenador_nome, f.coordenador_sigla_partido, f.coordenador_sigla_uf,
    f.telefone, f.email, f.url_website
)

select * from frentes_completa
order by total_membros desc, ano_criacao desc
