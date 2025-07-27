{{
  config(
    materialized='view'
  )
}}

with fct_proposicoes as (
  select * from {{ ref('fct_proposicoes') }}
),

dim_orgaos as (
  select * from {{ ref('dim_orgaos') }}
),

dim_legislaturas as (
  select * from {{ ref('dim_legislaturas') }}
),

proposicoes_autores as (
  select * from {{ ref('stg_proposicoes_autores') }}
),

proposicoes_temas as (
  select * from {{ ref('stg_proposicoes_temas') }}
),

proposicoes_analise as (
  select
    p.proposicao_id,
    p.sigla_tipo,
    p.numero,
    p.ano,
    p.descricao_tipo,
    p.ementa,
    p.data_apresentacao,
    
    -- Informações do órgão
    o.nome_orgao as orgao_atual,
    o.tipo_orgao,
    
    -- Informações da legislatura
    l.ano_inicio as legislatura_inicio,
    l.ano_fim as legislatura_fim,
    
    -- Autoria
    p.num_proponentes,
    p.num_autores_total,
    
    -- Temas
    p.num_temas,
    listagg(distinct pt.tema, ', ') within group (order by pt.tema asc) as temas_concatenados,
    
    -- Status e tramitação
    p.ultimo_status_descricao_situacao as situacao_atual,
    p.ultimo_status_descricao_tramitacao as tramitacao_atual,
    p.dias_tramitacao,
    p.eh_transformada_norma,
    p.eh_pronta_pauta,
    p.eh_ano_atual,
    
    -- Categorização por tipo
    case 
      when p.sigla_tipo in ('PL', 'PLP', 'PEC') then 'LEGISLACAO'
      when p.sigla_tipo in ('REQ', 'RIC', 'PDC') then 'FISCALIZACAO'
      when p.sigla_tipo in ('INC', 'MSC', 'OF') then 'COMUNICACAO'
      else 'OUTROS'
    end as categoria_proposicao,
    
    -- Análise de tramitação
    case 
      when p.dias_tramitacao <= 30 then 'RAPIDA'
      when p.dias_tramitacao <= 365 then 'NORMAL'
      when p.dias_tramitacao <= 1460 then 'LENTA'
      else 'MUITO_LENTA'
    end as velocidade_tramitacao,
    
    -- Metadata
    current_timestamp() as data_analise
    
  from fct_proposicoes p
  left join dim_orgaos o on p.ultimo_status_id_orgao = cast(regexp_substr(o.orgao_uri, '/(\\d+)$', 1, 1, 'e', 1) as integer)
  left join dim_legislaturas l on extract(year from p.data_apresentacao) between l.ano_inicio and l.ano_fim
  left join proposicoes_temas pt on p.proposicao_id = pt.proposicao_id
  group by
    p.proposicao_id, p.sigla_tipo, p.numero, p.ano, p.descricao_tipo, p.ementa,
    p.data_apresentacao, o.nome_orgao, o.tipo_orgao, l.ano_inicio, l.ano_fim,
    p.num_proponentes, p.num_autores_total, p.num_temas, p.ultimo_status_descricao_situacao,
    p.ultimo_status_descricao_tramitacao, p.dias_tramitacao, p.eh_transformada_norma,
    p.eh_pronta_pauta, p.eh_ano_atual
)

select * from proposicoes_analise
