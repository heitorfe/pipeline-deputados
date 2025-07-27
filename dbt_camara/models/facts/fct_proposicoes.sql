{{
  config(
    materialized='table'
  )
}}

with proposicoes_staging as (
  select * from {{ ref('stg_proposicoes') }}
),

proposicoes_autores_staging as (
  select * from {{ ref('stg_proposicoes_autores') }}
),

proposicoes_temas_staging as (
  select * from {{ ref('stg_proposicoes_temas') }}
),

fct_proposicoes as (
  select
    p.proposicao_id,
    p.proposicao_uri,
    p.sigla_tipo,
    p.numero,
    p.ano,
    p.cod_tipo,
    p.descricao_tipo,
    p.ementa,
    p.ementa_detalhada,
    p.keywords,
    p.data_apresentacao,
    
    -- Status atual
    p.ultimo_status_data_hora,
    p.ultimo_status_id_orgao,
    p.ultimo_status_sigla_orgao,
    p.ultimo_status_descricao_tramitacao,
    p.ultimo_status_descricao_situacao,
    p.ultimo_status_id_situacao,
    
    -- Métricas de autoria
    count(distinct case when pa.eh_proponente = true then pa.deputado_autor_id end) as num_proponentes,
    count(distinct pa.deputado_autor_id) as num_autores_total,
    
    -- Métricas de temas
    count(distinct pt.cod_tema) as num_temas,
    
    -- Flags calculadas
    case when p.ultimo_status_id_situacao = 1140 then true else false end as eh_transformada_norma,
    case when p.ultimo_status_id_situacao = 924 then true else false end as eh_pronta_pauta,
    case when extract(year from p.data_apresentacao) = extract(year from current_date()) then true else false end as eh_ano_atual,
    
    -- Tempo de tramitação
    datediff(day, current_date(), cast(p.data_apresentacao as date)) as dias_tramitacao,
    
    -- Metadata
    p.data_carga,
    p.fonte
    
  from proposicoes_staging p
  left join proposicoes_autores_staging pa on p.proposicao_id = pa.proposicao_id
  left join proposicoes_temas_staging pt on p.proposicao_id = pt.proposicao_id
  group by
    p.proposicao_id, p.proposicao_uri, p.sigla_tipo, p.numero, p.ano, p.cod_tipo,
    p.descricao_tipo, p.ementa, p.ementa_detalhada, p.keywords, p.data_apresentacao,
    p.ultimo_status_data_hora, p.ultimo_status_id_orgao, p.ultimo_status_sigla_orgao,
    p.ultimo_status_descricao_tramitacao, p.ultimo_status_descricao_situacao,
    p.ultimo_status_id_situacao, p.data_carga, p.fonte
)

select * from fct_proposicoes
