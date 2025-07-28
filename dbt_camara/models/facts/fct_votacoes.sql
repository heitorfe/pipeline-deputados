{{
  config(
    materialized='table'
  )
}}

with votacoes_base as (
  select
    id_votacao,
    data_votacao,
    proposicao_id,
    proposicao_sigla_tipo,
    proposicao_numero,
    proposicao_ano,
    ano,
    mes
  from {{ ref('stg_votacoes') }}
),

tempo_ref as (
  select sk_tempo, ano, mes from {{ ref('dim_tempo') }}
),

proposicoes_info as (
  select 
    proposicao_id,
    max(case when eh_proponente then deputado_autor_id else null end) as deputado_proponente_id
  from {{ ref('stg_proposicoes_autores') }}
  group by proposicao_id
),

votos_agregados as (
  select
    v.id_votacao,
    count(*) as total_votos,
    sum(case when v.tipo_voto = 'SIM' then 1 else 0 end) as votos_sim,
    sum(case when v.tipo_voto in ('NÃO', 'NAO') then 1 else 0 end) as votos_nao,
    sum(case when v.tipo_voto = 'ABSTENÇÃO' then 1 else 0 end) as votos_abstencao,
    sum(case when v.tipo_voto = 'OBSTRUÇÃO' then 1 else 0 end) as votos_obstrucao,
    sum(case when v.tipo_voto = 'ARTIGO 17' then 1 else 0 end) as votos_artigo17,
    sum(case when v.tipo_voto not in ('SIM', 'NÃO', 'NAO', 'ABSTENÇÃO', 'OBSTRUÇÃO', 'ARTIGO 17') then 1 else 0 end) as votos_outros
  from {{ ref('stg_votos') }} v
  group by v.id_votacao
),

fct_votacoes as (
  select
    -- Chaves
    {{ dbt_utils.generate_surrogate_key(['v.id_votacao']) }} as sk_votacao,
    v.id_votacao,
    t.sk_tempo,
    v.proposicao_id,
    p.deputado_proponente_id,
    
    -- Dimensões
    v.data_votacao,
    v.proposicao_sigla_tipo,
    v.proposicao_numero,
    v.proposicao_ano,
    
    -- Métricas de votação
    coalesce(va.total_votos, 0) as total_votos,
    coalesce(va.votos_sim, 0) as votos_sim,
    coalesce(va.votos_nao, 0) as votos_nao,
    coalesce(va.votos_abstencao, 0) as votos_abstencao,
    coalesce(va.votos_obstrucao, 0) as votos_obstrucao,
    coalesce(va.votos_artigo17, 0) as votos_artigo17,
    coalesce(va.votos_outros, 0) as votos_outros,
    
    -- Métricas calculadas
    case 
      when coalesce(va.votos_sim, 0) > coalesce(va.votos_nao, 0) then true
      else false
    end as foi_aprovada,
    
    case 
      when coalesce(va.total_votos, 0) > 0 
      then round((coalesce(va.votos_sim, 0) * 100.0 / va.total_votos), 2)
      else 0
    end as percentual_aprovacao,
    
    case 
      when coalesce(va.total_votos, 0) > 0 
      then round((coalesce(va.votos_nao, 0) * 100.0 / va.total_votos), 2)
      else 0
    end as percentual_rejeicao,
    
    case 
      when coalesce(va.total_votos, 0) > 0 
      then round((coalesce(va.votos_abstencao, 0) * 100.0 / va.total_votos), 2)
      else 0
    end as percentual_abstencao,
    
    -- Flags
    case when coalesce(va.votos_sim, 0) + coalesce(va.votos_nao, 0) = 0 then true else false end as eh_votacao_sem_resultado,
    case when coalesce(va.votos_obstrucao, 0) > 0 then true else false end as teve_obstrucao,
    case when coalesce(va.total_votos, 0) >= 300 then true else false end as eh_votacao_expressiva,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from votacoes_base v
  left join tempo_ref t on v.ano = t.ano and v.mes = t.mes
  left join proposicoes_info p on v.proposicao_id = p.proposicao_id
  left join votos_agregados va on v.id_votacao = va.id_votacao
)

select * from fct_votacoes
