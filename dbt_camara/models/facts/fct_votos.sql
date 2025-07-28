{{
  config(
    materialized='table'
  )
}}

with votos_base as (
  select
    id_votacao,
    deputado_id,
    tipo_voto,
    data_registro_voto,
    ano,
    mes
  from {{ ref('stg_votos') }}
),

tempo_ref as (
  select sk_tempo, ano, mes from {{ ref('dim_tempo') }}
),

deputados_ref as (
  select sk_deputado, nk_deputado as deputado_id from {{ ref('dim_deputados') }}
),

votacoes_ref as (
  select 
    sk_votacao, 
    id_votacao, 
    proposicao_id, 
    proposicao_sigla_tipo,
    foi_aprovada,
    deputado_proponente_id
  from {{ ref('fct_votacoes') }}
),

fct_votos as (
  select
    -- Chaves
    {{ dbt_utils.generate_surrogate_key(['v.id_votacao', 'v.deputado_id']) }} as sk_voto,
    vr.sk_votacao,
    d.sk_deputado,
    t.sk_tempo,
    
    -- Identificadores
    v.id_votacao,
    v.deputado_id,
    vr.proposicao_id,
    vr.proposicao_sigla_tipo,
    
    -- Dimensões do voto
    v.tipo_voto,
    v.data_registro_voto,
    
    -- Flags de análise
    case when v.tipo_voto = 'SIM' then 1 else 0 end as eh_voto_sim,
    case when v.tipo_voto in ('NÃO', 'NAO') then 1 else 0 end as eh_voto_nao,
    case when v.tipo_voto = 'ABSTENÇÃO' then 1 else 0 end as eh_voto_abstencao,
    case when v.tipo_voto = 'OBSTRUÇÃO' then 1 else 0 end as eh_voto_obstrucao,
    case when v.tipo_voto = 'ARTIGO 17' then 1 else 0 end as eh_voto_artigo17,
    
    -- Análises contextuais
    case 
      when vr.foi_aprovada and v.tipo_voto = 'SIM' then 1
      when not vr.foi_aprovada and v.tipo_voto in ('NÃO', 'NAO') then 1
      else 0
    end as eh_voto_alinhado_resultado,
    
    case 
      when vr.deputado_proponente_id = v.deputado_id then 1 
      else 0 
    end as eh_voto_proprio_autor,
    
    case 
      when vr.deputado_proponente_id = v.deputado_id and v.tipo_voto = 'SIM' then 1
      else 0
    end as eh_autor_votou_favor,
    
    case 
      when vr.deputado_proponente_id = v.deputado_id and v.tipo_voto in ('NÃO', 'NAO') then 1
      else 0
    end as eh_autor_votou_contra,
    
    -- Categorização do voto
    case 
      when v.tipo_voto = 'SIM' then 'FAVORAVEL'
      when v.tipo_voto in ('NÃO', 'NAO') then 'CONTRARIO'
      when v.tipo_voto = 'ABSTENÇÃO' then 'NEUTRO'
      when v.tipo_voto = 'OBSTRUÇÃO' then 'OBSTRUCAO'
      when v.tipo_voto = 'ARTIGO 17' then 'PROCEDURAL'
      else 'OUTROS'
    end as categoria_voto,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from votos_base v
  left join tempo_ref t on v.ano = t.ano and v.mes = t.mes
  left join deputados_ref d on v.deputado_id = d.deputado_id
  left join votacoes_ref vr on v.id_votacao = vr.id_votacao
)

select * from fct_votos
