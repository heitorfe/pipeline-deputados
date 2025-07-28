{{ config(
    materialized='view'
) }}

with despesas_agregadas as (
  select 
    dd.nk_deputado,
    dd.nome_deputado,
    dd.sigla_partido,
    dd.sigla_uf,
    dt.ano,
    dt.id_legislatura,
    sum(f.valor_liquido) as total_gasto,
    count(f.cod_documento) as total_despesas,
    count(distinct f.sk_fornecedor) as qtd_fornecedores_distintos,
    avg(f.valor_liquido) as valor_medio_despesa,
    max(f.valor_liquido) as maior_despesa,
    count(distinct dt.mes) as meses_com_despesa
  from {{ ref('fct_despesas') }} f
  inner join {{ ref('dim_deputados') }} dd on f.sk_deputado = dd.sk_deputado
  inner join {{ ref('dim_tempo') }} dt on f.sk_tempo = dt.sk_tempo
  group by dd.nk_deputado, dd.nome_deputado, dd.sigla_partido, dd.sigla_uf, dt.ano, dt.id_legislatura
),

votos_agregados as (
  select 
    dd.nk_deputado,
    dt.ano,
    count(*) as total_votos,
    sum(fv.eh_voto_sim) as votos_sim,
    sum(fv.eh_voto_nao) as votos_nao,
    sum(fv.eh_voto_abstencao) as votos_abstencao,
    sum(fv.eh_voto_obstrucao) as votos_obstrucao,
    sum(fv.eh_voto_alinhado_resultado) as votos_alinhados_resultado,
    sum(fv.eh_voto_proprio_autor) as votos_proprias_proposicoes,
    count(distinct fv.id_votacao) as sessoes_votacao_participou,
    round(avg(case when fv.eh_voto_sim = 1 then 100.0 else 0.0 end), 2) as percentual_votos_sim,
    round(avg(case when fv.eh_voto_nao = 1 then 100.0 else 0.0 end), 2) as percentual_votos_nao,
    round(avg(case when fv.eh_voto_alinhado_resultado = 1 then 100.0 else 0.0 end), 2) as percentual_alinhamento
  from {{ ref('fct_votos') }} fv
  inner join {{ ref('dim_deputados') }} dd on fv.sk_deputado = dd.sk_deputado  
  inner join {{ ref('dim_tempo') }} dt on fv.sk_tempo = dt.sk_tempo
  group by dd.nk_deputado, dt.ano
),

proposicoes_como_autor as (
  select 
    pa.deputado_autor_id as nk_deputado,
    extract(year from fvot.data_votacao) as ano,
    count(distinct pa.proposicao_id) as proposicoes_apresentadas,
    count(distinct case when fvot.foi_aprovada then pa.proposicao_id end) as proposicoes_aprovadas,
    count(distinct case when not fvot.foi_aprovada then pa.proposicao_id end) as proposicoes_rejeitadas,
    count(distinct case when pa.eh_proponente then pa.proposicao_id end) as proposicoes_como_proponente
  from {{ ref('stg_proposicoes_autores') }} pa
  left join {{ ref('fct_votacoes') }} fvot on pa.proposicao_id = fvot.proposicao_id
  where fvot.data_votacao is not null
  group by pa.deputado_autor_id, extract(year from fvot.data_votacao)
),

ranking_final as (
  select 
    -- Identificação do deputado
    d.nk_deputado,
    d.nome_deputado,
    d.sigla_partido,
    d.sigla_uf,
    d.ano,
    d.id_legislatura,
    
    -- Métricas de despesas
    coalesce(d.total_gasto, 0) as total_gasto,
    coalesce(d.total_despesas, 0) as total_despesas,
    coalesce(d.qtd_fornecedores_distintos, 0) as qtd_fornecedores_distintos,
    coalesce(d.valor_medio_despesa, 0) as valor_medio_despesa,
    coalesce(d.maior_despesa, 0) as maior_despesa,
    coalesce(d.meses_com_despesa, 0) as meses_com_despesa,
    
    -- Métricas de votação
    coalesce(v.total_votos, 0) as total_votos,
    coalesce(v.votos_sim, 0) as votos_sim,
    coalesce(v.votos_nao, 0) as votos_nao,
    coalesce(v.votos_abstencao, 0) as votos_abstencao,
    coalesce(v.votos_obstrucao, 0) as votos_obstrucao,
    coalesce(v.votos_alinhados_resultado, 0) as votos_alinhados_resultado,
    coalesce(v.votos_proprias_proposicoes, 0) as votos_proprias_proposicoes,
    coalesce(v.sessoes_votacao_participou, 0) as sessoes_votacao_participou,
    coalesce(v.percentual_votos_sim, 0) as percentual_votos_sim,
    coalesce(v.percentual_votos_nao, 0) as percentual_votos_nao,
    coalesce(v.percentual_alinhamento, 0) as percentual_alinhamento,
    
    -- Métricas de proposições
    coalesce(p.proposicoes_apresentadas, 0) as proposicoes_apresentadas,
    coalesce(p.proposicoes_aprovadas, 0) as proposicoes_aprovadas,
    coalesce(p.proposicoes_rejeitadas, 0) as proposicoes_rejeitadas,
    coalesce(p.proposicoes_como_proponente, 0) as proposicoes_como_proponente,
    
    -- Métricas calculadas
    case 
      when coalesce(p.proposicoes_apresentadas, 0) > 0 
      then round((coalesce(p.proposicoes_aprovadas, 0) * 100.0 / p.proposicoes_apresentadas), 2)
      else 0
    end as taxa_aprovacao_proposicoes,
    
    case 
      when coalesce(v.total_votos, 0) > 0 
      then round((coalesce(v.sessoes_votacao_participou, 0) * 100.0 / v.total_votos), 2)
      else 0
    end as taxa_participacao_votacoes,
    
    -- Score de atividade legislativa (0-100)
    case 
      when coalesce(v.total_votos, 0) + coalesce(p.proposicoes_apresentadas, 0) > 0 then
        round((
          (coalesce(v.sessoes_votacao_participou, 0) * 0.4) +
          (coalesce(p.proposicoes_apresentadas, 0) * 0.3) +
          (coalesce(p.proposicoes_aprovadas, 0) * 0.3)
        ), 2)
      else 0
    end as score_atividade_legislativa,
    
    -- Rankings
    row_number() over (partition by d.ano order by d.total_gasto desc) as ranking_gasto_ano,
    row_number() over (partition by d.ano, d.sigla_uf order by d.total_gasto desc) as ranking_gasto_uf,
    row_number() over (partition by d.ano order by coalesce(v.total_votos, 0) desc) as ranking_participacao_votacao,
    row_number() over (partition by d.ano order by coalesce(p.proposicoes_apresentadas, 0) desc) as ranking_proposicoes,
    row_number() over (partition by d.ano order by coalesce(p.proposicoes_aprovadas, 0) desc) as ranking_aprovacoes,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from despesas_agregadas d
  left join votos_agregados v on d.nk_deputado = v.nk_deputado and d.ano = v.ano
  left join proposicoes_como_autor p on d.nk_deputado = p.nk_deputado and d.ano = p.ano
)

select * from ranking_final
order by ano desc, total_gasto desc
