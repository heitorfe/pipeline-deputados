{{
  config(
    materialized='view'
  )
}}

with fct_licitacoes as (
  select * from {{ ref('fct_licitacoes') }}
),

licitacoes_dashboard as (
  select
    ano,
    modalidade,
    tipo,
    situacao,
    categoria_valor,
    
    -- Contadores
    count(*) as total_licitacoes,
    count(case when eh_encerrada then 1 end) as licitacoes_encerradas,
    count(case when tem_contratos then 1 end) as licitacoes_com_contratos,
    count(case when tem_pagamentos then 1 end) as licitacoes_com_pagamentos,
    count(case when excedeu_estimativa then 1 end) as licitacoes_excederam_estimativa,
    
    -- Valores financeiros
    sum(valor_estimado) as total_valor_estimado,
    sum(valor_contratado) as total_valor_contratado,
    sum(valor_pago) as total_valor_pago,
    sum(diferenca_estimado_contratado) as total_diferenca_estimado,
    sum(saldo_a_pagar) as total_saldo_pagar,
    
    -- Médias
    avg(valor_estimado) as media_valor_estimado,
    avg(valor_contratado) as media_valor_contratado,
    avg(valor_pago) as media_valor_pago,
    avg(perc_variacao_estimado) as media_perc_variacao_estimado,
    avg(perc_pago) as media_perc_pago,
    
    -- Tempos de processo
    avg(dias_autorizacao_publicacao) as media_dias_autorizacao_publicacao,
    
    -- Métricas de competitividade
    avg(num_propostas) as media_propostas_por_licitacao,
    avg(num_itens) as media_itens_por_licitacao,
    
    -- Percentuais
    round(count(case when eh_encerrada then 1 end) * 100.0 / count(*), 2) as perc_encerradas,
    round(count(case when tem_contratos then 1 end) * 100.0 / count(*), 2) as perc_com_contratos,
    round(count(case when excedeu_estimativa then 1 end) * 100.0 / count(*), 2) as perc_excederam_estimativa,
    
    -- Economia/Desperdício
    case 
      when sum(valor_estimado) > 0 then
        round((sum(valor_contratado) - sum(valor_estimado)) * 100.0 / sum(valor_estimado), 2)
      else null 
    end as perc_variacao_total_estimado,
    
    -- Metadata
    current_timestamp() as data_calculo
    
  from fct_licitacoes
  group by ano, modalidade, tipo, situacao, categoria_valor
)

select * from licitacoes_dashboard
order by ano desc, total_valor_contratado desc
