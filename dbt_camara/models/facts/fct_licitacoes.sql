{{
  config(
    materialized='table'
  )
}}

with licitacoes_staging as (
  select * from {{ ref('stg_licitacoes') }}
),

fct_licitacoes as (
  select
    licitacao_id,
    numero,
    ano,
    num_processo,
    ano_processo,
    objeto,
    modalidade,
    tipo,
    situacao,
    valor_estimado,
    valor_contratado,
    valor_pago,
    data_autorizacao,
    data_publicacao,
    -- data_abertura,
    num_itens,
    num_unidades,
    num_propostas,
    num_contratos,
    
    -- Campos calculados
    coalesce(valor_contratado, 0) - coalesce(valor_estimado, 0) as diferenca_estimado_contratado,
    case 
      when valor_estimado > 0 then 
        (coalesce(valor_contratado, 0) - valor_estimado) / valor_estimado * 100
      else null 
    end as perc_variacao_estimado,
    
    coalesce(valor_contratado, 0) - coalesce(valor_pago, 0) as saldo_a_pagar,
    case 
      when valor_contratado > 0 then 
        coalesce(valor_pago, 0) / valor_contratado * 100
      else null 
    end as perc_pago,
    
    -- Tempo de processo
    case 
      when data_publicacao is not null and data_autorizacao is not null then
        datediff(day, data_autorizacao, data_publicacao)
      else null 
    end as dias_autorizacao_publicacao,
    
    
    -- Flags
    case when situacao = 'Encerrada' then true else false end as eh_encerrada,
    case when num_contratos > 0 then true else false end as tem_contratos,
    case when valor_pago > 0 then true else false end as tem_pagamentos,
    case when valor_contratado > valor_estimado then true else false end as excedeu_estimativa,
    
    -- Categorização por valor
    case 
      when valor_estimado <= 8000 then 'DISPENSAVEL'
      when valor_estimado <= 40000 then 'CONVITE'
      when valor_estimado <= 650000 then 'TOMADA_PRECOS'
      else 'CONCORRENCIA'
    end as categoria_valor,
    
    -- Metadata
    data_carga,
    fonte
    
  from licitacoes_staging
)

select * from fct_licitacoes
