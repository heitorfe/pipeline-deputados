{{
  config(
    materialized='view'
  )
}}

with fct_eventos as (
  select * from {{ ref('fct_eventos') }}
),

eventos_resumo as (
  select
    ano_evento,
    mes_evento,
    
    -- Contadores
    count(*) as total_eventos,
    count(case when eh_encerrado then 1 end) as eventos_encerrados,
    count(case when eh_evento_externo then 1 end) as eventos_externos,
    
    -- Durações
    avg(duracao_minutos) as duracao_media_minutos,
    max(duracao_minutos) as duracao_maxima_minutos,
    min(duracao_minutos) as duracao_minima_minutos,
    
    -- Por dia da semana (1=Domingo, 7=Sábado)
    count(case when dia_semana = 1 then 1 end) as eventos_domingo,
    count(case when dia_semana = 2 then 1 end) as eventos_segunda,
    count(case when dia_semana = 3 then 1 end) as eventos_terca,
    count(case when dia_semana = 4 then 1 end) as eventos_quarta,
    count(case when dia_semana = 5 then 1 end) as eventos_quinta,
    count(case when dia_semana = 6 then 1 end) as eventos_sexta,
    count(case when dia_semana = 7 then 1 end) as eventos_sabado,
    
    -- Por horário
    count(case when hora_inicio between 6 and 11 then 1 end) as eventos_manha,
    count(case when hora_inicio between 12 and 17 then 1 end) as eventos_tarde,
    count(case when hora_inicio between 18 and 23 then 1 end) as eventos_noite,
    count(case when hora_inicio between 0 and 5 then 1 end) as eventos_madrugada,
    
    -- Por tipo de evento
    count(case when descricao_tipo like '%Comissão%' then 1 end) as eventos_comissao,
    count(case when descricao_tipo like '%Plenário%' then 1 end) as eventos_plenario,
    count(case when descricao_tipo like '%Audiência%' then 1 end) as eventos_audiencia,
    
    -- Por local mais usado
    
    -- Percentuais
    round(count(case when eh_encerrado then 1 end) * 100.0 / count(*), 2) as perc_eventos_encerrados,
    round(count(case when eh_evento_externo then 1 end) * 100.0 / count(*), 2) as perc_eventos_externos,
    
    -- Metadata
    current_timestamp() as data_calculo
    
  from fct_eventos
  group by ano_evento, mes_evento
)

select * from eventos_resumo
order by ano_evento desc, mes_evento desc
