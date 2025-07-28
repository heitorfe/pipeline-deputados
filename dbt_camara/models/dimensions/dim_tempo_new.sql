{{
  config(
    materialized='table'
  )
}}

with legislaturas_ref as (
  select 
    cast(idLegislatura as integer) as id_legislatura,
    try_to_date(dataInicio) as data_inicio,
    try_to_date(dataFim) as data_fim
  from {{ source('raw', 'legislaturas') }}
  where idLegislatura is not null
),

-- Gera uma sequência de datas mensais de 2000 até hoje
date_spine as (
  {{ dbt_utils.date_spine(
      datepart="month",
      start_date="cast('2000-01-01' as date)",
      end_date="current_date()"
   )
  }}
),

calendario_base as (
  select
    date_trunc('month', date_month) as data_mes,
    extract(year from date_month) as ano,
    extract(month from date_month) as mes,
    extract(quarter from date_month) as trimestre,
    case 
      when extract(month from date_month) in (1,2,3) then 'Q1'
      when extract(month from date_month) in (4,5,6) then 'Q2'
      when extract(month from date_month) in (7,8,9) then 'Q3'
      else 'Q4'
    end as trimestre_nome,
    case 
      when extract(month from date_month) <= 6 then 1
      else 2
    end as semestre,
    case 
      when extract(month from date_month) <= 6 then 'S1'
      else 'S2'
    end as semestre_nome,
    case extract(month from date_month)
      when 1 then 'Janeiro'
      when 2 then 'Fevereiro'
      when 3 then 'Março'
      when 4 then 'Abril'
      when 5 then 'Maio'
      when 6 then 'Junho'
      when 7 then 'Julho'
      when 8 then 'Agosto'
      when 9 then 'Setembro'
      when 10 then 'Outubro'
      when 11 then 'Novembro'
      when 12 then 'Dezembro'
    end as nome_mes
  from date_spine
),

calendario_com_legislatura as (
  select 
    c.*,
    l.id_legislatura,
    case 
      when c.data_mes between l.data_inicio and coalesce(l.data_fim, current_date()) 
      then true 
      else false 
    end as eh_legislatura_ativa,
    l.data_inicio as legislatura_inicio,
    l.data_fim as legislatura_fim
  from calendario_base c
  left join legislaturas_ref l 
    on c.data_mes between l.data_inicio and coalesce(l.data_fim, current_date())
),

final as (
  select
    -- Chave surrogate
    {{ dbt_utils.generate_surrogate_key(['ano', 'mes']) }} as sk_tempo,
    
    -- Campos temporais básicos
    data_mes,
    ano,
    mes,
    trimestre,
    trimestre_nome,
    semestre,
    semestre_nome,
    nome_mes,
    
    -- Informações de legislatura
    id_legislatura,
    eh_legislatura_ativa,
    legislatura_inicio,
    legislatura_fim,
    
    -- Campos calculados adicionais
    case when mes <= 6 then concat(ano, '-S1') else concat(ano, '-S2') end as ano_semestre,
    concat(ano, '-Q', trimestre) as ano_trimestre,
    concat(ano, '-', lpad(mes, 2, '0')) as ano_mes,
    
    -- Flags úteis para análise
    case when ano % 4 = 0 then true else false end as eh_ano_eleicao,
    case when mes in (7,8,12) then true else false end as eh_mes_recesso,
    case when data_mes >= '2000-01-01' then true else false end as eh_periodo_analise,
    
    -- Metadata
    current_timestamp() as data_carga
    
  from calendario_com_legislatura
  where id_legislatura is not null  -- Só períodos com legislatura definida
)

select * from final
order by ano, mes
