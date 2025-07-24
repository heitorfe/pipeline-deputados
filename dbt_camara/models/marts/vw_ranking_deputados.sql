{{ config(
    materialized='view'
) }}

SELECT 
    dd.nome_deputado,
    dd.sigla_partido,
    dd.sigla_uf,
    dt.ano,
    SUM(f.valor_liquido) AS total_gasto,
    COUNT(f.cod_documento) AS total_despesas,
    ROW_NUMBER() OVER (PARTITION BY dt.ano ORDER BY SUM(f.valor_liquido) DESC) AS ranking_gasto_ano,
    ROW_NUMBER() OVER (PARTITION BY dt.ano, dd.sigla_uf ORDER BY SUM(f.valor_liquido) DESC) AS ranking_gasto_uf,
    COUNT(DISTINCT dd.sk_deputado) AS qtd_periodos_historicos
FROM {{ ref('fct_despesas') }} f
INNER JOIN {{ ref('dim_deputados') }} dd ON f.sk_deputado = dd.sk_deputado
INNER JOIN {{ ref('dim_tempo') }} dt ON f.sk_tempo = dt.sk_tempo
GROUP BY 
    dd.nome_deputado, dd.sigla_partido, dd.sigla_uf, dt.ano
