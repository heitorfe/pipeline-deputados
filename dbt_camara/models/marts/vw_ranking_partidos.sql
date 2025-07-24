{{ config(
    materialized='view'
) }}

SELECT 
    dd.sigla_partido,
    dt.ano,
    dt.trimestre,
    COUNT(DISTINCT dd.nk_deputado) AS qtd_deputados,
    COUNT(f.cod_documento) AS total_despesas,
    SUM(f.valor_liquido) AS total_gasto_partido,
    AVG(f.valor_liquido) AS media_valor_despesa,
    SUM(f.valor_glosa) AS total_glosa_partido,
    -- Ranking nacional por ano
    ROW_NUMBER() OVER (PARTITION BY dt.ano ORDER BY SUM(f.valor_liquido) DESC) AS ranking_nacional_ano,
    -- Ranking por trimestre
    ROW_NUMBER() OVER (PARTITION BY dt.ano, dt.trimestre ORDER BY SUM(f.valor_liquido) DESC) AS ranking_trimestre,
    -- Percentual do total nacional
    ROUND(
        (SUM(f.valor_liquido) / SUM(SUM(f.valor_liquido)) OVER (PARTITION BY dt.ano)) * 100, 
        2
    ) AS percentual_gasto_nacional,
    -- Gasto médio por deputado do partido (considerando histórico)
    ROUND(
        SUM(f.valor_liquido) / COUNT(DISTINCT dd.nk_deputado), 
        2
    ) AS gasto_medio_por_deputado
FROM {{ ref('fct_despesas') }} f
INNER JOIN {{ ref('dim_deputados') }} dd ON f.sk_deputado = dd.sk_deputado
INNER JOIN {{ ref('dim_tempo') }} dt ON f.sk_tempo = dt.sk_tempo
WHERE dd.sigla_partido IS NOT NULL
GROUP BY 
    dd.sigla_partido, dt.ano, dt.trimestre
ORDER BY 
    dt.ano DESC, total_gasto_partido DESC
