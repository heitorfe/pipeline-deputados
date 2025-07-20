{{ config(
    materialized='view'
) }}

SELECT 
    dd.nk_deputado,
    dd.nome_deputado,
    dd.sigla_partido,
    dd.sigla_uf,
    dt.ano,
    dt.nome_mes,
    dt.trimestre,
    COUNT(f.cod_documento) AS qtd_despesas,
    SUM(f.valor_documento) AS total_valor_documento,
    SUM(f.valor_liquido) AS total_valor_liquido,
    SUM(f.valor_glosa) AS total_valor_glosa,
    AVG(f.valor_liquido) AS media_valor_liquido
FROM {{ ref('fct_despesas') }} f
INNER JOIN {{ ref('dim_deputados') }} dd ON f.sk_deputado = dd.sk_deputado
INNER JOIN {{ ref('dim_tempo') }} dt ON f.sk_tempo = dt.sk_tempo
GROUP BY 
    dd.nk_deputado, dd.nome_deputado, dd.sigla_partido, dd.sigla_uf,
    dt.ano, dt.nome_mes, dt.trimestre
