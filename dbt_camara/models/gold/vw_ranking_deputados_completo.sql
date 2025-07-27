{{ config(
    materialized='view'
) }}

SELECT 
    nome_deputado,
    sigla_partido,
    sigla_uf,
    ano,
    
    -- Métricas de despesas
    SUM(total_valor_liquido) AS total_gasto_ano,
    SUM(qtd_despesas) AS total_despesas_ano,
    AVG(media_valor_liquido) AS media_valor_despesa,
    
    -- Métricas de participação
    SUM(qtd_votos_total) AS total_votos_ano,
    SUM(qtd_votacoes_participou) AS total_votacoes_participou,
    AVG(percentual_participacao_votacoes) AS media_participacao_votacoes,
    
    -- Análise de comportamento de voto
    AVG(percentual_votos_sim) AS media_percentual_votos_sim,
    AVG(percentual_votos_nao) AS media_percentual_votos_nao,
    
    -- Rankings
    ROW_NUMBER() OVER (PARTITION BY ano ORDER BY SUM(total_valor_liquido) DESC) AS ranking_gasto_nacional,
    ROW_NUMBER() OVER (PARTITION BY ano, sigla_uf ORDER BY SUM(total_valor_liquido) DESC) AS ranking_gasto_uf,
    ROW_NUMBER() OVER (PARTITION BY ano ORDER BY AVG(percentual_participacao_votacoes) DESC) AS ranking_participacao_nacional,
    
    -- Flags de perfil
    CASE 
        WHEN AVG(percentual_participacao_votacoes) >= 80 THEN 'ALTA'
        WHEN AVG(percentual_participacao_votacoes) >= 50 THEN 'MÉDIA'
        ELSE 'BAIXA'
    END AS categoria_participacao,
    
    CASE 
        WHEN SUM(total_valor_liquido) >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY SUM(total_valor_liquido)) OVER (PARTITION BY ano) THEN 'ALTO'
        WHEN SUM(total_valor_liquido) >= PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SUM(total_valor_liquido)) OVER (PARTITION BY ano) THEN 'MÉDIO'
        ELSE 'BAIXO'
    END AS categoria_gasto

FROM {{ ref('obt_camara_completa') }}
WHERE teve_despesas = TRUE OR participou_votacoes = TRUE
GROUP BY nome_deputado, sigla_partido, sigla_uf, ano
ORDER BY ano DESC, total_gasto_ano DESC
