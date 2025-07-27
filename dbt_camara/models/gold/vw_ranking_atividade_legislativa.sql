{{ config(
    materialized='view'
) }}

SELECT 
    sigla_partido,
    sigla_uf,
    ano,
    trimestre,
    
    -- Métricas de participação
    COUNT(DISTINCT deputado_id) AS qtd_deputados_ativos,
    AVG(percentual_participacao_votacoes) AS media_participacao_votacoes,
    
    -- Métricas de atividade legislativa
    AVG(qtd_tramitacoes_mes) AS media_tramitacoes_mes,
    AVG(qtd_proposicoes_tramitadas_mes) AS media_proposicoes_tramitadas,
    AVG(indice_eficiencia_legislativa) AS media_eficiencia_legislativa,
    
    -- Análise de produtividade
    SUM(qtd_votos_total) AS total_votos_partido,
    SUM(total_valor_liquido) AS total_gastos_partido,
    
    -- Métricas de velocidade
    AVG(media_dias_entre_tramitacoes) AS media_dias_tramitacao_partido,
    
    -- Rankings
    ROW_NUMBER() OVER (
        PARTITION BY ano 
        ORDER BY AVG(indice_eficiencia_legislativa) DESC
    ) AS ranking_eficiencia_nacional,
    
    ROW_NUMBER() OVER (
        PARTITION BY ano 
        ORDER BY AVG(percentual_participacao_votacoes) DESC
    ) AS ranking_participacao_nacional,
    
    -- Categorização
    CASE 
        WHEN AVG(indice_eficiencia_legislativa) >= 70 THEN 'ALTA EFICIÊNCIA'
        WHEN AVG(indice_eficiencia_legislativa) >= 40 THEN 'MÉDIA EFICIÊNCIA'
        ELSE 'BAIXA EFICIÊNCIA'
    END AS categoria_eficiencia_legislativa

FROM {{ ref('obt_camara_completa') }}
WHERE teve_atividade_legislativa = TRUE 
   OR participou_votacoes = TRUE
GROUP BY sigla_partido, sigla_uf, ano, trimestre
ORDER BY ano DESC, media_eficiencia_legislativa DESC
