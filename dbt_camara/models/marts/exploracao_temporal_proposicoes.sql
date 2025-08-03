{{ config(materialized='view') }}

-- Análise exploratória: Análise temporal das proposições
WITH proposicoes_por_ano AS (
    SELECT 
        ano,
        COUNT(*) as total_proposicoes,
        COUNT(DISTINCT sigla_tipo) as tipos_distintos,
        COUNT(CASE WHEN status_descricao ILIKE '%aprovad%' THEN 1 END) as aprovadas,
        COUNT(CASE WHEN status_descricao ILIKE '%arquivad%' THEN 1 END) as arquivadas,
        COUNT(CASE WHEN status_descricao ILIKE '%tramitand%' THEN 1 END) as em_tramitacao
    FROM {{ source('camara_raw', 'proposicoes') }}
    WHERE ano IS NOT NULL
    GROUP BY ano
),
tendencias AS (
    SELECT 
        *,
        LAG(total_proposicoes) OVER (ORDER BY ano) as proposicoes_ano_anterior,
        ROUND((total_proposicoes - LAG(total_proposicoes) OVER (ORDER BY ano))::FLOAT / 
              NULLIF(LAG(total_proposicoes) OVER (ORDER BY ano), 0) * 100, 2) as crescimento_percentual
    FROM proposicoes_por_ano
)
SELECT 
    ano,
    total_proposicoes,
    tipos_distintos,
    aprovadas,
    arquivadas,
    em_tramitacao,
    ROUND(aprovadas::FLOAT / total_proposicoes * 100, 2) as taxa_aprovacao,
    crescimento_percentual,
    CASE 
        WHEN crescimento_percentual > 10 THEN 'ALTO_CRESCIMENTO'
        WHEN crescimento_percentual BETWEEN 0 AND 10 THEN 'CRESCIMENTO_MODERADO'
        WHEN crescimento_percentual BETWEEN -10 AND 0 THEN 'DECLINIO_MODERADO'
        ELSE 'ALTO_DECLINIO'
    END as categoria_tendencia
FROM tendencias
ORDER BY ano DESC
