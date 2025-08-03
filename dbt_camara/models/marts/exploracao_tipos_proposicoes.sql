{{ config(materialized='view') }}

-- Análise exploratória: Tipos de proposições mais comuns
SELECT 
    sigla_tipo as tipo_proposicao,
    descricao_tipo,
    COUNT(*) as total_proposicoes,
    COUNT(DISTINCT numero) as numeros_distintos,
    MIN(ano) as ano_mais_antigo,
    MAX(ano) as ano_mais_recente,
    COUNT(CASE WHEN status_descricao ILIKE '%aprovad%' THEN 1 END) as proposicoes_aprovadas,
    COUNT(CASE WHEN status_descricao ILIKE '%arquivad%' THEN 1 END) as proposicoes_arquivadas,
    ROUND(COUNT(CASE WHEN status_descricao ILIKE '%aprovad%' THEN 1 END)::FLOAT / COUNT(*) * 100, 2) as taxa_aprovacao_percent
FROM {{ source('camara_raw', 'proposicoes') }}
WHERE sigla_tipo IS NOT NULL
GROUP BY sigla_tipo, descricao_tipo
ORDER BY total_proposicoes DESC
