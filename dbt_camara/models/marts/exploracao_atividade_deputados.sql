{{ config(materialized='view') }}

-- Análise exploratória: Análise de votações e efetividade dos deputados
WITH votacoes_por_deputado AS (
    SELECT 
        nome_deputado,
        COUNT(*) as total_votacoes,
        COUNT(CASE WHEN voto = 'Sim' THEN 1 END) as votos_sim,
        COUNT(CASE WHEN voto = 'Não' THEN 1 END) as votos_nao,
        COUNT(CASE WHEN voto = 'Abstenção' THEN 1 END) as abstencoes,
        COUNT(CASE WHEN voto IS NULL OR voto = '' THEN 1 END) as ausencias,
        COUNT(DISTINCT data_votacao) as dias_votacao_distintos
    FROM {{ source('camara_raw', 'votos') }}
    WHERE nome_deputado IS NOT NULL
    GROUP BY nome_deputado
),
producao_legislativa AS (
    SELECT 
        nome_autor,
        COUNT(*) as proposicoes_criadas,
        COUNT(CASE WHEN status_descricao ILIKE '%aprovad%' THEN 1 END) as proposicoes_aprovadas,
        COUNT(DISTINCT sigla_tipo) as tipos_proposicao_criados
    FROM {{ source('camara_raw', 'proposicoes_autores') }} pa
    JOIN {{ source('camara_raw', 'proposicoes') }} p 
        ON pa.id_proposicao = p.id
    GROUP BY nome_autor
)
SELECT 
    COALESCE(v.nome_deputado, p.nome_autor) as nome_deputado,
    COALESCE(v.total_votacoes, 0) as total_votacoes,
    COALESCE(v.votos_sim, 0) as votos_sim,
    COALESCE(v.votos_nao, 0) as votos_nao,
    COALESCE(v.abstencoes, 0) as abstencoes,
    COALESCE(v.ausencias, 0) as ausencias,
    COALESCE(p.proposicoes_criadas, 0) as proposicoes_criadas,
    COALESCE(p.proposicoes_aprovadas, 0) as proposicoes_aprovadas,
    COALESCE(p.tipos_proposicao_criados, 0) as tipos_proposicao_criados,
    
    -- Métricas de participação
    CASE 
        WHEN v.total_votacoes > 0 THEN ROUND(v.ausencias::FLOAT / v.total_votacoes * 100, 2)
        ELSE 0 
    END as taxa_ausencia_percent,
    
    -- Métricas de efetividade legislativa
    CASE 
        WHEN p.proposicoes_criadas > 0 THEN ROUND(p.proposicoes_aprovadas::FLOAT / p.proposicoes_criadas * 100, 2)
        ELSE 0 
    END as taxa_aprovacao_proposicoes,
    
    -- Score de atividade parlamentar
    (COALESCE(v.total_votacoes, 0) * 0.3 + COALESCE(p.proposicoes_criadas, 0) * 0.7) as score_atividade,
    
    -- Classificação do deputado
    CASE 
        WHEN COALESCE(v.total_votacoes, 0) > 1000 AND COALESCE(p.proposicoes_criadas, 0) > 10 THEN 'MUITO_ATIVO'
        WHEN COALESCE(v.total_votacoes, 0) > 500 AND COALESCE(p.proposicoes_criadas, 0) > 5 THEN 'ATIVO'
        WHEN COALESCE(v.total_votacoes, 0) > 100 OR COALESCE(p.proposicoes_criadas, 0) > 1 THEN 'MODERADO'
        ELSE 'BAIXA_ATIVIDADE'
    END as categoria_atividade

FROM votacoes_por_deputado v
FULL OUTER JOIN producao_legislativa p ON v.nome_deputado = p.nome_autor
WHERE COALESCE(v.total_votacoes, 0) > 0 OR COALESCE(p.proposicoes_criadas, 0) > 0
ORDER BY score_atividade DESC
