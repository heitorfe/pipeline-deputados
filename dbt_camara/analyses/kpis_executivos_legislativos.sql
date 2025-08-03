-- Análise Executiva: Principais KPIs do Comportamento Legislativo
WITH kpis_gerais AS (
    SELECT 
        'Período Analisado' as metrica,
        MIN(ano_votacao)::STRING || ' - ' || MAX(ano_votacao)::STRING as valor,
        'Anos com dados de votação' as descricao
    FROM {{ ref('fct_votos_individuais') }}
    
    UNION ALL
    
    SELECT 
        'Total de Votos Individuais',
        COUNT(*)::STRING,
        'Votos registrados no sistema'
    FROM {{ ref('fct_votos_individuais') }}
    
    UNION ALL
    
    SELECT 
        'Deputados Únicos',
        COUNT(DISTINCT nk_deputado)::STRING,
        'Deputados com pelo menos 1 voto registrado'
    FROM {{ ref('fct_votos_individuais') }}
    
    UNION ALL
    
    SELECT 
        'Votações Únicas',
        COUNT(DISTINCT nk_votacao)::STRING,
        'Votações nominais realizadas'
    FROM {{ ref('fct_votos_individuais') }}
    
    UNION ALL
    
    SELECT 
        'Proposições Votadas',
        COUNT(DISTINCT nk_proposicao)::STRING,
        'Proposições que passaram por votação'
    FROM {{ ref('fct_votos_individuais') }}
    WHERE flag_tem_proposicao_vinculada = TRUE
),

kpis_comportamento AS (
    SELECT 
        'Taxa de Fidelidade Partidária',
        ROUND(COUNT(CASE WHEN fidelidade_partidaria = 'SEGUIU_ORIENTACAO' THEN 1 END) * 100.0 / 
              NULLIF(COUNT(CASE WHEN fidelidade_partidaria IN ('SEGUIU_ORIENTACAO', 'CONTRARIOU_ORIENTACAO') THEN 1 END), 0), 2)::STRING || '%',
        'Percentual de votos que seguiram orientação partidária'
    FROM {{ ref('fct_votos_individuais') }}
    
    UNION ALL
    
    SELECT 
        'Taxa de Aprovação Média',
        ROUND(COUNT(CASE WHEN flag_votacao_aprovada THEN 1 END) * 100.0 / COUNT(DISTINCT nk_votacao), 2)::STRING || '%',
        'Percentual de votações que foram aprovadas'
    FROM {{ ref('fct_votos_individuais') }}
    
    UNION ALL
    
    SELECT 
        'Votos SIM vs NÃO',
        ROUND(COUNT(CASE WHEN tipo_voto = 'SIM' THEN 1 END) * 100.0 / 
              NULLIF(COUNT(CASE WHEN tipo_voto IN ('SIM', 'NÃO', 'NAO') THEN 1 END), 0), 2)::STRING || '% SIM',
        'Proporção de votos favoráveis vs contrários'
    FROM {{ ref('fct_votos_individuais') }}
)

SELECT * FROM kpis_gerais
UNION ALL
SELECT * FROM kpis_comportamento
ORDER BY metrica
