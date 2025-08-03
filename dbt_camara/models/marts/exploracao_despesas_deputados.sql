{{ config(materialized='view') }}

-- Análise exploratória: Análise de despesas por deputado
WITH despesas_consolidadas AS (
    SELECT 
        nome_deputado,
        ano,
        COUNT(*) as total_despesas,
        SUM(valor_liquido) as valor_total_liquido,
        SUM(valor_documento) as valor_total_documento,
        COUNT(DISTINCT tipo_despesa) as tipos_despesa_distintos,
        COUNT(DISTINCT cnpj_cpf_fornecedor) as fornecedores_distintos,
        AVG(valor_liquido) as valor_medio_despesa
    FROM {{ source('camara_raw', 'despesas') }}
    WHERE nome_deputado IS NOT NULL 
      AND valor_liquido > 0
    GROUP BY nome_deputado, ano
),
ranking_deputados AS (
    SELECT 
        nome_deputado,
        SUM(valor_total_liquido) as total_gasto_periodo,
        AVG(valor_total_liquido) as media_anual,
        COUNT(DISTINCT ano) as anos_atividade,
        SUM(total_despesas) as total_transacoes,
        AVG(valor_medio_despesa) as valor_medio_geral
    FROM despesas_consolidadas
    GROUP BY nome_deputado
)
SELECT 
    nome_deputado,
    total_gasto_periodo,
    media_anual,
    anos_atividade,
    total_transacoes,
    valor_medio_geral,
    ROUND(total_gasto_periodo / anos_atividade, 2) as gasto_medio_por_ano,
    CASE 
        WHEN total_gasto_periodo > 1000000 THEN 'ALTO_GASTO'
        WHEN total_gasto_periodo > 500000 THEN 'MEDIO_GASTO'
        ELSE 'BAIXO_GASTO'
    END as categoria_gasto,
    ROW_NUMBER() OVER (ORDER BY total_gasto_periodo DESC) as ranking_gasto
FROM ranking_deputados
WHERE anos_atividade >= 2  -- Apenas deputados com pelo menos 2 anos de atividade
ORDER BY total_gasto_periodo DESC
