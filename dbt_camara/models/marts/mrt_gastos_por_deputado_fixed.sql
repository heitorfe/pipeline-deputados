{{
  config(
    materialized='table',
    unique_key='sk_gastos_deputado'
  )
}}

-- Mart Gastos por Deputado: JOIN SCD Type 2 correto
-- JOIN pela chave natural + intervalo de vigência temporal
WITH gastos_com_deputados AS (
    SELECT 
        fd.*,
        dd.sk_deputado,
        dd.nome_deputado,
        dd.nome_civil,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.condicao_eleitoral,
        dd.situacao
    FROM {{ ref('fct_despesas') }} fd
    LEFT JOIN {{ ref('dim_deputados') }} dd
        ON fd.nk_deputado = dd.nk_deputado
        AND fd.data_documento BETWEEN dd.data_inicio_vigencia 
        AND COALESCE(dd.data_fim_vigencia, CURRENT_DATE())
    WHERE fd.valor_documento > 0
),

gastos_deputado AS (
    SELECT 
        -- Usar sk_deputado quando disponível, caso contrário criar um placeholder
        COALESCE(sk_deputado, 'DEPUTADO_NAO_ENCONTRADO_' || nk_deputado) AS sk_deputado,
        nk_deputado AS deputado_id,
        COALESCE(nome_deputado, 'Deputado ID: ' || nk_deputado) AS nome_deputado,
        COALESCE(nome_civil, 'N/A') AS nome_civil,
        COALESCE(sigla_partido, 'N/A') AS sigla_partido,
        COALESCE(sigla_uf, 'N/A') AS sigla_uf,
        COALESCE(condicao_eleitoral, 'N/A') AS condicao_eleitoral,
        COALESCE(situacao, 'N/A') AS situacao,
        
        -- Agregações por deputado (todos os períodos)
        COUNT(DISTINCT cod_documento) AS total_documentos,
        COUNT(DISTINCT CONCAT(ano, '-', LPAD(mes::STRING, 2, '0'))) AS total_meses_atividade,
        
        -- Valores totais
        SUM(valor_documento) AS valor_total_gasto,
        SUM(valor_liquido) AS valor_total_liquido,
        SUM(valor_glosa) AS valor_total_glosa,
        
        -- Médias mensais
        ROUND(AVG(valor_documento), 2) AS valor_medio_mensal,
        ROUND(AVG(valor_liquido), 2) AS valor_liquido_medio_mensal,
        
        -- Percentuais
        ROUND(SUM(valor_glosa) / NULLIF(SUM(valor_documento), 0) * 100, 2) AS percentual_glosa,
        
        -- Por faixa de valor
        ROUND(SUM(CASE WHEN faixa_valor = 'ALTO' THEN valor_documento ELSE 0 END), 2) AS gastos_valor_alto,
        ROUND(SUM(CASE WHEN faixa_valor = 'MEDIO' THEN valor_documento ELSE 0 END), 2) AS gastos_valor_medio,
        ROUND(SUM(CASE WHEN faixa_valor = 'BAIXO' THEN valor_documento ELSE 0 END), 2) AS gastos_valor_baixo,
        ROUND(SUM(CASE WHEN faixa_valor = 'MUITO_ALTO' THEN valor_documento ELSE 0 END), 2) AS gastos_valor_muito_alto,
        
        -- Análise temporal
        MIN(data_documento) AS primeira_despesa,
        MAX(data_documento) AS ultima_despesa,
        
        -- Análise da cota parlamentar
        ROUND(SUM(valor_liquido) / (30416.83 * COUNT(DISTINCT CONCAT(ano, '-', LPAD(mes::STRING, 2, '0')))) * 100, 2) AS percentual_uso_cota_mensal
        
    FROM gastos_com_deputados
    GROUP BY 
        COALESCE(sk_deputado, 'DEPUTADO_NAO_ENCONTRADO_' || nk_deputado),
        nk_deputado,
        COALESCE(nome_deputado, 'Deputado ID: ' || nk_deputado),
        COALESCE(nome_civil, 'N/A'),
        COALESCE(sigla_partido, 'N/A'),
        COALESCE(sigla_uf, 'N/A'),
        COALESCE(condicao_eleitoral, 'N/A'),
        COALESCE(situacao, 'N/A')
)

SELECT 
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'sk_deputado'
    ]) }} AS sk_gastos_deputado,
    
    -- Informações do deputado
    sk_deputado,
    deputado_id,
    nome_deputado,
    nome_civil,
    sigla_partido,
    sigla_uf,
    condicao_eleitoral,
    situacao,
    
    -- Métricas de gastos
    total_documentos,
    total_meses_atividade,
    valor_total_gasto,
    valor_total_liquido,
    valor_total_glosa,
    valor_medio_mensal,
    valor_liquido_medio_mensal,
    percentual_glosa,
    
    -- Distribuição por valor
    gastos_valor_alto,
    gastos_valor_medio,
    gastos_valor_baixo,
    gastos_valor_muito_alto,
    
    -- Análise temporal
    primeira_despesa,
    ultima_despesa,
    
    -- Uso da cota
    percentual_uso_cota_mensal,
    
    -- Classificações
    CASE 
        WHEN percentual_uso_cota_mensal >= 90 THEN 'Muito Alto'
        WHEN percentual_uso_cota_mensal >= 70 THEN 'Alto'
        WHEN percentual_uso_cota_mensal >= 50 THEN 'Médio'
        WHEN percentual_uso_cota_mensal >= 30 THEN 'Baixo'
        ELSE 'Muito Baixo'
    END AS classificacao_uso_cota,
    
    -- Flags de eficiência
    CASE WHEN percentual_glosa < 5 THEN TRUE ELSE FALSE END AS flag_baixa_glosa,
    CASE WHEN percentual_uso_cota_mensal < 80 THEN TRUE ELSE FALSE END AS flag_uso_eficiente,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS data_processamento
    
FROM gastos_deputado
ORDER BY valor_total_liquido DESC
