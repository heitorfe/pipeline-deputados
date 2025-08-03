{{
  config(
    materialized='table',
    unique_key='sk_gastos_deputado'
  )
}}

-- Mart Gastos por Deputado: Análise de gastos e uso da cota parlamentar
-- JOIN corrigido para usar nk_deputado da fct_despesas com dim_deputados
WITH gastos_deputado AS (
    SELECT 
        dd.sk_deputado,
        fd.nk_deputado AS deputado_id,
        dd.nome_deputado,
        dd.nome_civil,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.condicao_eleitoral,
        dd.situacao,
        
        -- Agregações por deputado (todos os períodos)
        COUNT(DISTINCT fd.cod_documento) AS total_documentos,
        COUNT(DISTINCT CONCAT(fd.ano, '-', LPAD(fd.mes::STRING, 2, '0'))) AS total_meses_atividade,
        
        -- Valores totais
        SUM(fd.valor_documento) AS valor_total_gasto,
        SUM(fd.valor_liquido) AS valor_total_liquido,
        SUM(fd.valor_glosa) AS valor_total_glosa,
        
        -- Médias mensais
        ROUND(AVG(fd.valor_documento), 2) AS valor_medio_mensal,
        ROUND(AVG(fd.valor_liquido), 2) AS valor_liquido_medio_mensal,
        
        -- Percentuais
        ROUND(SUM(fd.valor_glosa) / NULLIF(SUM(fd.valor_documento), 0) * 100, 2) AS percentual_glosa,
        
        -- Por faixa de valor (corrigindo nomes das categorias)
        ROUND(SUM(CASE WHEN fd.faixa_valor = 'ALTO' THEN fd.valor_documento ELSE 0 END), 2) AS gastos_valor_alto,
        ROUND(SUM(CASE WHEN fd.faixa_valor = 'MEDIO' THEN fd.valor_documento ELSE 0 END), 2) AS gastos_valor_medio,
        ROUND(SUM(CASE WHEN fd.faixa_valor = 'BAIXO' THEN fd.valor_documento ELSE 0 END), 2) AS gastos_valor_baixo,
        ROUND(SUM(CASE WHEN fd.faixa_valor = 'MUITO_ALTO' THEN fd.valor_documento ELSE 0 END), 2) AS gastos_valor_muito_alto,
        
        -- Análise temporal
        MIN(fd.data_documento) AS primeira_despesa,
        MAX(fd.data_documento) AS ultima_despesa,
        
        -- Análise da cota parlamentar (assumindo cota de R$ 30.416,83/mês em 2023)
        ROUND(SUM(fd.valor_liquido) / (30416.83 * COUNT(DISTINCT CONCAT(fd.ano, '-', LPAD(fd.mes::STRING, 2, '0')))) * 100, 2) AS percentual_uso_cota_mensal
        
    FROM {{ ref('fct_despesas') }} fd
    LEFT JOIN {{ ref('dim_deputados') }} dd 
        ON fd.nk_deputado = dd.nk_deputado
        AND fd.data_documento between dd.data_inicio_vigencia and dd.data_fim_vigencia
        -- AND dd.is_current = TRUE  -- Pegar apenas o registro atual do deputado
    WHERE fd.valor_documento > 0
      AND dd.sk_deputado IS NOT NULL  -- Garantir que encontramos o deputado na dimensão
    GROUP BY 
        dd.sk_deputado,
        fd.nk_deputado,
        dd.nome_deputado,
        dd.nome_civil,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.condicao_eleitoral,
        dd.situacao
),

gastos_por_uf AS (
    SELECT 
        sigla_uf,
        COUNT(*) AS total_deputados_uf,
        ROUND(AVG(valor_total_liquido), 2) AS media_gastos_uf,
        ROUND(AVG(percentual_uso_cota_mensal), 2) AS media_uso_cota_uf
    FROM gastos_deputado
    GROUP BY sigla_uf
),

gastos_por_partido AS (
    SELECT 
        sigla_partido,
        COUNT(*) AS total_deputados_partido,
        ROUND(AVG(valor_total_liquido), 2) AS media_gastos_partido,
        ROUND(AVG(percentual_uso_cota_mensal), 2) AS media_uso_cota_partido
    FROM gastos_deputado
    GROUP BY sigla_partido
),

final AS (
    SELECT 
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'gd.sk_deputado'
        ]) }} AS sk_gastos_deputado,
        
        -- Informações do deputado
        gd.sk_deputado,
        gd.deputado_id,
        gd.nome_deputado,
        gd.nome_civil,
        gd.sigla_partido,
        gd.sigla_uf,
        gd.condicao_eleitoral,
        gd.situacao,
        
        -- Métricas de gastos
        gd.total_documentos,
        gd.total_meses_atividade,
        gd.valor_total_gasto,
        gd.valor_total_liquido,
        gd.valor_total_glosa,
        gd.valor_medio_mensal,
        gd.valor_liquido_medio_mensal,
        gd.percentual_glosa,
        
        -- Distribuição por valor
        gd.gastos_valor_alto,
        gd.gastos_valor_medio,
        gd.gastos_valor_baixo,
        gd.gastos_valor_muito_alto,
        
        -- Análise temporal
        gd.primeira_despesa,
        gd.ultima_despesa,
        
        -- Uso da cota
        gd.percentual_uso_cota_mensal,
        
        -- Comparações
        uf.media_gastos_uf,
        uf.media_uso_cota_uf,
        pt.media_gastos_partido,
        pt.media_uso_cota_partido,
        
        -- Rankings e classificações
        CASE 
            WHEN gd.percentual_uso_cota_mensal >= 90 THEN 'Muito Alto'
            WHEN gd.percentual_uso_cota_mensal >= 70 THEN 'Alto'
            WHEN gd.percentual_uso_cota_mensal >= 50 THEN 'Médio'
            WHEN gd.percentual_uso_cota_mensal >= 30 THEN 'Baixo'
            ELSE 'Muito Baixo'
        END AS classificacao_uso_cota,
        
        CASE 
            WHEN gd.valor_total_liquido > uf.media_gastos_uf THEN 'Acima da Média UF'
            ELSE 'Abaixo da Média UF'
        END AS comparacao_uf,
        
        CASE 
            WHEN gd.valor_total_liquido > pt.media_gastos_partido THEN 'Acima da Média Partido'
            ELSE 'Abaixo da Média Partido'
        END AS comparacao_partido,
        
        -- Flags de eficiência
        CASE WHEN gd.percentual_glosa < 5 THEN TRUE ELSE FALSE END AS flag_baixa_glosa,
        CASE WHEN gd.percentual_uso_cota_mensal < 80 THEN TRUE ELSE FALSE END AS flag_uso_eficiente,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM gastos_deputado gd
    LEFT JOIN gastos_por_uf uf ON gd.sigla_uf = uf.sigla_uf
    LEFT JOIN gastos_por_partido pt ON gd.sigla_partido = pt.sigla_partido
)

SELECT * FROM final
ORDER BY valor_total_liquido DESC
