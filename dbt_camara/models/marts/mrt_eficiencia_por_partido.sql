{{
  config(
    materialized='table',
    unique_key='sk_eficiencia_partido'
  )
}}

-- Mart Eficiência por Partido: Análise comparativa de performance entre partidos
WITH dados_por_partido AS (
    -- Agregação de gastos por partido
    SELECT 
        dd.sigla_partido,
        COUNT(DISTINCT dd.nk_deputado) AS total_deputados,
        
        -- Gastos
        COALESCE(SUM(fd.valor_total_liquido), 0) AS gastos_totais_partido,
        COALESCE(ROUND(AVG(fd.valor_total_liquido), 2), 0) AS gastos_medio_por_deputado,
        COALESCE(ROUND(AVG(fd.percentual_uso_cota_mensal), 2), 0) AS uso_medio_cota_percentual,
        
        -- Produtividade Legislativa
        COALESCE(SUM(pl.total_proposicoes), 0) AS proposicoes_totais_partido,
        COALESCE(SUM(pl.proposicoes_aprovadas), 0) AS proposicoes_aprovadas_partido,
        COALESCE(ROUND(AVG(pl.total_proposicoes), 2), 0) AS proposicoes_media_por_deputado,
        COALESCE(ROUND(AVG(pl.taxa_aprovacao_percentual), 2), 0) AS taxa_aprovacao_media_partido,
        
        -- Presença e Fidelidade
        COALESCE(ROUND(AVG(ps.taxa_presenca_sessoes), 2), 0) AS taxa_presenca_media_partido,
        COALESCE(ROUND(AVG(ps.taxa_fidelidade_partidaria), 2), 0) AS taxa_fidelidade_media_partido,
        COALESCE(ROUND(AVG(ps.taxa_rebeldia_partidaria), 2), 0) AS taxa_rebeldia_media_partido,
        
        -- Fidelidade consolidada do partido
        COALESCE(fp.taxa_fidelidade_partidaria, 0) AS fidelidade_partidaria_consolidada,
        COALESCE(fp.coesao_media_partido, 0) AS coesao_interna_partido,
        COALESCE(fp.classificacao_disciplina, 'Sem Dados') AS classificacao_disciplina_partido,
        COALESCE(fp.classificacao_coesao, 'Sem Dados') AS classificacao_coesao_partido,
        
        -- Especialização por tipo de proposição
        COALESCE(SUM(pl.projetos_lei), 0) AS total_projetos_lei_partido,
        COALESCE(SUM(pl.propostas_emenda_constitucional), 0) AS total_pec_partido,
        COALESCE(SUM(pl.requerimentos), 0) AS total_requerimentos_partido
        
    FROM {{ ref('dim_deputados') }} dd
    LEFT JOIN {{ ref('mrt_gastos_por_deputado') }} fd 
        ON dd.sk_deputado = fd.sk_deputado
    LEFT JOIN {{ ref('mrt_produtividade_legislativa') }} pl 
        ON dd.sk_deputado = pl.sk_deputado
    LEFT JOIN {{ ref('mrt_presenca_sessoes') }} ps 
        ON dd.sk_deputado = ps.sk_deputado
    LEFT JOIN {{ ref('mrt_fidelidade_partidaria') }} fp 
        ON dd.sigla_partido = fp.sigla_partido
    WHERE dd.is_current = TRUE  -- Apenas registros atuais do SCD
    GROUP BY dd.sigla_partido, fp.taxa_fidelidade_partidaria, fp.coesao_media_partido, 
             fp.classificacao_disciplina, fp.classificacao_coesao
),

rankings AS (
    SELECT 
        *,
        -- Rankings de gastos (menor é melhor)
        ROW_NUMBER() OVER (ORDER BY gastos_medio_por_deputado ASC) AS ranking_economia_gastos,
        ROW_NUMBER() OVER (ORDER BY uso_medio_cota_percentual ASC) AS ranking_economia_cota,
        
        -- Rankings de produtividade (maior é melhor)  
        ROW_NUMBER() OVER (ORDER BY proposicoes_media_por_deputado DESC) AS ranking_produtividade,
        ROW_NUMBER() OVER (ORDER BY taxa_aprovacao_media_partido DESC) AS ranking_efetividade,
        
        -- Rankings de presença e fidelidade (maior é melhor)
        ROW_NUMBER() OVER (ORDER BY taxa_presenca_media_partido DESC) AS ranking_presenca,
        ROW_NUMBER() OVER (ORDER BY fidelidade_partidaria_consolidada DESC) AS ranking_fidelidade,
        ROW_NUMBER() OVER (ORDER BY coesao_interna_partido DESC) AS ranking_coesao,
        
        -- Percentis para classificações
        PERCENT_RANK() OVER (ORDER BY gastos_medio_por_deputado) AS percentil_gastos,
        PERCENT_RANK() OVER (ORDER BY proposicoes_media_por_deputado) AS percentil_produtividade,
        PERCENT_RANK() OVER (ORDER BY taxa_presenca_media_partido) AS percentil_presenca,
        PERCENT_RANK() OVER (ORDER BY fidelidade_partidaria_consolidada) AS percentil_fidelidade
        
    FROM dados_por_partido
    WHERE total_deputados >= 3  -- Apenas partidos com pelo menos 3 deputados
),

benchmarks AS (
    SELECT 
        ROUND(AVG(gastos_medio_por_deputado), 2) AS benchmark_gastos_medio,
        ROUND(AVG(uso_medio_cota_percentual), 2) AS benchmark_uso_cota,
        ROUND(AVG(proposicoes_media_por_deputado), 2) AS benchmark_produtividade,
        ROUND(AVG(taxa_aprovacao_media_partido), 2) AS benchmark_efetividade,
        ROUND(AVG(taxa_presenca_media_partido), 2) AS benchmark_presenca,
        ROUND(AVG(fidelidade_partidaria_consolidada), 2) AS benchmark_fidelidade,
        ROUND(AVG(coesao_interna_partido), 2) AS benchmark_coesao
    FROM rankings
),

final AS (
    SELECT 
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'r.sigla_partido'
        ]) }} AS sk_eficiencia_partido,
        
        -- Identificação
        r.sigla_partido,
        r.total_deputados,
        
        -- Métricas de gastos
        r.gastos_totais_partido,
        r.gastos_medio_por_deputado,
        r.uso_medio_cota_percentual,
        
        -- Métricas de produtividade
        r.proposicoes_totais_partido,
        r.proposicoes_aprovadas_partido,
        r.proposicoes_media_por_deputado,
        r.taxa_aprovacao_media_partido,
        
        -- Métricas de presença e fidelidade
        r.taxa_presenca_media_partido,
        r.taxa_fidelidade_media_partido,
        r.taxa_rebeldia_media_partido,
        r.fidelidade_partidaria_consolidada,
        r.coesao_interna_partido,
        r.classificacao_disciplina_partido,
        r.classificacao_coesao_partido,
        
        -- Especialização
        r.total_projetos_lei_partido,
        r.total_pec_partido,
        r.total_requerimentos_partido,
        
        -- Eficiência relativa (proposições aprovadas por real gasto)
        CASE 
            WHEN r.gastos_totais_partido > 0 
            THEN ROUND(r.proposicoes_aprovadas_partido::FLOAT / (r.gastos_totais_partido / 1000000), 4)
            ELSE 0 
        END AS proposicoes_aprovadas_por_milhao_gasto,
        
        -- Rankings
        r.ranking_economia_gastos,
        r.ranking_economia_cota,
        r.ranking_produtividade,
        r.ranking_efetividade,
        r.ranking_presenca,
        r.ranking_fidelidade,
        r.ranking_coesao,
        
        -- Score composto de eficiência (0-100, maior é melhor)
        ROUND(
            ((1 - r.percentil_gastos) * 20) +     -- 20% para economia
            (r.percentil_produtividade * 25) +    -- 25% para produtividade
            (r.percentil_presenca * 20) +         -- 20% para presença
            (r.percentil_fidelidade * 15) +       -- 15% para fidelidade
            (r.taxa_aprovacao_media_partido / 100 * 20), -- 20% para efetividade
            2
        ) AS score_eficiencia_geral,
        
        -- Comparações com benchmarks
        CASE 
            WHEN r.gastos_medio_por_deputado < b.benchmark_gastos_medio THEN 'Abaixo da Média'
            ELSE 'Acima da Média'
        END AS comparacao_gastos,
        
        CASE 
            WHEN r.proposicoes_media_por_deputado > b.benchmark_produtividade THEN 'Acima da Média'
            ELSE 'Abaixo da Média'
        END AS comparacao_produtividade,
        
        CASE 
            WHEN r.taxa_presenca_media_partido > b.benchmark_presenca THEN 'Acima da Média'
            ELSE 'Abaixo da Média'
        END AS comparacao_presenca,
        
        CASE 
            WHEN r.fidelidade_partidaria_consolidada > b.benchmark_fidelidade THEN 'Acima da Média'
            ELSE 'Abaixo da Média'
        END AS comparacao_fidelidade,
        
        -- Classificações qualitativas
        CASE 
            WHEN r.percentil_gastos <= 0.2 THEN 'Muito Econômico'
            WHEN r.percentil_gastos <= 0.4 THEN 'Econômico'
            WHEN r.percentil_gastos <= 0.6 THEN 'Moderado'
            WHEN r.percentil_gastos <= 0.8 THEN 'Alto Gasto'
            ELSE 'Muito Alto Gasto'
        END AS classificacao_gastos,
        
        CASE 
            WHEN r.percentil_produtividade >= 0.8 THEN 'Muito Produtivo'
            WHEN r.percentil_produtividade >= 0.6 THEN 'Produtivo'
            WHEN r.percentil_produtividade >= 0.4 THEN 'Moderado'
            WHEN r.percentil_produtividade >= 0.2 THEN 'Baixa Produtividade'
            ELSE 'Muito Baixa Produtividade'
        END AS classificacao_produtividade,
        
        CASE 
            WHEN r.percentil_presenca >= 0.8 THEN 'Excelente Presença'
            WHEN r.percentil_presenca >= 0.6 THEN 'Boa Presença'
            WHEN r.percentil_presenca >= 0.4 THEN 'Presença Regular'
            WHEN r.percentil_presenca >= 0.2 THEN 'Baixa Presença'
            ELSE 'Muito Baixa Presença'
        END AS classificacao_presenca,
        
        CASE 
            WHEN r.percentil_fidelidade >= 0.8 THEN 'Muito Fiel'
            WHEN r.percentil_fidelidade >= 0.6 THEN 'Fiel'
            WHEN r.percentil_fidelidade >= 0.4 THEN 'Moderadamente Fiel'
            WHEN r.percentil_fidelidade >= 0.2 THEN 'Pouco Fiel'
            ELSE 'Rebelde'
        END AS classificacao_fidelidade_comparativa,
        
        -- Perfil do partido
        CASE 
            WHEN r.total_projetos_lei_partido >= r.proposicoes_totais_partido * 0.6 THEN 'Focado em Projetos de Lei'
            WHEN r.total_pec_partido >= r.proposicoes_totais_partido * 0.3 THEN 'Focado em PECs'
            WHEN r.total_requerimentos_partido >= r.proposicoes_totais_partido * 0.5 THEN 'Focado em Fiscalização'
            ELSE 'Atuação Diversificada'
        END AS perfil_atuacao,
        
        -- Flags de destaque
        CASE WHEN r.ranking_economia_gastos <= 5 THEN TRUE ELSE FALSE END AS flag_top5_economia,
        CASE WHEN r.ranking_produtividade <= 5 THEN TRUE ELSE FALSE END AS flag_top5_produtividade,
        CASE WHEN r.ranking_presenca <= 5 THEN TRUE ELSE FALSE END AS flag_top5_presenca,
        CASE WHEN r.ranking_efetividade <= 5 THEN TRUE ELSE FALSE END AS flag_top5_efetividade,
        CASE WHEN r.ranking_fidelidade <= 5 THEN TRUE ELSE FALSE END AS flag_top5_fidelidade,
        CASE WHEN r.ranking_coesao <= 5 THEN TRUE ELSE FALSE END AS flag_top5_coesao,
        
        -- Metadados
        b.benchmark_gastos_medio,
        b.benchmark_produtividade,
        b.benchmark_presenca,
        b.benchmark_efetividade,
        b.benchmark_fidelidade,
        b.benchmark_coesao,
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM rankings r
    CROSS JOIN benchmarks b
)

SELECT * FROM final
ORDER BY score_eficiencia_geral DESC
