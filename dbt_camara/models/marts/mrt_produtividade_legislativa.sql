{{
  config(
    materialized='table',
    unique_key='sk_produtividade_deputado'
  )
}}

-- Mart Produtividade Legislativa: Análise de proposições e efetividade legislativa
WITH proposicoes_deputado AS (
    SELECT 
        spa.deputado_autor_id,
        dd.sk_deputado,
        dd.nk_deputado AS deputado_id,
        dd.nome_deputado,
        dd.nome_civil,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.condicao_eleitoral,
        dd.situacao,
        
        -- Contadores de proposições (apenas como proponente principal)
        COUNT(CASE WHEN spa.eh_proponente THEN 1 END) AS total_proposicoes,        COUNT(CASE WHEN spa.eh_proponente AND fp.status_final_classificado = 'APROVADA' THEN 1 END) AS proposicoes_aprovadas,
        COUNT(CASE WHEN spa.eh_proponente AND fp.status_final_classificado = 'REJEITADA' THEN 1 END) AS proposicoes_rejeitadas,
        COUNT(CASE WHEN spa.eh_proponente AND fp.status_final_classificado = 'EM_TRAMITACAO' THEN 1 END) AS proposicoes_em_tramitacao,
        COUNT(CASE WHEN spa.eh_proponente AND fp.status_final_classificado = 'ARQUIVADA' THEN 1 END) AS proposicoes_arquivadas,
        
        -- Por tipo de proposição
        COUNT(CASE WHEN spa.eh_proponente AND fp.sigla_tipo = 'PL' THEN 1 END) AS projetos_lei,
        COUNT(CASE WHEN spa.eh_proponente AND fp.sigla_tipo = 'PEC' THEN 1 END) AS propostas_emenda_constitucional,
        COUNT(CASE WHEN spa.eh_proponente AND fp.sigla_tipo = 'PDC' THEN 1 END) AS projetos_decreto_legislativo,
        COUNT(CASE WHEN spa.eh_proponente AND fp.sigla_tipo = 'PLP' THEN 1 END) AS projetos_lei_complementar,
        COUNT(CASE WHEN spa.eh_proponente AND fp.sigla_tipo = 'REQ' THEN 1 END) AS requerimentos,
        
        -- Análise de votações
        COUNT(CASE WHEN spa.eh_proponente AND fp.total_votacoes > 0 THEN 1 END) AS proposicoes_votadas,
        ROUND(AVG(CASE WHEN spa.eh_proponente THEN fp.total_votacoes END), 2) AS media_votacoes_por_proposicao,
        SUM(CASE WHEN spa.eh_proponente THEN fp.total_votacoes ELSE 0 END) AS total_votacoes_proposicoes,
        
        -- Análise temporal
        ROUND(AVG(CASE WHEN spa.eh_proponente THEN fp.dias_tramitacao END), 2) AS tempo_medio_tramitacao_dias,
        MIN(CASE WHEN spa.eh_proponente THEN fp.data_apresentacao END) AS primeira_proposicao,
        MAX(CASE WHEN spa.eh_proponente THEN fp.data_apresentacao END) AS ultima_proposicao,
        
        -- Por ano de apresentação
        COUNT(CASE WHEN spa.eh_proponente AND EXTRACT(YEAR FROM fp.data_apresentacao) = 2023 THEN 1 END) AS proposicoes_2023,
        COUNT(CASE WHEN spa.eh_proponente AND EXTRACT(YEAR FROM fp.data_apresentacao) = 2022 THEN 1 END) AS proposicoes_2022,
        COUNT(CASE WHEN spa.eh_proponente AND EXTRACT(YEAR FROM fp.data_apresentacao) = 2021 THEN 1 END) AS proposicoes_2021,
        
        -- Coautorias (total de proposições que participou, incluindo não sendo proponente)
        COUNT(*) AS total_participacoes_proposicoes,
        ROUND(AVG(fp.total_autores), 2) AS media_coautores
        
    FROM {{ ref('stg_proposicoes_autores') }} spa
    INNER JOIN {{ ref('fct_proposicoes') }} fp 
        ON spa.proposicao_id = fp.nk_proposicao
    INNER JOIN {{ ref('dim_deputados') }} dd 
        ON spa.deputado_autor_id = dd.nk_deputado
    WHERE spa.deputado_autor_id IS NOT NULL
    GROUP BY 
        spa.deputado_autor_id,
        dd.sk_deputado,
        dd.nk_deputado,
        dd.nome_deputado,
        dd.nome_civil,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.condicao_eleitoral,
        dd.situacao
),

produtividade_por_uf AS (
    SELECT 
        sigla_uf,
        COUNT(*) AS total_deputados_uf,
        ROUND(AVG(total_proposicoes), 2) AS media_proposicoes_uf,
        ROUND(AVG(CASE WHEN total_proposicoes > 0 THEN proposicoes_aprovadas::FLOAT / total_proposicoes * 100 ELSE 0 END), 2) AS taxa_aprovacao_media_uf
    FROM proposicoes_deputado
    GROUP BY sigla_uf
),

produtividade_por_partido AS (
    SELECT 
        sigla_partido,
        COUNT(*) AS total_deputados_partido,
        ROUND(AVG(total_proposicoes), 2) AS media_proposicoes_partido,
        ROUND(AVG(CASE WHEN total_proposicoes > 0 THEN proposicoes_aprovadas::FLOAT / total_proposicoes * 100 ELSE 0 END), 2) AS taxa_aprovacao_media_partido
    FROM proposicoes_deputado
    GROUP BY sigla_partido
),

final AS (
    SELECT 
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'pd.sk_deputado'
        ]) }} AS sk_produtividade_deputado,
        
        -- Informações do deputado
        pd.sk_deputado,
        pd.deputado_id,
        pd.nome_deputado,
        pd.nome_civil,
        pd.sigla_partido,
        pd.sigla_uf,
        pd.condicao_eleitoral,
        pd.situacao,
        
        -- Métricas de produtividade
        pd.total_proposicoes,
        pd.proposicoes_aprovadas,
        pd.proposicoes_rejeitadas,
        pd.proposicoes_em_tramitacao,
        pd.proposicoes_arquivadas,
        
        -- Taxa de sucesso (aprovação)
        CASE 
            WHEN pd.total_proposicoes > 0 
            THEN ROUND(pd.proposicoes_aprovadas::FLOAT / pd.total_proposicoes * 100, 2)
            ELSE 0 
        END AS taxa_aprovacao_percentual,
        
        -- Taxa de rejeição
        CASE 
            WHEN pd.total_proposicoes > 0 
            THEN ROUND(pd.proposicoes_rejeitadas::FLOAT / pd.total_proposicoes * 100, 2)
            ELSE 0 
        END AS taxa_rejeicao_percentual,
        
        -- Por tipo de proposição
        pd.projetos_lei,
        pd.propostas_emenda_constitucional,
        pd.projetos_decreto_legislativo,
        pd.projetos_lei_complementar,
        pd.requerimentos,
        
        -- Análise de votações
        pd.proposicoes_votadas,
        pd.media_votacoes_por_proposicao,
        pd.total_votacoes_proposicoes,
        
        -- Análise temporal
        pd.tempo_medio_tramitacao_dias,
        pd.primeira_proposicao,
        pd.ultima_proposicao,
        
        -- Tendência temporal
        pd.proposicoes_2023,
        pd.proposicoes_2022,
        pd.proposicoes_2021,
        
        -- Coautorias
        pd.media_coautores,
        
        -- Comparações
        uf.media_proposicoes_uf,
        uf.taxa_aprovacao_media_uf,
        pt.media_proposicoes_partido,
        pt.taxa_aprovacao_media_partido,
        
        -- Classificações de produtividade
        CASE 
            WHEN pd.total_proposicoes >= 50 THEN 'Muito Produtivo'
            WHEN pd.total_proposicoes >= 20 THEN 'Produtivo'
            WHEN pd.total_proposicoes >= 10 THEN 'Moderado'
            WHEN pd.total_proposicoes >= 5 THEN 'Baixo'
            ELSE 'Muito Baixo'
        END AS classificacao_produtividade,
        
        CASE 
            WHEN pd.total_proposicoes > 0 AND pd.proposicoes_aprovadas::FLOAT / pd.total_proposicoes >= 0.3 THEN 'Alta Efetividade'
            WHEN pd.total_proposicoes > 0 AND pd.proposicoes_aprovadas::FLOAT / pd.total_proposicoes >= 0.15 THEN 'Média Efetividade'
            WHEN pd.total_proposicoes > 0 AND pd.proposicoes_aprovadas::FLOAT / pd.total_proposicoes >= 0.05 THEN 'Baixa Efetividade'
            ELSE 'Muito Baixa Efetividade'
        END AS classificacao_efetividade,
        
        -- Especialização em tipos de proposição
        CASE 
            WHEN pd.projetos_lei >= pd.total_proposicoes * 0.5 THEN 'Especialista em Projetos de Lei'
            WHEN pd.propostas_emenda_constitucional >= pd.total_proposicoes * 0.3 THEN 'Especialista em PECs'
            WHEN pd.requerimentos >= pd.total_proposicoes * 0.5 THEN 'Especialista em Requerimentos'
            ELSE 'Generalista'
        END AS perfil_especializacao,
        
        -- Flags de performance
        CASE WHEN pd.total_proposicoes > uf.media_proposicoes_uf THEN TRUE ELSE FALSE END AS flag_acima_media_uf,
        CASE WHEN pd.total_proposicoes > pt.media_proposicoes_partido THEN TRUE ELSE FALSE END AS flag_acima_media_partido,
        CASE WHEN pd.total_proposicoes > 0 AND pd.proposicoes_aprovadas::FLOAT / pd.total_proposicoes > 0.2 THEN TRUE ELSE FALSE END AS flag_alta_efetividade,
        CASE WHEN pd.media_coautores > 2 THEN TRUE ELSE FALSE END AS flag_colaborativo,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM proposicoes_deputado pd
    LEFT JOIN produtividade_por_uf uf ON pd.sigla_uf = uf.sigla_uf
    LEFT JOIN produtividade_por_partido pt ON pd.sigla_partido = pt.sigla_partido
)

SELECT * FROM final
