{{
  config(
    materialized='table',
    unique_key='sk_presenca_deputado'
  )
}}

-- Mart Presença em Sessões: Análise de presença, participação e fidelidade partidária
WITH presenca_fidelidade_deputado AS (
    SELECT 
        fvi.sk_deputado,
        fvi.nk_deputado AS deputado_id,
        fvi.nome_deputado,
        fvi.sigla_partido,
        fvi.sigla_uf,
        fvi.condicao_eleitoral,
        
        -- Contadores de votações
        COUNT(DISTINCT fvi.nk_votacao) AS total_votacoes_participadas,
        COUNT(*) AS total_votos_registrados,
        
        -- Por tipo de voto
        COUNT(CASE WHEN fvi.tipo_voto = 'SIM' THEN 1 END) AS votos_sim,
        COUNT(CASE WHEN fvi.tipo_voto IN ('NÃO', 'NAO') THEN 1 END) AS votos_nao,
        COUNT(CASE WHEN fvi.tipo_voto = 'ABSTENÇÃO' THEN 1 END) AS votos_abstencao,
        COUNT(CASE WHEN fvi.tipo_voto = 'OBSTRUÇÃO' THEN 1 END) AS votos_obstrucao,
        
        -- Análise de fidelidade partidária
        COUNT(CASE WHEN fvi.fidelidade_partidaria = 'SEGUIU_ORIENTACAO' THEN 1 END) AS votos_seguiu_orientacao,
        COUNT(CASE WHEN fvi.fidelidade_partidaria = 'CONTRARIOU_ORIENTACAO' THEN 1 END) AS votos_contrariou_orientacao,
        COUNT(CASE WHEN fvi.fidelidade_partidaria = 'SEM_ORIENTACAO' THEN 1 END) AS votos_sem_orientacao,
        COUNT(CASE WHEN fvi.fidelidade_partidaria = 'VOTO_LIVRE' THEN 1 END) AS votos_livres,
        COUNT(CASE WHEN fvi.flag_tem_orientacao_partido THEN 1 END) AS votos_com_orientacao_disponivel,
        
        -- Análise por ano
        COUNT(CASE WHEN fvi.ano_votacao = 2023 THEN 1 END) AS votos_2023,
        COUNT(CASE WHEN fvi.ano_votacao = 2022 THEN 1 END) AS votos_2022,
        COUNT(CASE WHEN fvi.ano_votacao = 2021 THEN 1 END) AS votos_2021,
        
        -- Análise por tipo de proposição
        COUNT(CASE WHEN fvi.sigla_tipo_proposicao = 'PL' THEN 1 END) AS votos_projetos_lei,
        COUNT(CASE WHEN fvi.sigla_tipo_proposicao = 'PEC' THEN 1 END) AS votos_pec,
        COUNT(CASE WHEN fvi.sigla_tipo_proposicao = 'MPV' THEN 1 END) AS votos_medida_provisoria,
        
        -- Análise de posicionamento
        COUNT(CASE WHEN fvi.categoria_voto = 'POSICIONAMENTO' THEN 1 END) AS votos_com_posicionamento,
        COUNT(CASE WHEN fvi.flag_votacao_aprovada AND fvi.tipo_voto = 'SIM' THEN 1 END) AS votos_lado_vencedor,
        COUNT(CASE WHEN NOT fvi.flag_votacao_aprovada AND fvi.tipo_voto IN ('NÃO', 'NAO') THEN 1 END) AS votos_lado_vencedor_nao,
        
        -- Análise temporal
        MIN(fvi.data_votacao) AS primeira_votacao,
        MAX(fvi.data_votacao) AS ultima_votacao
        
    FROM {{ ref('fct_votos_individuais') }} fvi
    GROUP BY 
        fvi.sk_deputado,
        fvi.nk_deputado,
        fvi.nome_deputado,
        fvi.sigla_partido,
        fvi.sigla_uf,
        fvi.condicao_eleitoral),

fidelidade_por_uf AS (
    SELECT 
        sigla_uf,
        COUNT(*) AS total_deputados_uf,
        ROUND(AVG(CASE WHEN votos_com_orientacao_disponivel > 0 
            THEN votos_seguiu_orientacao::FLOAT / votos_com_orientacao_disponivel * 100 
            ELSE 0 END), 2) AS taxa_fidelidade_media_uf
    FROM presenca_fidelidade_deputado
    GROUP BY sigla_uf
),

fidelidade_por_partido AS (
    SELECT 
        sigla_partido,
        COUNT(*) AS total_deputados_partido,
        ROUND(AVG(CASE WHEN votos_com_orientacao_disponivel > 0 
            THEN votos_seguiu_orientacao::FLOAT / votos_com_orientacao_disponivel * 100 
            ELSE 0 END), 2) AS taxa_fidelidade_media_partido
    FROM presenca_fidelidade_deputado
    GROUP BY sigla_partido
),

final AS (
    SELECT 
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'pfd.sk_deputado'
        ]) }} AS sk_presenca_deputado,
        
        -- Informações do deputado
        pfd.sk_deputado,
        pfd.deputado_id,
        pfd.nome_deputado,
        pfd.sigla_partido,
        pfd.sigla_uf,
        pfd.condicao_eleitoral,
        
        -- Métricas de participação
        pfd.total_votacoes_participadas,
        pfd.total_votos_registrados,
        
        -- Distribuição de votos
        pfd.votos_sim,
        pfd.votos_nao,
        pfd.votos_abstencao,
        pfd.votos_obstrucao,
        
        -- Percentuais por tipo de voto
        CASE 
            WHEN pfd.total_votos_registrados > 0 
            THEN ROUND(pfd.votos_sim::FLOAT / pfd.total_votos_registrados * 100, 2)
            ELSE 0 
        END AS percentual_votos_sim,
        
        CASE 
            WHEN pfd.total_votos_registrados > 0 
            THEN ROUND(pfd.votos_nao::FLOAT / pfd.total_votos_registrados * 100, 2)
            ELSE 0 
        END AS percentual_votos_nao,
        
        -- Métricas de fidelidade partidária
        pfd.votos_seguiu_orientacao,
        pfd.votos_contrariou_orientacao,
        pfd.votos_sem_orientacao,
        pfd.votos_livres,
        pfd.votos_com_orientacao_disponivel,
        
        -- Taxa de fidelidade partidária
        CASE 
            WHEN pfd.votos_com_orientacao_disponivel > 0 
            THEN ROUND(pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel * 100, 2)
            ELSE 0 
        END AS taxa_fidelidade_partidaria,
        
        -- Taxa de rebeldia
        CASE 
            WHEN pfd.votos_com_orientacao_disponivel > 0 
            THEN ROUND(pfd.votos_contrariou_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel * 100, 2)
            ELSE 0 
        END AS taxa_rebeldia_partidaria,
        
        -- Análise temporal anual
        pfd.votos_2023,
        pfd.votos_2022,
        pfd.votos_2021,
        
        -- Análise por tipo de proposição
        pfd.votos_projetos_lei,
        pfd.votos_pec,
        pfd.votos_medida_provisoria,
        
        -- Efetividade política
        pfd.votos_com_posicionamento,
        pfd.votos_lado_vencedor + pfd.votos_lado_vencedor_nao AS total_votos_lado_vencedor,
        
        CASE 
            WHEN pfd.votos_com_posicionamento > 0 
            THEN ROUND((pfd.votos_lado_vencedor + pfd.votos_lado_vencedor_nao)::FLOAT / pfd.votos_com_posicionamento * 100, 2)
            ELSE 0 
        END AS taxa_efetividade_politica,
        
        -- Temporal
        pfd.primeira_votacao,
        pfd.ultima_votacao,
        
        -- Comparações
        uf.taxa_fidelidade_media_uf,
        pt.taxa_fidelidade_media_partido,
        
        -- Classificações
        CASE 
            WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel >= 0.9 THEN 'Muito Fiel'
            WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel >= 0.8 THEN 'Fiel'
            WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel >= 0.7 THEN 'Moderadamente Fiel'
            WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel >= 0.5 THEN 'Pouco Fiel'
            ELSE 'Rebelde'
        END AS classificacao_fidelidade,
        
        CASE 
            WHEN pfd.total_votos_registrados >= 200 THEN 'Muito Ativo'
            WHEN pfd.total_votos_registrados >= 100 THEN 'Ativo'
            WHEN pfd.total_votos_registrados >= 50 THEN 'Moderadamente Ativo'
            ELSE 'Pouco Ativo'
        END AS classificacao_participacao,
        
        CASE 
            WHEN pfd.votos_abstencao::FLOAT / NULLIF(pfd.total_votos_registrados, 0) > 0.3 THEN 'Alto Abstencionista'
            WHEN pfd.votos_obstrucao::FLOAT / NULLIF(pfd.total_votos_registrados, 0) > 0.2 THEN 'Alto Obstrucionista'
            WHEN (pfd.votos_sim + pfd.votos_nao)::FLOAT / NULLIF(pfd.total_votos_registrados, 0) > 0.8 THEN 'Decisivo'
            ELSE 'Equilibrado'
        END AS perfil_votacao,
        
        -- Flags de performance
        CASE WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel >= 0.85 THEN TRUE ELSE FALSE END AS flag_alta_fidelidade,
        CASE WHEN pfd.total_votos_registrados >= 100 THEN TRUE ELSE FALSE END AS flag_participativo,
        CASE WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel > uf.taxa_fidelidade_media_uf / 100 THEN TRUE ELSE FALSE END AS flag_fidelidade_acima_media_uf,
        CASE WHEN pfd.votos_com_orientacao_disponivel > 0 AND pfd.votos_seguiu_orientacao::FLOAT / pfd.votos_com_orientacao_disponivel > pt.taxa_fidelidade_media_partido / 100 THEN TRUE ELSE FALSE END AS flag_fidelidade_acima_media_partido,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM presenca_fidelidade_deputado pfd
    LEFT JOIN fidelidade_por_uf uf ON pfd.sigla_uf = uf.sigla_uf
    LEFT JOIN fidelidade_por_partido pt ON pfd.sigla_partido = pt.sigla_partido
)


SELECT * FROM final
