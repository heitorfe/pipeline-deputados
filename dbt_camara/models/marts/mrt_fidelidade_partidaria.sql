{{
  config(
    materialized='table',
    unique_key='sk_fidelidade_partido'
  )
}}

-- Mart Fidelidade Partidária: Análise de coesão e disciplina partidária nas votações
WITH votos_por_partido AS (
    SELECT 
        fvi.sigla_partido,
        COUNT(*) AS total_votos_partido,
        COUNT(DISTINCT fvi.nk_deputado) AS total_deputados_partido,
        COUNT(DISTINCT fvi.nk_votacao) AS total_votacoes_partido,
        
        -- Análise de fidelidade
        COUNT(CASE WHEN fvi.fidelidade_partidaria = 'SEGUIU_ORIENTACAO' THEN 1 END) AS votos_fieis,
        COUNT(CASE WHEN fvi.fidelidade_partidaria = 'CONTRARIOU_ORIENTACAO' THEN 1 END) AS votos_rebeldes,
        COUNT(CASE WHEN fvi.flag_tem_orientacao_partido THEN 1 END) AS votos_com_orientacao,
        
        -- Análise por tipo de voto
        COUNT(CASE WHEN fvi.tipo_voto = 'SIM' THEN 1 END) AS votos_sim_partido,
        COUNT(CASE WHEN fvi.tipo_voto IN ('NÃO', 'NAO') THEN 1 END) AS votos_nao_partido,
        COUNT(CASE WHEN fvi.tipo_voto = 'ABSTENÇÃO' THEN 1 END) AS votos_abstencao_partido,
        COUNT(CASE WHEN fvi.tipo_voto = 'OBSTRUÇÃO' THEN 1 END) AS votos_obstrucao_partido,
        
        -- Análise temporal
        COUNT(CASE WHEN fvi.ano_votacao = 2023 THEN 1 END) AS votos_2023,
        COUNT(CASE WHEN fvi.ano_votacao = 2022 THEN 1 END) AS votos_2022,
        COUNT(CASE WHEN fvi.ano_votacao = 2021 THEN 1 END) AS votos_2021,
        
        -- Análise por tipo de proposição
        COUNT(CASE WHEN fvi.sigla_tipo_proposicao = 'PL' THEN 1 END) AS votos_projetos_lei,
        COUNT(CASE WHEN fvi.sigla_tipo_proposicao = 'PEC' THEN 1 END) AS votos_pec,
        COUNT(CASE WHEN fvi.sigla_tipo_proposicao = 'MPV' THEN 1 END) AS votos_medida_provisoria,
        
        -- Efetividade política do partido
        COUNT(CASE WHEN fvi.flag_votacao_aprovada AND fvi.tipo_voto = 'SIM' THEN 1 END) AS votos_lado_vencedor_sim,
        COUNT(CASE WHEN NOT fvi.flag_votacao_aprovada AND fvi.tipo_voto IN ('NÃO', 'NAO') THEN 1 END) AS votos_lado_vencedor_nao,
        COUNT(CASE WHEN fvi.categoria_voto = 'POSICIONAMENTO' THEN 1 END) AS votos_com_posicionamento
        
    FROM {{ ref('fct_votos_individuais') }} fvi
    WHERE fvi.sigla_partido IS NOT NULL
    GROUP BY fvi.sigla_partido
),

coesao_por_votacao AS (
    -- Analisa a coesão interna do partido em cada votação
    SELECT 
        fvi.sigla_partido,
        fvi.nk_votacao,
        COUNT(*) AS deputados_votaram,
        COUNT(DISTINCT fvi.tipo_voto) AS tipos_voto_distintos,
        
        -- Contagem por tipo de voto
        COUNT(CASE WHEN fvi.tipo_voto = 'SIM' THEN 1 END) AS votos_sim_votacao,
        COUNT(CASE WHEN fvi.tipo_voto IN ('NÃO', 'NAO') THEN 1 END) AS votos_nao_votacao,
        COUNT(CASE WHEN fvi.tipo_voto = 'ABSTENÇÃO' THEN 1 END) AS votos_abstencao_votacao,
        COUNT(CASE WHEN fvi.tipo_voto = 'OBSTRUÇÃO' THEN 1 END) AS votos_obstrucao_votacao,
        
        -- Calcula o percentual do voto majoritário do partido
        ROUND(
            GREATEST(
                COUNT(CASE WHEN fvi.tipo_voto = 'SIM' THEN 1 END)::FLOAT,
                COUNT(CASE WHEN fvi.tipo_voto IN ('NÃO', 'NAO') THEN 1 END)::FLOAT,
                COUNT(CASE WHEN fvi.tipo_voto = 'ABSTENÇÃO' THEN 1 END)::FLOAT,
                COUNT(CASE WHEN fvi.tipo_voto = 'OBSTRUÇÃO' THEN 1 END)::FLOAT
            ) / COUNT(*) * 100, 2
        ) AS coesao_interna_percentual
        
    FROM {{ ref('fct_votos_individuais') }} fvi
    WHERE fvi.sigla_partido IS NOT NULL
    GROUP BY fvi.sigla_partido, fvi.nk_votacao
    HAVING COUNT(*) >= 3  -- Apenas votações com pelo menos 3 deputados do partido
),

metricas_coesao AS (
    SELECT 
        sigla_partido,
        COUNT(*) AS total_votacoes_analisadas,
        ROUND(AVG(coesao_interna_percentual), 2) AS coesao_media_partido,
        ROUND(STDDEV(coesao_interna_percentual), 2) AS desvio_coesao,
        COUNT(CASE WHEN coesao_interna_percentual >= 90 THEN 1 END) AS votacoes_alta_coesao,
        COUNT(CASE WHEN coesao_interna_percentual < 60 THEN 1 END) AS votacoes_baixa_coesao,
        COUNT(CASE WHEN tipos_voto_distintos = 1 THEN 1 END) AS votacoes_unanimes
    FROM coesao_por_votacao
    GROUP BY sigla_partido
),

benchmarks AS (
    SELECT 
        ROUND(AVG(vpp.votos_fieis::FLOAT / NULLIF(vpp.votos_com_orientacao, 0) * 100), 2) AS benchmark_fidelidade,
        ROUND(AVG(mc.coesao_media_partido), 2) AS benchmark_coesao,
        COUNT(*) AS total_partidos_analisados
    FROM votos_por_partido vpp
    LEFT JOIN metricas_coesao mc ON vpp.sigla_partido = mc.sigla_partido
    WHERE vpp.total_deputados_partido >= 5  -- Apenas partidos com bancada significativa
),

final AS (
    SELECT 
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'vpp.sigla_partido'
        ]) }} AS sk_fidelidade_partido,
        
        -- Identificação
        vpp.sigla_partido,
        vpp.total_deputados_partido,
        vpp.total_votacoes_partido,
        vpp.total_votos_partido,
        
        -- Métricas de fidelidade
        vpp.votos_fieis,
        vpp.votos_rebeldes,
        vpp.votos_com_orientacao,
        
        -- Taxa de fidelidade
        CASE 
            WHEN vpp.votos_com_orientacao > 0 
            THEN ROUND(vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao * 100, 2)
            ELSE 0 
        END AS taxa_fidelidade_partidaria,
        
        -- Taxa de rebeldia
        CASE 
            WHEN vpp.votos_com_orientacao > 0 
            THEN ROUND(vpp.votos_rebeldes::FLOAT / vpp.votos_com_orientacao * 100, 2)
            ELSE 0 
        END AS taxa_rebeldia_partidaria,
        
        -- Métricas de coesão interna
        COALESCE(mc.coesao_media_partido, 0) AS coesao_media_partido,
        COALESCE(mc.desvio_coesao, 0) AS desvio_coesao,
        COALESCE(mc.votacoes_alta_coesao, 0) AS votacoes_alta_coesao,
        COALESCE(mc.votacoes_baixa_coesao, 0) AS votacoes_baixa_coesao,
        COALESCE(mc.votacoes_unanimes, 0) AS votacoes_unanimes,
        COALESCE(mc.total_votacoes_analisadas, 0) AS total_votacoes_analisadas,
        
        -- Percentual de votações com alta coesão
        CASE 
            WHEN mc.total_votacoes_analisadas > 0 
            THEN ROUND(mc.votacoes_alta_coesao::FLOAT / mc.total_votacoes_analisadas * 100, 2)
            ELSE 0 
        END AS percentual_alta_coesao,
        
        -- Distribuição de votos
        vpp.votos_sim_partido,
        vpp.votos_nao_partido,
        vpp.votos_abstencao_partido,
        vpp.votos_obstrucao_partido,
        
        -- Tendência de posicionamento
        CASE 
            WHEN vpp.votos_sim_partido > vpp.votos_nao_partido THEN 'GOVERNO'
            WHEN vpp.votos_nao_partido > vpp.votos_sim_partido THEN 'OPOSIÇÃO'
            ELSE 'NEUTRO'
        END AS tendencia_posicionamento,
        
        -- Análise temporal
        vpp.votos_2023,
        vpp.votos_2022,
        vpp.votos_2021,
        
        -- Análise por tipo de proposição
        vpp.votos_projetos_lei,
        vpp.votos_pec,
        vpp.votos_medida_provisoria,
        
        -- Efetividade política
        vpp.votos_lado_vencedor_sim + vpp.votos_lado_vencedor_nao AS total_votos_lado_vencedor,
        CASE 
            WHEN vpp.votos_com_posicionamento > 0 
            THEN ROUND((vpp.votos_lado_vencedor_sim + vpp.votos_lado_vencedor_nao)::FLOAT / vpp.votos_com_posicionamento * 100, 2)
            ELSE 0 
        END AS taxa_efetividade_politica,
        
        -- Comparações com benchmarks
        b.benchmark_fidelidade,
        b.benchmark_coesao,
        
        -- Rankings e classificações
        CASE 
            WHEN vpp.votos_com_orientacao > 0 AND vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao >= 0.9 THEN 'Muito Disciplinado'
            WHEN vpp.votos_com_orientacao > 0 AND vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao >= 0.8 THEN 'Disciplinado'
            WHEN vpp.votos_com_orientacao > 0 AND vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao >= 0.7 THEN 'Moderadamente Disciplinado'
            WHEN vpp.votos_com_orientacao > 0 AND vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao >= 0.5 THEN 'Pouco Disciplinado'
            ELSE 'Indisciplinado'
        END AS classificacao_disciplina,
        
        CASE 
            WHEN COALESCE(mc.coesao_media_partido, 0) >= 85 THEN 'Muito Coeso'
            WHEN COALESCE(mc.coesao_media_partido, 0) >= 75 THEN 'Coeso'
            WHEN COALESCE(mc.coesao_media_partido, 0) >= 65 THEN 'Moderadamente Coeso'
            WHEN COALESCE(mc.coesao_media_partido, 0) >= 50 THEN 'Pouco Coeso'
            ELSE 'Fragmentado'
        END AS classificacao_coesao,
        
        CASE 
            WHEN vpp.total_deputados_partido >= 50 THEN 'Grande'
            WHEN vpp.total_deputados_partido >= 20 THEN 'Médio'
            WHEN vpp.total_deputados_partido >= 10 THEN 'Pequeno'
            ELSE 'Micro'
        END AS tamanho_bancada,
        
        -- Flags de destaque
        CASE WHEN vpp.votos_com_orientacao > 0 AND vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao >= 0.85 THEN TRUE ELSE FALSE END AS flag_alta_disciplina,
        CASE WHEN COALESCE(mc.coesao_media_partido, 0) >= 80 THEN TRUE ELSE FALSE END AS flag_alta_coesao,
        CASE WHEN vpp.votos_com_orientacao > 0 AND vpp.votos_fieis::FLOAT / vpp.votos_com_orientacao > b.benchmark_fidelidade / 100 THEN TRUE ELSE FALSE END AS flag_fidelidade_acima_benchmark,
        CASE WHEN COALESCE(mc.coesao_media_partido, 0) > b.benchmark_coesao THEN TRUE ELSE FALSE END AS flag_coesao_acima_benchmark,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM votos_por_partido vpp
    LEFT JOIN metricas_coesao mc ON vpp.sigla_partido = mc.sigla_partido
    CROSS JOIN benchmarks b
    WHERE vpp.total_deputados_partido >= 3  -- Apenas partidos com bancada mínima
)

SELECT * FROM final
ORDER BY taxa_fidelidade_partidaria DESC, coesao_media_partido DESC
