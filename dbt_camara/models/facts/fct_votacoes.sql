{{
  config(
    materialized='table',
    unique_key='sk_votacao'
  )
}}

-- Fato Votações: une votações, votos, proposições e deputados
WITH votacoes_base AS (
    SELECT 
        sv.id_votacao,
        sv.data_votacao,
        sv.descricao AS descricao_votacao,
        sv.aprovada,
        sv.id_evento,
        sv.id_orgao,
        sv.sigla_orgao,
        sv.proposicao_descricao,
        sv.proposicao_id,
        sv.uri AS uri_votacao,
        sv.uri_evento,
        sv.uri_orgao,
        sv.votos_sim,
        sv.votos_nao,
        sv.votos_outros,
        sv.ano,
        sv.mes,
        sv.data_carga
    FROM {{ ref('stg_votacoes') }} sv
    WHERE sv.id_votacao IS NOT NULL
),

votos_agregados AS (
    SELECT 
        sv.id_votacao,
        COUNT(*) AS total_votos_registrados,
        COUNT(CASE WHEN sv.tipo_voto = 'SIM' THEN 1 END) AS votos_sim_detalhados,
        COUNT(CASE WHEN sv.tipo_voto IN ('NÃO', 'NAO') THEN 1 END) AS votos_nao_detalhados,
        COUNT(CASE WHEN sv.tipo_voto = 'ABSTENÇÃO' THEN 1 END) AS votos_abstencao,
        COUNT(CASE WHEN sv.tipo_voto = 'OBSTRUÇÃO' THEN 1 END) AS votos_obstrucao,
        COUNT(CASE WHEN sv.tipo_voto = 'ARTIGO 17' THEN 1 END) AS votos_artigo17,
        COUNT(CASE WHEN sv.tipo_voto NOT IN ('SIM', 'NÃO', 'NAO', 'ABSTENÇÃO', 'OBSTRUÇÃO', 'ARTIGO 17') THEN 1 END) AS votos_outros_detalhados,
          -- Partidos que mais votaram SIM
        LISTAGG(DISTINCT CASE WHEN sv.tipo_voto = 'SIM' THEN dd.sigla_partido END, ', ') AS partidos_favoraveis,
        
        -- Partidos que mais votaram NÃO
        LISTAGG(DISTINCT CASE WHEN sv.tipo_voto IN ('NÃO', 'NAO') THEN dd.sigla_partido END, ', ') AS partidos_contrarios
              FROM {{ ref('stg_votos') }} sv
    LEFT JOIN {{ ref('dim_deputados') }} dd 
        ON sv.deputado_id = dd.nk_deputado
        AND sv.data_registro_voto BETWEEN dd.data_inicio_vigencia AND COALESCE(dd.data_fim_vigencia, '9999-12-31'::DATE)
    GROUP BY sv.id_votacao
),

proposicoes_info AS (
    SELECT 
        sp.proposicao_id,
        sp.sigla_tipo,
        sp.numero,
        sp.ano AS ano_proposicao,
        sp.cod_tipo,
        sp.descricao_tipo,
        sp.ementa,
        sp.ementa_detalhada,
        sp.data_apresentacao,
        sp.ultimo_status_descricao_situacao,
        sp.ultimo_status_id_situacao,
        sp.ultimo_status_apreciacao
    FROM {{ ref('stg_proposicoes') }} sp
),

fct_votacoes AS (
    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'vb.id_votacao'
        ]) }} AS sk_votacao,
        
        -- Chaves naturais
        vb.id_votacao AS nk_votacao,
        vb.proposicao_id AS nk_proposicao,
          -- Informações da votação
        vb.data_votacao,
        vb.descricao_votacao,
        CASE 
            WHEN UPPER(TRIM(vb.aprovada::STRING)) IN ('TRUE', '1', 'SIM', 'S') THEN TRUE
            ELSE FALSE
        END AS aprovada,
        vb.id_evento,
        vb.id_orgao,
        vb.sigla_orgao,
        vb.uri_votacao,
        vb.uri_evento,
        vb.uri_orgao,
        
        -- Resultados oficiais
        vb.votos_sim,
        vb.votos_nao,
        vb.votos_outros,
        vb.votos_sim + vb.votos_nao + vb.votos_outros AS total_votos_oficiais,
        
        -- Resultados detalhados (dos votos individuais)
        COALESCE(va.total_votos_registrados, 0) AS total_votos_registrados,
        COALESCE(va.votos_sim_detalhados, 0) AS votos_sim_detalhados,
        COALESCE(va.votos_nao_detalhados, 0) AS votos_nao_detalhados,
        COALESCE(va.votos_abstencao, 0) AS votos_abstencao,
        COALESCE(va.votos_obstrucao, 0) AS votos_obstrucao,
        COALESCE(va.votos_artigo17, 0) AS votos_artigo17,
        COALESCE(va.votos_outros_detalhados, 0) AS votos_outros_detalhados,
        
        -- Informações da proposição
        pi.sigla_tipo AS proposicao_sigla_tipo,
        pi.numero AS proposicao_numero,
        pi.ano_proposicao,
        pi.descricao_tipo AS proposicao_descricao_tipo,
        COALESCE(pi.ementa, vb.proposicao_descricao) AS proposicao_ementa,
        pi.ementa_detalhada AS proposicao_ementa_detalhada,
        pi.data_apresentacao AS proposicao_data_apresentacao,
        pi.ultimo_status_descricao_situacao AS proposicao_situacao_atual,
        pi.ultimo_status_apreciacao AS proposicao_apreciacao,
        
        -- Análises políticas
        va.partidos_favoraveis,
        va.partidos_contrarios,
        
        -- Métricas derivadas
        CASE 
            WHEN vb.votos_sim + vb.votos_nao + vb.votos_outros = 0 THEN NULL
            ELSE ROUND((vb.votos_sim::FLOAT / (vb.votos_sim + vb.votos_nao + vb.votos_outros)) * 100, 2)
        END AS percentual_aprovacao,
        
        CASE 
            WHEN vb.votos_nao + vb.votos_outros > 0
            THEN ROUND((vb.votos_nao::FLOAT / (vb.votos_sim + vb.votos_nao + vb.votos_outros)) * 100, 2)
            ELSE 0
        END AS percentual_rejeicao,
          CASE 
            WHEN UPPER(TRIM(vb.aprovada::STRING)) IN ('TRUE', '1', 'SIM', 'S') THEN 'APROVADA'
            ELSE 'REJEITADA'
        END AS resultado_votacao,
        
        CASE 
            WHEN vb.votos_sim > vb.votos_nao + vb.votos_outros THEN 'MAIORIA_SIMPLES'
            WHEN vb.votos_sim > (vb.votos_sim + vb.votos_nao + vb.votos_outros) * 0.66 THEN 'MAIORIA_QUALIFICADA'
            WHEN vb.votos_sim = vb.votos_nao THEN 'EMPATE'
            ELSE 'MAIORIA_CONTRA'
        END AS tipo_maioria,
        
        -- Classificação por período
        vb.ano,
        vb.mes,
        CASE 
            WHEN vb.mes IN (1,2,3) THEN 'T1'
            WHEN vb.mes IN (4,5,6) THEN 'T2'
            WHEN vb.mes IN (7,8,9) THEN 'T3'
            ELSE 'T4'
        END AS trimestre,
        
        -- Flags de controle
        CASE 
            WHEN pi.proposicao_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS tem_proposicao_vinculada,
        
        CASE 
            WHEN va.total_votos_registrados > 0 THEN TRUE 
            ELSE FALSE 
        END AS tem_votos_detalhados,
        
        CASE 
            WHEN COALESCE(va.votos_obstrucao, 0) > 0 THEN TRUE 
            ELSE FALSE 
        END AS teve_obstrucao,
        
        CASE 
            WHEN COALESCE(va.total_votos_registrados, 0) >= 300 THEN TRUE 
            ELSE FALSE 
        END AS eh_votacao_expressiva,
        
        -- Metadados
        vb.data_carga,
        CURRENT_TIMESTAMP() AS data_atualizacao
        
    FROM votacoes_base vb
    LEFT JOIN votos_agregados va 
        ON vb.id_votacao = va.id_votacao
    LEFT JOIN proposicoes_info pi 
        ON vb.proposicao_id = pi.proposicao_id
)

SELECT * FROM fct_votacoes
ORDER BY data_votacao DESC, nk_votacao
