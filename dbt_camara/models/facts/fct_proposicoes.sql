{{
  config(
    materialized='table',
    unique_key='sk_proposicao'
  )
}}

-- Fato Proposições: status final de cada proposição e consolidação das votações
WITH proposicoes_base AS (
    SELECT 
        sp.proposicao_id,
        sp.sigla_tipo,
        sp.numero,
        sp.ano,
        sp.cod_tipo,
        sp.descricao_tipo,
        sp.ementa,
        sp.ementa_detalhada,
        sp.keywords,
        sp.data_apresentacao,
        
        -- Status atual
        sp.ultimo_status_data_hora,
        sp.ultimo_status_sequencia,
        sp.ultimo_status_uri_relator,
        sp.ultimo_status_id_orgao,
        sp.ultimo_status_sigla_orgao,
        sp.ultimo_status_uri_orgao,
        sp.ultimo_status_regime,
        sp.ultimo_status_descricao_tramitacao,
        sp.ultimo_status_id_tipo_tramitacao,
        sp.ultimo_status_descricao_situacao,
        sp.ultimo_status_id_situacao,
        sp.ultimo_status_despacho,
        sp.ultimo_status_apreciacao,
        sp.ultimo_status_url,
        
        sp.data_carga
    FROM {{ ref('stg_proposicoes') }} sp
    WHERE sp.proposicao_id IS NOT NULL
),

autores_agregados AS (
    SELECT 
        spa.proposicao_id,
        COUNT(*) AS total_autores,
        COUNT(CASE WHEN spa.eh_proponente THEN 1 END) AS total_proponentes,
        
        -- Autor principal (proponente)
        MAX(CASE WHEN spa.eh_proponente THEN spa.deputado_autor_id END) AS deputado_proponente_id,
        MAX(CASE WHEN spa.eh_proponente THEN spa.nome_autor END) AS nome_proponente,
        MAX(CASE WHEN spa.eh_proponente THEN spa.sigla_partido_autor END) AS partido_proponente,
        MAX(CASE WHEN spa.eh_proponente THEN spa.sigla_uf_autor END) AS uf_proponente
            
    FROM {{ ref('stg_proposicoes_autores') }} spa
    GROUP BY spa.proposicao_id
),

temas_agregados AS (
    SELECT 
        spt.proposicao_id,
        COUNT(*) AS total_temas,
        LISTAGG(DISTINCT spt.tema, ', ') 
            WITHIN GROUP (ORDER BY spt.tema) AS temas_proposicao
    FROM {{ ref('stg_proposicoes_temas') }} spt
    GROUP BY spt.proposicao_id
),

votacoes_consolidadas AS (
    SELECT 
        sv.proposicao_id,
        COUNT(*) AS total_votacoes,        COUNT(CASE WHEN UPPER(TRIM(sv.aprovada::STRING)) IN ('TRUE', '1', 'SIM', 'S') THEN 1 END) AS votacoes_aprovadas,
        COUNT(CASE WHEN NOT (UPPER(TRIM(sv.aprovada::STRING)) IN ('TRUE', '1', 'SIM', 'S')) THEN 1 END) AS votacoes_rejeitadas,
          -- Última votação
        MAX(sv.data_votacao) AS data_ultima_votacao,
        
        -- Métricas de todas as votações
        SUM(sv.votos_sim) AS total_votos_sim_historico,
        SUM(sv.votos_nao) AS total_votos_nao_historico,
        SUM(sv.votos_outros) AS total_votos_outros_historico
        
    FROM {{ ref('stg_votacoes') }} sv
    WHERE sv.proposicao_id IS NOT NULL
    GROUP BY sv.proposicao_id
),

proposicoes_completas AS (
    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'pb.proposicao_id'
        ]) }} AS sk_proposicao,
        
        -- Chave natural
        pb.proposicao_id AS nk_proposicao,
        
        -- Informações básicas da proposição
        pb.sigla_tipo,
        pb.numero,
        pb.ano,
        pb.cod_tipo,
        pb.descricao_tipo,
        pb.ementa,
        pb.ementa_detalhada,
        pb.keywords,
        pb.data_apresentacao,
        
        -- Status atual
        pb.ultimo_status_data_hora,
        pb.ultimo_status_descricao_situacao,
        pb.ultimo_status_apreciacao,
        pb.ultimo_status_sigla_orgao,
        
        -- Informações dos autores
        COALESCE(aa.total_autores, 0) AS total_autores,
        COALESCE(aa.total_proponentes, 0) AS total_proponentes,
        aa.deputado_proponente_id,
        aa.nome_proponente,
        aa.partido_proponente,
        aa.uf_proponente,
        
        -- Informações dos temas
        COALESCE(ta.total_temas, 0) AS total_temas,
        ta.temas_proposicao,
        
        -- Informações das votações
        COALESCE(vc.total_votacoes, 0) AS total_votacoes,
        COALESCE(vc.votacoes_aprovadas, 0) AS votacoes_aprovadas,
        COALESCE(vc.votacoes_rejeitadas, 0) AS votacoes_rejeitadas,
        vc.data_ultima_votacao,
        vc.total_votos_sim_historico,
        vc.total_votos_nao_historico,
        vc.total_votos_outros_historico,
        
        -- Análises e classificações
        CASE 
            WHEN pb.ultimo_status_descricao_situacao ILIKE '%APROVAD%' THEN 'APROVADA'
            WHEN pb.ultimo_status_descricao_situacao ILIKE '%ARQUIVAD%' THEN 'ARQUIVADA'
            WHEN pb.ultimo_status_descricao_situacao ILIKE '%REJEITAD%' THEN 'REJEITADA'
            WHEN pb.ultimo_status_descricao_situacao ILIKE '%TRAMITA%' THEN 'EM_TRAMITACAO'
            WHEN pb.ultimo_status_descricao_situacao ILIKE '%PENDENT%' THEN 'PENDENTE'
            ELSE 'OUTRO_STATUS'
        END AS status_final_classificado,
        
        CASE 
            WHEN vc.total_votacoes > 0 THEN 'COM_VOTACAO'
            ELSE 'SEM_VOTACAO'
        END AS status_votacao,
        
        -- Tempo de tramitação
        CASE 
            WHEN pb.data_apresentacao IS NOT NULL AND pb.ultimo_status_data_hora IS NOT NULL
            THEN DATEDIFF('day', pb.data_apresentacao, pb.ultimo_status_data_hora)
            ELSE NULL
        END AS dias_tramitacao,
        
        -- Período de apresentação
        EXTRACT(YEAR FROM pb.data_apresentacao) AS ano_apresentacao,
        EXTRACT(MONTH FROM pb.data_apresentacao) AS mes_apresentacao,
        
        -- Flags de controle
        CASE WHEN aa.deputado_proponente_id IS NOT NULL THEN TRUE ELSE FALSE END AS tem_deputado_proponente,
        CASE WHEN ta.total_temas > 0 THEN TRUE ELSE FALSE END AS tem_temas_classificados,
        CASE WHEN vc.total_votacoes > 0 THEN TRUE ELSE FALSE END AS foi_votada,
        
        -- Metadados
        pb.data_carga,
        CURRENT_TIMESTAMP() AS data_atualizacao
        
    FROM proposicoes_base pb
    LEFT JOIN autores_agregados aa 
        ON pb.proposicao_id = aa.proposicao_id
    LEFT JOIN temas_agregados ta 
        ON pb.proposicao_id = ta.proposicao_id
    LEFT JOIN votacoes_consolidadas vc 
        ON pb.proposicao_id = vc.proposicao_id
)

SELECT * FROM proposicoes_completas
ORDER BY data_apresentacao DESC, nk_proposicao
