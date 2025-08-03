{{
  config(
    materialized='table',
    unique_key='sk_voto_individual'
  )
}}

-- Fato Votos Individuais: construído apenas a partir das tabelas staging
-- Sem JOIN com dim_deputados para evitar problemas de compatibilidade temporal
WITH votos_base AS (
    SELECT 
        sv.id_votacao,
        sv.deputado_id,
        sv.tipo_voto,
        sv.data_registro_voto,
        sv.data_carga
    FROM {{ ref('stg_votos') }} sv
    WHERE sv.id_votacao IS NOT NULL 
      AND sv.deputado_id IS NOT NULL
),

votacoes_info AS (
    SELECT 
        sv.id_votacao,
        sv.data_votacao,
        sv.descricao AS descricao_votacao,
        sv.aprovada,
        sv.proposicao_id,
        sv.id_orgao,
        sv.sigla_orgao
    FROM {{ ref('stg_votacoes') }} sv
),

orientacoes_partido AS (
    SELECT 
        sov.id_votacao,
        sov.sigla_bancada AS sigla_partido_orientacao,
        sov.orientacao_padronizada,
        sov.tipo_bancada
    FROM {{ ref('stg_orientacoes_votacoes') }} sov
    WHERE sov.tipo_bancada = 'PARTIDO'
      AND sov.tem_orientacao = TRUE
),

proposicoes_info AS (
    SELECT 
        sp.proposicao_id,
        sp.sigla_tipo,
        sp.numero,
        sp.ano,
        sp.ementa
    FROM {{ ref('stg_proposicoes') }} sp
),

votos_com_contexto AS (
    SELECT 
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'vb.id_votacao',
            'vb.deputado_id'
        ]) }} AS sk_voto_individual,
        
        -- Chaves naturais (sem sk_deputado da dimensão)
        vb.id_votacao AS nk_votacao,
        vb.deputado_id AS nk_deputado,
        vi.proposicao_id AS nk_proposicao,
        
        -- Informações do voto
        vb.tipo_voto,
        vb.data_registro_voto,
        
        -- Informações da votação
        vi.data_votacao,
        vi.descricao_votacao,
        vi.aprovada AS votacao_aprovada,
        vi.id_orgao,
        vi.sigla_orgao,        
        -- Orientação do partido (simplificada sem JOIN com deputado)
        op.orientacao_padronizada AS orientacao_partido,
        
        -- Análise de fidelidade partidária (simplificada)
        CASE 
            WHEN op.orientacao_padronizada IS NULL THEN 'SEM_ORIENTACAO'
            WHEN op.orientacao_padronizada = 'LIBERADO' THEN 'VOTO_LIVRE'
            WHEN UPPER(TRIM(vb.tipo_voto)) = UPPER(TRIM(op.orientacao_padronizada)) THEN 'SEGUIU_ORIENTACAO'
            WHEN UPPER(TRIM(vb.tipo_voto)) != UPPER(TRIM(op.orientacao_padronizada)) THEN 'CONTRARIOU_ORIENTACAO'
            ELSE 'INDEFINIDO'
        END AS fidelidade_partidaria,
        
        -- Informações da proposição
        pi.sigla_tipo AS sigla_tipo_proposicao,
        pi.numero AS numero_proposicao,
        pi.ano AS ano_proposicao,
        pi.ementa,
        
        -- Análises derivadas
        CASE 
            WHEN UPPER(TRIM(vb.tipo_voto)) = 'SIM' THEN 1
            WHEN UPPER(TRIM(vb.tipo_voto)) IN ('NÃO', 'NAO') THEN -1
            ELSE 0
        END AS peso_voto_numerico,
        
        CASE 
            WHEN UPPER(TRIM(vb.tipo_voto)) IN ('SIM', 'NÃO', 'NAO') THEN 'POSICIONAMENTO'
            WHEN UPPER(TRIM(vb.tipo_voto)) = 'ABSTENÇÃO' THEN 'ABSTENCAO'
            WHEN UPPER(TRIM(vb.tipo_voto)) = 'OBSTRUÇÃO' THEN 'OBSTRUCAO'
            ELSE 'OUTROS'
        END AS categoria_voto,
        
        -- Metadados temporais
        EXTRACT(YEAR FROM vi.data_votacao) AS ano_votacao,
        EXTRACT(MONTH FROM vi.data_votacao) AS mes_votacao,
        CASE 
            WHEN EXTRACT(MONTH FROM vi.data_votacao) IN (1,2,3) THEN 'T1'
            WHEN EXTRACT(MONTH FROM vi.data_votacao) IN (4,5,6) THEN 'T2'
            WHEN EXTRACT(MONTH FROM vi.data_votacao) IN (7,8,9) THEN 'T3'
            ELSE 'T4'
        END AS trimestre_votacao,        -- Flags de controle
        CASE WHEN UPPER(TRIM(vi.aprovada::STRING)) IN ('TRUE', '1', 'SIM', 'S') THEN TRUE ELSE FALSE END AS flag_votacao_aprovada,
        CASE WHEN op.orientacao_padronizada IS NOT NULL THEN TRUE ELSE FALSE END AS flag_tem_orientacao_partido,
        CASE WHEN pi.proposicao_id IS NOT NULL THEN TRUE ELSE FALSE END AS flag_tem_proposicao_vinculada,
        
        -- Metadados
        vb.data_carga,
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM votos_base vb
    LEFT JOIN votacoes_info vi 
        ON vb.id_votacao = vi.id_votacao
    LEFT JOIN orientacoes_partido op 
        ON vb.id_votacao = op.id_votacao 
        -- Removido JOIN com deputado para orientação partidária
    LEFT JOIN proposicoes_info pi 
        ON vi.proposicao_id = pi.proposicao_id
    -- Removido filtro que exigia deputado válido na dimensão
)

SELECT * FROM votos_com_contexto
ORDER BY data_votacao DESC, nk_deputado, nk_votacao
