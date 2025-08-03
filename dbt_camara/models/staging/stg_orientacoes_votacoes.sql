{{
  config(
    materialized='view'
  )
}}

-- Staging Orientações de Votações: padroniza dados de orientação partidária em votações
WITH orientacoes_base AS (
    SELECT 
        id_votacao,
        descricao,
        orientacao,
        sigla_bancada,
        sigla_orgao,
        uri_bancada,
        uri_votacao,
        
        -- Padronização da orientação
        CASE 
            WHEN UPPER(TRIM(orientacao)) IN ('SIM', 'S', 'FAVORÁVEL', 'FAVORAVEL') THEN 'SIM'
            WHEN UPPER(TRIM(orientacao)) IN ('NÃO', 'NAO', 'N', 'CONTRÁRIO', 'CONTRARIO') THEN 'NÃO'
            WHEN UPPER(TRIM(orientacao)) IN ('ABSTENÇÃO', 'ABSTENCAO', 'ABSTER') THEN 'ABSTENÇÃO'
            WHEN UPPER(TRIM(orientacao)) IN ('OBSTRUÇÃO', 'OBSTRUCAO', 'OBSTRUIR') THEN 'OBSTRUÇÃO'
            WHEN UPPER(TRIM(orientacao)) IN ('LIBERADO', 'LIVRE', 'LIBERAL') THEN 'LIBERADO'
            ELSE UPPER(TRIM(orientacao))
        END AS orientacao_padronizada,
        
        -- Classificação da bancada
        CASE 
            WHEN sigla_bancada IS NOT NULL AND LENGTH(TRIM(sigla_bancada)) BETWEEN 2 AND 10 THEN 'PARTIDO'
            WHEN sigla_bancada ILIKE '%BLOCO%' OR sigla_bancada ILIKE '%BLOC%' THEN 'BLOCO'
            WHEN sigla_bancada ILIKE '%FRENTE%' OR sigla_bancada ILIKE '%FRONT%' THEN 'FRENTE'
            WHEN sigla_bancada ILIKE '%GOVERNO%' OR sigla_bancada ILIKE '%GOV%' THEN 'GOVERNO'
            WHEN sigla_bancada ILIKE '%OPOSIÇÃO%' OR sigla_bancada ILIKE '%OPOS%' THEN 'OPOSIÇÃO'
            ELSE 'OUTRO'
        END AS tipo_bancada,
        
        -- Flags de controle
        CASE WHEN orientacao IS NOT NULL AND TRIM(orientacao) != '' THEN TRUE ELSE FALSE END AS tem_orientacao,
        CASE WHEN uri_votacao IS NOT NULL AND TRIM(uri_votacao) != '' THEN TRUE ELSE FALSE END AS tem_uri_votacao,
        CASE WHEN uri_bancada IS NOT NULL AND TRIM(uri_bancada) != '' THEN TRUE ELSE FALSE END AS tem_uri_bancada,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM {{ source('camara_raw', 'votacoes_orientacoes') }}
    WHERE id_votacao IS NOT NULL
)

SELECT 
    id_votacao,
    descricao,
    orientacao,
    orientacao_padronizada,
    sigla_bancada,
    sigla_orgao,
    tipo_bancada,
    uri_bancada,
    uri_votacao,
    tem_orientacao,
    tem_uri_votacao,
    tem_uri_bancada,
    data_processamento
FROM orientacoes_base
ORDER BY id_votacao, sigla_bancada
