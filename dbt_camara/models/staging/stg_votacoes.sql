{{ config(
    materialized='view'
) }}

SELECT 
    TRIM(id) AS id_votacao,
    data as data_votacao,
    UPPER(TRIM(descricao)) AS descricao,
    aprovacao as aprovada,
    id_evento,
    id_orgao,
    UPPER(TRIM(sigla_orgao)) AS sigla_orgao,
    TRIM(ultima_apresentacao_proposicao_descricao) AS proposicao_descricao,
    ultima_apresentacao_proposicao_id as proposicao_id,
    TRIM(uri) AS uri,
    TRIM(uri_evento) AS uri_evento,
    TRIM(uri_orgao) AS uri_orgao,
    votos_sim,
    votos_nao,
    votos_outros,
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'votacoes') }}
WHERE id IS NOT NULL 
  AND data IS NOT NULL
  AND data >= '2000-01-01'
