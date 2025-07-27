{{ config(
    materialized='view'
) }}

SELECT 
    TRIM(id) AS id_votacao,
    data AS data_votacao,
    data_hora_registro,
    UPPER(TRIM(descricao)) AS descricao_votacao,
    aprovacao,
    id_orgao,
    UPPER(TRIM(sigla_orgao)) AS sigla_orgao,
    efeitos_registrados,
    proposicoes_afetadas,
    ultima_apresentacao_proposicao,
    uri,
    uri_orgao,
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'votacoes') }}
WHERE id IS NOT NULL 
  AND data IS NOT NULL
  AND data >= '2000-01-01'
