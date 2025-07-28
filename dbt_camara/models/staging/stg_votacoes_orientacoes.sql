{{ config(
    materialized='view'
) }}

-- Orientações de votação por bancadas/partidos
SELECT 
    TRIM(id_votacao) AS id_votacao,
    UPPER(TRIM(descricao)) AS descricao_votacao,
    UPPER(TRIM(orientacao)) AS orientacao,
    UPPER(TRIM(sigla_bancada)) AS sigla_bancada,
    UPPER(TRIM(sigla_orgao)) AS sigla_orgao,
    TRIM(uri_bancada) AS uri_bancada,
    TRIM(uri_votacao) AS uri_votacao,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('raw', 'votacoes_orientacoes') }}
WHERE id_votacao IS NOT NULL 
  AND orientacao IS NOT NULL
