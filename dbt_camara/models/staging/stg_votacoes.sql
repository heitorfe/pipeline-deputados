{{ config(
    materialized='view'
) }}

-- Baseado na tabela votacoes_objetos que contém informações das votações e proposições
SELECT 
    TRIM(id_votacao) AS id_votacao,
    data AS data_votacao,
    UPPER(TRIM(descricao)) AS descricao_votacao,
    proposicao_id,
    UPPER(TRIM(proposicao_sigla_tipo)) AS proposicao_sigla_tipo,
    proposicao_numero,
    proposicao_ano,
    proposicao_cod_tipo,
    TRIM(proposicao_titulo) AS proposicao_titulo,
    TRIM(proposicao_ementa) AS proposicao_ementa,
    TRIM(proposicao_uri) AS proposicao_uri,
    TRIM(uri_votacao) AS uri_votacao,
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('raw', 'votacoes_objetos') }}
WHERE id_votacao IS NOT NULL 
  AND data IS NOT NULL
  AND data >= '2000-01-01'
