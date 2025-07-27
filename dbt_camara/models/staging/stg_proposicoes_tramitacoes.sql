{{ config(
    materialized='view'
) }}

SELECT 
    proposicao_id,
    sequencia,
    UPPER(TRIM(ambito)) AS ambito,
    UPPER(TRIM(apreciacao)) AS apreciacao,
    TRIM(cod_tipo_tramitacao) AS cod_tipo_tramitacao,
    data_hora AS data_tramitacao,
    UPPER(TRIM(descricao_tramitacao)) AS descricao_tramitacao,
    despacho,
    UPPER(TRIM(regime)) AS regime,
    UPPER(TRIM(sigla_orgao)) AS sigla_orgao,
    uri_orgao,
    url,
    EXTRACT(YEAR FROM data_hora) AS ano,
    EXTRACT(MONTH FROM data_hora) AS mes,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'proposicoes_tramitacoes') }}
WHERE proposicao_id IS NOT NULL 
  AND sequencia IS NOT NULL
  AND data_hora IS NOT NULL
  AND data_hora >= '2000-01-01'
