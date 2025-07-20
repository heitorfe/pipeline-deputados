{{ config(
    materialized='view'
) }}

SELECT 
    id AS deputado_id,
    UPPER(TRIM(nome)) AS nome_deputado,
    UPPER(TRIM(partido)) AS sigla_partido,
    UPPER(TRIM(uf)) AS sigla_uf,
    url_foto,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'deputados') }}
WHERE id IS NOT NULL
