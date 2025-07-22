{{ config(
    materialized='view'
) }}

SELECT 
    id AS deputado_id,
    {{ dbt_utils.generate_surrogate_key(['id', 'legislatura_id', 'partido']) }} AS sk_deputado_historico,
    UPPER(TRIM(nome)) AS nome_deputado,
    UPPER(TRIM(partido)) AS sigla_partido,
    UPPER(TRIM(uf)) AS sigla_uf,
    url_foto,
    legislatura_id,
    is_current,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'deputados_historico') }}
WHERE id IS NOT NULL
  AND legislatura_id IS NOT NULL
