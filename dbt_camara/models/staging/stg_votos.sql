{{ config(
    materialized='view'
) }}

SELECT 
    TRIM(id_votacao) AS id_votacao,
    deputado_id,
    UPPER(TRIM(tipo_voto)) AS tipo_voto,
    data_registro_voto,
    EXTRACT(YEAR FROM data_registro_voto) AS ano,
    EXTRACT(MONTH FROM data_registro_voto) AS mes,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'votos') }}
WHERE id_votacao IS NOT NULL 
  AND deputado_id IS NOT NULL
  AND tipo_voto IS NOT NULL
  AND data_registro_voto IS NOT NULL
  AND ano >= 2000
