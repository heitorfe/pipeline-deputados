{{ config(
    materialized='table'
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['tipo_despesa']) }} AS sk_tipo_despesa,
    tipo_despesa,
    CURRENT_TIMESTAMP() AS data_carga
FROM (
    SELECT DISTINCT tipo_despesa 
    FROM {{ ref('stg_despesas') }}
    WHERE tipo_despesa IS NOT NULL
) t
