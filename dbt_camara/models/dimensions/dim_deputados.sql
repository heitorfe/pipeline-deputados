{{ config(
    materialized='table'
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['deputado_id']) }} AS sk_deputado,
    deputado_id AS nk_deputado,
    nome_deputado,
    sigla_partido,
    sigla_uf,
    url_foto,
    data_carga,
    CURRENT_TIMESTAMP() AS data_atualizacao
FROM {{ ref('stg_deputados') }}
