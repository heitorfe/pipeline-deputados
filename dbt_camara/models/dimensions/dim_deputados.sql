{{ config(
    materialized='table'
) }}

SELECT 
    sk_deputado_historico AS sk_deputado,
    deputado_id AS nk_deputado,
    nome_deputado,
    sigla_partido,
    sigla_uf,
    url_foto,
    legislatura_id,
    is_current,
    data_carga,
    CURRENT_TIMESTAMP() AS data_atualizacao
FROM {{ ref('stg_deputados') }}
