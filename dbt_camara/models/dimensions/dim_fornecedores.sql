{{ config(
    materialized='table'
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['cnpj_fornecedor']) }} AS sk_fornecedor,
    cnpj_fornecedor AS nk_fornecedor,
    nome_fornecedor,
    MIN(data_carga) AS data_carga,
    CURRENT_TIMESTAMP() AS data_atualizacao
FROM {{ ref('stg_despesas') }}
WHERE cnpj_fornecedor IS NOT NULL
GROUP BY cnpj_fornecedor, nome_fornecedor
