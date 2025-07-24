{{ config(
    materialized='table'
) }}

WITH fornecedor_stats AS (
    SELECT 
        cnpj_cpf_fornecedor,
        nome_fornecedor,
        COUNT(*) as name_frequency,
        LENGTH(nome_fornecedor) as name_length,
        MIN(data_carga) as data_carga
    FROM {{ ref('stg_despesas') }}
    WHERE cnpj_cpf_fornecedor IS NOT NULL
    GROUP BY cnpj_cpf_fornecedor, nome_fornecedor
),
ranked_names AS (
    SELECT 
        cnpj_cpf_fornecedor,
        nome_fornecedor,
        data_carga,
        ROW_NUMBER() OVER (
            PARTITION BY cnpj_cpf_fornecedor 
            ORDER BY name_frequency DESC, name_length ASC
        ) as rn
    FROM fornecedor_stats
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['cnpj_cpf_fornecedor']) }} AS sk_fornecedor,
    cnpj_cpf_fornecedor AS nk_fornecedor,
    nome_fornecedor,
    data_carga,
    CURRENT_TIMESTAMP() AS data_atualizacao
FROM ranked_names
WHERE rn = 1
