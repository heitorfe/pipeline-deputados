{{ config(
    materialized='view'
) }}

SELECT 
    deputado_id,
    cod_documento,
    ano,
    mes,
    TRIM(cnpj_fornecedor) AS cnpj_fornecedor,
    cod_lote,
    data_documento,
    UPPER(TRIM(nome_fornecedor)) AS nome_fornecedor,
    TRIM(num_documento) AS num_documento,
    UPPER(TRIM(tipo_despesa)) AS tipo_despesa,
    UPPER(TRIM(tipo_documento)) AS tipo_documento,
    url_documento,
    valor_documento,
    valor_liquido,
    valor_glosa,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'despesas') }}
WHERE deputado_id IS NOT NULL 
  AND cod_documento IS NOT NULL
  AND valor_documento >= 0
