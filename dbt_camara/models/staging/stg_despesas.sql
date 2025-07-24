{{ config(
    materialized='view'
) }}

SELECT DISTINCT
    deputado_id,
    cod_documento,
    ano,
    mes,
    TRIM(cnpj_cpf_fornecedor) AS cnpj_cpf_fornecedor,
    cod_lote,
    cod_tipo_documento,
    data_documento,
    UPPER(TRIM(nome_fornecedor)) AS nome_fornecedor,
    TRIM(num_documento) AS num_documento,
    num_ressarcimento,
    parcela,
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
  AND data_documento IS NOT NULL
  AND valor_documento >= 0
