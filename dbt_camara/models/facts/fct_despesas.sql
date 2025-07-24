{{ config(
    materialized='table'
) }}

SELECT 
    d.cod_documento,
    dd.sk_deputado,
    df.sk_fornecedor,
    dt.sk_tempo,
    dtd.sk_tipo_despesa,
    d.cod_lote,
    d.cod_tipo_documento,
    d.data_documento,
    d.num_documento,
    d.num_ressarcimento,
    d.parcela,
    d.tipo_documento,
    d.url_documento,
    d.valor_documento,
    d.valor_liquido,
    d.valor_glosa,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ ref('stg_despesas') }} d
INNER JOIN {{ ref('dim_deputados') }} dd 
    ON d.deputado_id = dd.nk_deputado
    AND d.data_documento >= dd.data_inicio_vigencia
    AND d.data_documento < dd.data_fim_vigencia
LEFT JOIN {{ ref('dim_fornecedores') }} df 
    ON d.nome_fornecedor = df.nome_fornecedor
INNER JOIN {{ ref('dim_tempo') }} dt 
    ON {{ dbt_utils.generate_surrogate_key(['d.ano', 'd.mes']) }} = dt.sk_tempo
LEFT JOIN {{ ref('dim_tipo_despesa') }} dtd 
    ON {{ dbt_utils.generate_surrogate_key(['d.tipo_despesa']) }} = dtd.sk_tipo_despesa
