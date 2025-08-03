{{
  config(
    materialized='table',
    unique_key='sk_despesa'
  )
}}

-- Fato Despesas: registro único por deputado, mês, documento
WITH despesas_base AS (
    SELECT 
        sd.deputado_id,
        sd.cod_documento,
        sd.ano,
        sd.mes,
        sd.cnpj_cpf_fornecedor,
        sd.cod_lote,
        sd.cod_tipo_documento,
        sd.data_documento,
        sd.nome_fornecedor,
        sd.num_documento,
        sd.num_ressarcimento,
        sd.parcela,
        sd.tipo_despesa,
        sd.tipo_documento,
        sd.url_documento,
        sd.valor_documento,
        sd.valor_liquido,
        sd.valor_glosa,
        sd.data_carga
    FROM {{ ref('stg_despesas') }} sd
    WHERE sd.deputado_id IS NOT NULL
      AND sd.cod_documento IS NOT NULL
      AND sd.data_documento IS NOT NULL

    {% if is_incremental() %}
        AND sd.ano >= (SELECT MAX(ano) FROM {{ this }})
    {% endif %}
),

despesas_com_chaves AS (
    SELECT 
        -- Surrogate Key único
        {{ dbt_utils.generate_surrogate_key([
            'deputado_id',
            'cod_documento',
            'ano',
            'mes'
        ]) }} AS sk_despesa,
        
        -- Chaves estrangeiras para dimensões
        dd.sk_deputado,
        
        -- Chave natural
        deputado_id AS nk_deputado,
        cod_documento,
        
        -- Atributos da despesa
        ano,
        mes,
        data_documento,
        num_documento,
        num_ressarcimento,
        parcela,
        tipo_despesa,
        tipo_documento,
        url_documento,
        
        -- Fornecedor
        cnpj_cpf_fornecedor,
        nome_fornecedor,
        cod_lote,
        cod_tipo_documento,
        
        -- Valores financeiros
        valor_documento,
        valor_liquido,
        valor_glosa,
        
        -- Métricas derivadas
        CASE 
            WHEN valor_documento > 0 
            THEN (valor_glosa / valor_documento) * 100
            ELSE 0 
        END AS percentual_glosa,
        
        CASE 
            WHEN valor_liquido > valor_documento THEN 'INCONSISTENTE'
            WHEN valor_glosa > valor_documento THEN 'GLOSA_ALTA'
            WHEN valor_glosa = 0 THEN 'SEM_GLOSA'
            ELSE 'NORMAL'
        END AS status_financeiro,
        
        -- Categorização de valores
        CASE 
            WHEN valor_liquido <= 100 THEN 'BAIXO'
            WHEN valor_liquido <= 1000 THEN 'MEDIO'
            WHEN valor_liquido <= 5000 THEN 'ALTO'
            ELSE 'MUITO_ALTO'
        END AS faixa_valor,
        
        -- Data e período
        CONCAT(ano, '-', LPAD(mes::STRING, 2, '0')) AS ano_mes,
        CASE 
            WHEN mes IN (1,2,3) THEN 'T1'
            WHEN mes IN (4,5,6) THEN 'T2'
            WHEN mes IN (7,8,9) THEN 'T3'
            ELSE 'T4'
        END AS trimestre,
        
        -- Flags de controle
        CASE 
            WHEN url_documento IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS tem_documento,
        
        CASE 
            WHEN num_ressarcimento IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS tem_ressarcimento,
          -- Metadados
        db.data_carga,
        CURRENT_TIMESTAMP() AS data_atualizacao
          FROM despesas_base db
    LEFT JOIN {{ ref('dim_deputados') }} dd 
        ON db.deputado_id = dd.nk_deputado
        AND db.data_documento BETWEEN dd.data_inicio_vigencia AND COALESCE(dd.data_fim_vigencia, '9999-12-31'::DATE)
)

SELECT 
    sk_despesa,
    sk_deputado,
    nk_deputado,
    cod_documento,
    ano,
    mes,
    ano_mes,
    trimestre,
    data_documento,
    num_documento,
    num_ressarcimento,
    parcela,
    tipo_despesa,
    tipo_documento,
    url_documento,
    cnpj_cpf_fornecedor,
    nome_fornecedor,
    cod_lote,
    cod_tipo_documento,
    valor_documento,
    valor_liquido,
    valor_glosa,
    percentual_glosa,
    status_financeiro,
    faixa_valor,
    tem_documento,
    tem_ressarcimento,
    data_carga,
    data_atualizacao
FROM despesas_com_chaves
WHERE sk_deputado IS NOT NULL  -- Garantir que temos a dimensão deputado
ORDER BY ano DESC, mes DESC, valor_liquido DESC
