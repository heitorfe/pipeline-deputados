{{
  config(
    materialized='table',
    unique_key='sk_deputado'
  )
}}

-- Dimensão Deputados baseada no snapshot SCD Type 2
WITH deputados_snapshot AS (
    SELECT 
        -- Dados do snapshot com SCD Type 2 automático
        deputado_id AS nk_deputado,
        cpf,
        nome_civil,
        data_nascimento,
        municipio_nascimento,
        uf_nascimento,
        sexo,
        sexo_descricao,
        idade_atual,
        redes_sociais,
        ultimo_status_data,
        nome_deputado,
        nome_eleitoral,
        sigla_partido,
        sigla_uf,
        id_legislatura,
        condicao_eleitoral,
        situacao,
        url_foto,
        email_gabinete,
        telefone_gabinete,
        nome_gabinete,
        uri,
        fonte_origem,
        
        -- Campos de controle SCD Type 2 do snapshot
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        
        data_carga,
        data_atualizacao
    FROM {{ ref('snapshot_deputados') }}
),

deputados_com_sk AS (
    SELECT 
        -- Surrogate Key baseado no dbt_scd_id do snapshot
        {{ dbt_utils.generate_surrogate_key([
            'dbt_scd_id'
        ]) }} AS sk_deputado,
        
        -- Natural Key
        nk_deputado,
        
        -- Atributos do deputado (SCD Type 1)
        nome_deputado,
        nome_eleitoral,
        nome_civil,
        cpf,
        data_nascimento,
        municipio_nascimento,
        uf_nascimento,
        sexo,
        sexo_descricao,
        idade_atual,
        redes_sociais,
        
        -- Atributos que mudam (SCD Type 2) - já controlados pelo snapshot
        sigla_partido,
        sigla_uf,
        id_legislatura,
        condicao_eleitoral,
        situacao,
        
        -- Contatos e informações adicionais
        url_foto,
        email_gabinete,
        telefone_gabinete,
        nome_gabinete,
        uri,
        fonte_origem,
        
        -- Metadados SCD Type 2 do snapshot
        dbt_valid_from AS data_inicio_vigencia,
        dbt_valid_to AS data_fim_vigencia,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        
        -- IDs internos do snapshot
        dbt_scd_id,
        dbt_updated_at,
        
        -- Flags e métricas derivadas adicionais
        CASE 
            WHEN situacao ILIKE '%EXERC%' THEN TRUE
            ELSE FALSE
        END AS em_exercicio,
        
        CASE 
            WHEN condicao_eleitoral = 'TITULAR' THEN TRUE
            ELSE FALSE
        END AS eh_titular,
        
        CASE 
            WHEN url_foto IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS tem_foto,
        
        CASE 
            WHEN email_gabinete IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS tem_email_gabinete,
        
        -- Metadados
        data_carga,
        data_atualizacao,
        CURRENT_TIMESTAMP() AS data_processamento

    FROM deputados_snapshot
)

SELECT 
    sk_deputado,
    nk_deputado,
    nome_deputado,
    nome_eleitoral,
    nome_civil,
    cpf,
    data_nascimento,
    municipio_nascimento,
    uf_nascimento,
    sexo,
    sexo_descricao,
    idade_atual,
    redes_sociais,
    sigla_partido,
    sigla_uf,
    id_legislatura,
    condicao_eleitoral,
    situacao,
    url_foto,
    email_gabinete,
    telefone_gabinete,
    nome_gabinete,
    uri,
    fonte_origem,
    data_inicio_vigencia,
    data_fim_vigencia,
    is_current,
    em_exercicio,
    eh_titular,
    tem_foto,
    tem_email_gabinete,
    dbt_scd_id,
    dbt_updated_at,
    data_carga,
    data_atualizacao,
    data_processamento
FROM deputados_com_sk
ORDER BY nk_deputado, data_inicio_vigencia DESC