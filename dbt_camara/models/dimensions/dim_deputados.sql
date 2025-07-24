{{ config(
    materialized='table'
) }}

WITH historico_com_fim AS (
    SELECT 
        h.*,
        LEAD(h.data_inicio_vigencia) OVER (
            PARTITION BY h.deputado_id 
            ORDER BY h.data_inicio_vigencia
        ) AS data_fim_vigencia_calc
    FROM {{ ref('stg_deputado_historico') }} h
),

deputados_scd AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['h.deputado_id', 'h.data_inicio_vigencia']) }} AS sk_deputado,
        h.deputado_id AS nk_deputado,
        COALESCE(h.nome_deputado, d.nome_civil) AS nome_deputado,
        COALESCE(h.nome_eleitoral, h.nome_deputado, d.nome_civil) AS nome_eleitoral,
        h.sigla_partido,
        h.sigla_uf,
        h.id_legislatura,
        h.condicao_eleitoral,
        h.situacao,
        h.url_foto,
        -- Dados pessoais (SCD Type 1)
        d.cpf,
        d.nome_civil,
        d.data_nascimento,
        d.municipio_nascimento,
        d.uf_nascimento,
        d.sexo,
        -- SCD Type 2 fields
        h.data_inicio_vigencia,
        CASE 
            WHEN data_fim_vigencia_calc IS NOT NULL 
            THEN DATEADD('second', -1, data_fim_vigencia_calc)
            ELSE '9999-12-31'::DATE
        END AS data_fim_vigencia,
        CASE WHEN data_fim_vigencia_calc IS NULL THEN TRUE ELSE FALSE END AS is_current,
        h.data_carga,
        CURRENT_TIMESTAMP() AS data_atualizacao
    FROM historico_com_fim h
    LEFT JOIN {{ ref('stg_deputados_detalhes') }} d 
        ON h.deputado_id = d.deputado_id
)

SELECT * FROM deputados_scd