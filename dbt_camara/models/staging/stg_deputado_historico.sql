{{ config(
    materialized='view'
) }}

WITH historico_raw AS (
    SELECT 
        CAST(deputado_id AS INTEGER) AS deputado_id,
        data_hora AS data_inicio_vigencia,
        UPPER(TRIM(condicao_eleitoral)) AS condicao_eleitoral,
        UPPER(TRIM(descricao_status)) AS descricao_status,
        id_legislatura AS id_legislatura,
        UPPER(TRIM(nome)) AS nome_deputado,
        UPPER(TRIM(nome_eleitoral)) AS nome_eleitoral,
        UPPER(TRIM(sigla_partido)) AS sigla_partido,
        UPPER(TRIM(sigla_uf)) AS sigla_uf,
        UPPER(TRIM(situacao)) AS situacao,
        uri,
        uri_partido AS uri_partido,
        url_foto AS url_foto,
        CURRENT_TIMESTAMP() AS data_carga
    FROM {{ source('camara_raw', 'deputado_historico') }}
    WHERE deputado_id IS NOT NULL
      AND data_hora IS NOT NULL
      AND data_hora > '1980-01-01' -- Filtrar dados muito antigos
),

historico_dedup AS (
    SELECT 
        *,
        -- Ranking para remover duplicados: manter o registro com maior historico_id
        ROW_NUMBER() OVER (
            PARTITION BY deputado_id, data_inicio_vigencia 
            ORDER BY deputado_id DESC, data_carga DESC
        ) AS rn
    FROM historico_raw
)

SELECT 
    deputado_id,
    data_inicio_vigencia,
    condicao_eleitoral,
    descricao_status,
    id_legislatura,
    nome_deputado,
    nome_eleitoral,
    sigla_partido,
    sigla_uf,
    situacao,
    uri,
    uri_partido,
    url_foto,
    data_carga
FROM historico_dedup
WHERE rn = 1 -- Manter apenas o primeiro registro (mais recente) por deputado/data
