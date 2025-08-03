{{ config(
    materialized='view'
) }}

-- Staging model para deputados combinando detalhes e histórico
WITH deputados_detalhes AS (
    SELECT 
        CAST(id AS INTEGER) AS deputado_id,
        UPPER(TRIM(cpf)) AS cpf,
        UPPER(TRIM(nome_civil)) AS nome_civil,
        CAST(data_nascimento AS DATE) AS data_nascimento,
        UPPER(TRIM(municipio_nascimento)) AS municipio_nascimento,
        UPPER(TRIM(uf_nascimento)) AS uf_nascimento,
        UPPER(TRIM(sexo)) AS sexo,
        TRIM(redes_sociais) AS redes_sociais,
        CAST(ultimo_status_data AS DATE) AS ultimo_status_data,
        UPPER(TRIM(ultimo_status_nome)) AS ultimo_status_nome,
        UPPER(TRIM(ultimo_status_nome_eleitoral)) AS ultimo_status_nome_eleitoral,
        UPPER(TRIM(ultimo_status_sigla_partido)) AS ultimo_status_sigla_partido,
        UPPER(TRIM(ultimo_status_sigla_uf)) AS ultimo_status_sigla_uf,
        CAST(ultimo_status_id_legislatura AS INTEGER) AS ultimo_status_id_legislatura,
        UPPER(TRIM(ultimo_status_condicao_eleitoral)) AS ultimo_status_condicao_eleitoral,
        UPPER(TRIM(ultimo_status_situacao)) AS ultimo_status_situacao,
        TRIM(ultimo_status_url_foto) AS ultimo_status_url_foto,
        TRIM(ultimo_status_email_gabinete) AS ultimo_status_email_gabinete,
        TRIM(ultimo_status_telefone_gabinete) AS ultimo_status_telefone_gabinete,
        TRIM(ultimo_status_nome_gabinete) AS ultimo_status_nome_gabinete,
        TRIM(uri) AS uri,
        CURRENT_TIMESTAMP() AS data_carga,
        'camara_deputados_detalhes' AS fonte_dado
    FROM {{ source('camara_raw', 'deputados_detalhes') }}
    WHERE id IS NOT NULL
      AND cpf IS NOT NULL
),

deputados_historico AS (
    SELECT DISTINCT
        CAST(id AS INTEGER) AS id_historico,
        CAST(deputado_id AS INTEGER) AS deputado_id,
        UPPER(TRIM(nome)) AS nome,
        UPPER(TRIM(nome_eleitoral)) AS nome_eleitoral,
        UPPER(TRIM(sigla_partido)) AS sigla_partido,
        UPPER(TRIM(sigla_uf)) AS sigla_uf,
        CAST(id_legislatura AS INTEGER) AS id_legislatura,
        UPPER(TRIM(situacao)) AS situacao,
        UPPER(TRIM(condicao_eleitoral)) AS condicao_eleitoral,
        TRIM(descricao_status) AS descricao_status,
        CAST(data_hora AS TIMESTAMP) AS data_hora,
        -- Normalizar data para formato YYYY-MM-DD
        CAST(DATE(data_hora) AS DATE) AS data_evento,
        EXTRACT(YEAR FROM data_hora) AS ano_evento,
        EXTRACT(MONTH FROM data_hora) AS mes_evento,
        TRIM(uri) AS uri_deputado,
        TRIM(uri_partido) AS uri_partido,
        TRIM(url_foto) AS url_foto,
        CURRENT_TIMESTAMP() AS data_carga,
        'camara_deputado_historico' AS fonte_dado
    FROM {{ source('camara_raw', 'deputado_historico') }}
    WHERE id IS NOT NULL
      AND deputado_id IS NOT NULL
      AND data_hora IS NOT NULL
      AND data_hora >= '2000-01-01'
)

-- Combinar as duas fontes
SELECT 
    dd.deputado_id,
    dd.cpf,
    dd.nome_civil,
    dd.data_nascimento,
    dd.municipio_nascimento,
    dd.uf_nascimento,
    dd.sexo,
    dd.redes_sociais,
    dd.ultimo_status_data,
    dd.ultimo_status_nome,
    dd.ultimo_status_nome_eleitoral,
    dd.ultimo_status_sigla_partido,
    dd.ultimo_status_sigla_uf,
    dd.ultimo_status_id_legislatura,
    dd.ultimo_status_condicao_eleitoral,
    dd.ultimo_status_situacao,
    dd.ultimo_status_url_foto,
    dd.ultimo_status_email_gabinete,
    dd.ultimo_status_telefone_gabinete,
    dd.ultimo_status_nome_gabinete,
    dd.uri,
    dd.data_carga,
    dd.fonte_dado
FROM deputados_detalhes dd

UNION ALL

-- Incluir registros históricos únicos que não estão nos detalhes
SELECT 
    dh.deputado_id,
    NULL AS cpf,
    dh.nome AS nome_civil,
    NULL AS data_nascimento,
    NULL AS municipio_nascimento,
    NULL AS uf_nascimento,
    NULL AS sexo,
    NULL AS redes_sociais,
    dh.data_evento AS ultimo_status_data,
    dh.nome AS ultimo_status_nome,
    dh.nome_eleitoral AS ultimo_status_nome_eleitoral,
    dh.sigla_partido AS ultimo_status_sigla_partido,
    dh.sigla_uf AS ultimo_status_sigla_uf,
    dh.id_legislatura AS ultimo_status_id_legislatura,
    dh.condicao_eleitoral AS ultimo_status_condicao_eleitoral,
    dh.situacao AS ultimo_status_situacao,
    dh.url_foto AS ultimo_status_url_foto,
    NULL AS ultimo_status_email_gabinete,
    NULL AS ultimo_status_telefone_gabinete,
    NULL AS ultimo_status_nome_gabinete,
    dh.uri_deputado AS uri,
    dh.data_carga,
    dh.fonte_dado
FROM deputados_historico dh
WHERE dh.deputado_id NOT IN (
    SELECT deputado_id FROM deputados_detalhes
)
