{{ config(
    materialized='view'
) }}

SELECT 
    id AS deputado_id,
    UPPER(TRIM(cpf)) AS cpf,
    UPPER(TRIM(nome_civil)) AS nome_civil,
    data_nascimento,
    UPPER(TRIM(municipio_nascimento)) AS municipio_nascimento,
    UPPER(TRIM(uf_nascimento)) AS uf_nascimento,
    UPPER(TRIM(sexo)) AS sexo,
    redes_sociais,
    ultimo_status_data,
    UPPER(TRIM(ultimo_status_nome)) AS ultimo_status_nome,
    UPPER(TRIM(ultimo_status_nome_eleitoral)) AS ultimo_status_nome_eleitoral,
    UPPER(TRIM(ultimo_status_sigla_partido)) AS ultimo_status_sigla_partido,
    UPPER(TRIM(ultimo_status_sigla_uf)) AS ultimo_status_sigla_uf,
    ultimo_status_id_legislatura,
    UPPER(TRIM(ultimo_status_condicao_eleitoral)) AS ultimo_status_condicao_eleitoral,
    UPPER(TRIM(ultimo_status_situacao)) AS ultimo_status_situacao,
    ultimo_status_url_foto,
    ultimo_status_email_gabinete,
    ultimo_status_telefone_gabinete,
    UPPER(TRIM(ultimo_status_nome_gabinete)) AS ultimo_status_nome_gabinete,
    uri,
    CURRENT_TIMESTAMP() AS data_carga
FROM {{ source('camara_raw', 'deputados_detalhes') }}
WHERE id IS NOT NULL
