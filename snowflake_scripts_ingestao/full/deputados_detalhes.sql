-- 1. Contexto
USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

CREATE OR REPLACE TABLE raw.deputados_detalhes (
    id NUMBER,
    cpf STRING,
    nome_civil STRING,
    data_nascimento DATE,
    municipio_nascimento STRING,
    uf_nascimento STRING,
    sexo STRING,
    redes_sociais STRING,
    ultimo_status_data DATE,
    ultimo_status_nome STRING,
    ultimo_status_nome_eleitoral STRING,
    ultimo_status_sigla_partido STRING,
    ultimo_status_sigla_uf STRING,
    ultimo_status_id_legislatura NUMBER,
    ultimo_status_condicao_eleitoral STRING,
    ultimo_status_situacao STRING,
    ultimo_status_url_foto STRING,
    ultimo_status_email_gabinete STRING,
    ultimo_status_telefone_gabinete STRING,
    ultimo_status_nome_gabinete STRING,
    uri STRING
);

INSERT INTO raw.deputados_detalhes
SELECT
    $1:id::NUMBER,
    $1:cpf::STRING,
    $1:nomeCivil::STRING,
    TRY_TO_DATE($1:dataNascimento::STRING),
    $1:municipioNascimento::STRING,
    $1:ufNascimento::STRING,
    $1:sexo::STRING,
    ARRAY_TO_STRING($1:redeSocial, ', '),
    TRY_TO_DATE($1:ultimoStatus:data::STRING),
    $1:ultimoStatus:nome::STRING,
    $1:ultimoStatus:nomeEleitoral::STRING,
    $1:ultimoStatus:siglaPartido::STRING,
    $1:ultimoStatus:siglaUf::STRING,
    $1:ultimoStatus:idLegislatura::NUMBER,
    $1:ultimoStatus:condicaoEleitoral::STRING,
    $1:ultimoStatus:situacao::STRING,
    $1:ultimoStatus:urlFoto::STRING,
    $1:ultimoStatus:gabinete:email::STRING,
    $1:ultimoStatus:gabinete:telefone::STRING,
    $1:ultimoStatus:gabinete:nome::STRING,
    $1:uri::STRING
FROM @camara/deputados/parquet/deputados_detail.parquet;