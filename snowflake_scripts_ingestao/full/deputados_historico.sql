-- 1. Contexto
USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

CREATE OR REPLACE TABLE raw.deputado_historico (
    id NUMBER,
    deputado_id STRING,
    nome STRING,
    nome_eleitoral STRING,
    sigla_partido STRING,
    sigla_uf STRING,
    id_legislatura NUMBER,
    situacao STRING,
    condicao_eleitoral STRING,
    descricao_status STRING,
    data_hora TIMESTAMP_NTZ,
    uri STRING,
    uri_partido STRING,
    url_foto STRING
);

INSERT INTO raw.deputado_historico (
    id, deputado_id, nome, nome_eleitoral, sigla_partido, sigla_uf,
    id_legislatura, situacao, condicao_eleitoral, descricao_status, data_hora,
    uri, uri_partido, url_foto
)
SELECT
    $1:id::NUMBER,
    $1:deputado_id::STRING,
    $1:nome::STRING,
    $1:nomeEleitoral::STRING,
    $1:siglaPartido::STRING,
    $1:siglaUf::STRING,
    $1:idLegislatura::NUMBER,
    $1:situacao::STRING,
    $1:condicaoEleitoral::STRING,
    $1:descricaoStatus::STRING,
    TRY_TO_TIMESTAMP($1:dataHora::STRING),
    $1:uri::STRING,
    $1:uriPartido::STRING,
    $1:urlFoto::STRING
FROM @camara/deputados/parquet/historicos-deputados.parquet;