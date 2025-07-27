USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

-- 1. Cria tabela final, se não existir
CREATE TABLE IF NOT EXISTS raw.proposicoes_tramitacoes (
    proposicao_id NUMBER,
    sequencia NUMBER,
    ambito STRING,
    apreciacao STRING,
    cod_tipo_tramitacao STRING,
    data_hora TIMESTAMP_NTZ,
    descricao_tramitacao STRING,
    despacho STRING,
    regime STRING,
    sigla_orgao STRING,
    uri_orgao STRING,
    url STRING
);

-- 2. Insere dados diretamente do Stage
INSERT INTO raw.proposicoes_tramitacoes (
    proposicao_id, sequencia, ambito, apreciacao, cod_tipo_tramitacao, data_hora,
    descricao_tramitacao, despacho, regime, sigla_orgao, uri_orgao, url
)
SELECT
    $1:proposicao_id::NUMBER,
    $1:sequencia::NUMBER,
    $1:ambito::STRING,
    $1:apreciacao::STRING,
    $1:codTipoTramitacao::STRING,
    TRY_TO_TIMESTAMP($1:dataHora::STRING),
    $1:descricaoTramitacao::STRING,
    $1:despacho::STRING,
    $1:regime::STRING,
    $1:siglaOrgao::STRING,
    $1:uriOrgao::STRING,
    $1:url::STRING
FROM @camara/proposicoes_tramitacoes/parquet/;
