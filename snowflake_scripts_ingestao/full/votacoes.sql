-- 1. Contexto
USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

SELECT $1
FROM @camara/proposicoes/parquet
(FILE_FORMAT => (TYPE => PARQUET))
LIMIT 5;

CREATE OR REPLACE TABLE raw.votacoes (
    id STRING,
    data DATE,
    data_hora_registro TIMESTAMP_NTZ,
    descricao STRING,
    aprovacao FLOAT,
    id_orgao NUMBER,
    sigla_orgao STRING,
    efeitos_registrados STRING,
    proposicoes_afetadas STRING,
    ultima_apresentacao_proposicao STRING,
    uri STRING,
    uri_orgao STRING
);

INSERT INTO raw.votacoes (
    id, data, data_hora_registro, descricao, aprovacao, id_orgao, sigla_orgao,
    efeitos_registrados, proposicoes_afetadas, ultima_apresentacao_proposicao,
    uri, uri_orgao
)
SELECT
    $1:id::STRING,
    TRY_TO_DATE($1:data::STRING),
    TRY_TO_TIMESTAMP($1:dataHoraRegistro::STRING),
    $1:descricao::STRING,
    $1:aprovacao::FLOAT,
    $1:idOrgao::NUMBER,
    $1:siglaOrgao::STRING,
    TO_JSON($1:efeitosRegistrados),
    TO_JSON($1:proposicoesAfetadas),
    $1:ultimaApresentacaoProposicao::STRING,
    $1:uri::STRING,
    $1:uriOrgao::STRING
FROM @camara/votacoes/parquet/
(FILE_FORMAT => 'parquet_format');