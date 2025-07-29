USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

-- 1. Cria a tabela final
CREATE OR REPLACE TABLE  raw.votacoes_orientacoes (
    id_votacao STRING,
    descricao STRING,
    orientacao STRING,
    sigla_bancada STRING,
    sigla_orgao STRING,
    uri_bancada STRING,
    uri_votacao STRING
);

-- 2. Insere os dados diretamente do Stage
INSERT INTO raw.votacoes_orientacoes (
    id_votacao, descricao, orientacao, sigla_bancada, sigla_orgao, uri_bancada, uri_votacao
)
SELECT
    $1:idVotacao::STRING,
    $1:descricao::STRING,
    $1:orientacao::STRING,
    $1:siglaBancada::STRING,
    $1:siglaOrgao::STRING,
    $1:uriBancada::STRING,
    $1:uriVotacao::STRING
FROM @camara/votacoes-orientacoes/parquet;
