USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

-- 1. Cria a tabela com a nova estrutura
CREATE OR REPLACE TABLE raw.votacoes (
    id STRING,
    data DATE,
    descricao STRING,
    aprovacao NUMBER,
    id_evento NUMBER,
    id_orgao NUMBER,
    sigla_orgao STRING,
    ultima_apresentacao_proposicao_descricao STRING,
    ultima_apresentacao_proposicao_id NUMBER,
    uri STRING,
    uri_evento STRING,
    uri_orgao STRING,
    votos_sim NUMBER,
    votos_nao NUMBER,
    votos_outros NUMBER
);

-- 2. Insere dados diretamente do Stage
INSERT INTO raw.votacoes (
    id, data, descricao, aprovacao, id_evento, id_orgao, sigla_orgao,
    ultima_apresentacao_proposicao_descricao, ultima_apresentacao_proposicao_id,
    uri, uri_evento, uri_orgao, votos_sim, votos_nao, votos_outros
)
SELECT
    $1:id::STRING,
    TRY_TO_DATE($1:data::STRING),
    $1:descricao::STRING,
    $1:aprovacao::NUMBER,
    $1:idEvento::NUMBER,
    $1:idOrgao::NUMBER,
    $1:siglaOrgao::STRING,
    $1:ultimaApresentacaoProposicao_descricao::STRING,
    $1:ultimaApresentacaoProposicao_idProposicao::NUMBER,
    $1:uri::STRING,
    $1:uriEvento::STRING,
    $1:uriOrgao::STRING,
    $1:votosSim::NUMBER,
    $1:votosNao::NUMBER,
    $1:votosOutros::NUMBER
FROM @camara/votacoes/parquet/;
