USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

-- 1. Cria a tabela final, se não existir
CREATE OR REPLACE TABLE raw.votacoes_objetos (
    id_votacao STRING,
    data DATE,
    descricao STRING,
    proposicao_id NUMBER,
    proposicao_sigla_tipo STRING,
    proposicao_numero NUMBER,
    proposicao_ano INT,
    proposicao_cod_tipo NUMBER,
    proposicao_titulo STRING,
    proposicao_ementa STRING,
    proposicao_uri STRING,
    uri_votacao STRING
);

-- 2. Insere os dados diretamente do Stage
INSERT INTO raw.votacoes_objetos (
    id_votacao, data, descricao,
    proposicao_id, proposicao_sigla_tipo, proposicao_numero, proposicao_ano,
    proposicao_cod_tipo, proposicao_titulo, proposicao_ementa,
    proposicao_uri, uri_votacao
)
SELECT
    $1:idVotacao::STRING,
    TRY_TO_DATE($1:data::STRING),
    $1:descricao::STRING,
    $1:proposicao_id::NUMBER,
    $1:proposicao_siglaTipo::STRING,
    $1:proposicao_numero::NUMBER,
    $1:proposicao_ano::INT,
    $1:proposicao_codTipo::NUMBER,
    $1:proposicao_titulo::STRING,
    $1:proposicao_ementa::STRING,
    $1:proposicao_uri::STRING,
    $1:uriVotacao::STRING
FROM @camara/votacoes-objetos/parquet;
