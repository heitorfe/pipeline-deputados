USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

-- 1. Cria tabela final, se não existir
CREATE OR REPLACE TABLE raw.licitacoes (
 id_licitacao NUMBER,
    ano INT,
    ano_processo INT,
    numero NUMBER,
    num_processo NUMBER,
    data_autorizacao DATE,
    data_publicacao DATE,
    modalidade STRING,
    tipo STRING,
    situacao STRING,
    objeto STRING,
    num_contratos NUMBER,
    num_itens NUMBER,
    num_propostas NUMBER,
    num_unidades NUMBER,
    vlr_estimado FLOAT,
    vlr_contratado FLOAT,
    vlr_pago FLOAT
);

-- 2. Insere dados diretamente do Stage
INSERT INTO raw.licitacoes (
    id_licitacao, ano, ano_processo, numero, num_processo,
    data_autorizacao, data_publicacao, modalidade, tipo, situacao, objeto,
    num_contratos, num_itens, num_propostas, num_unidades,
    vlr_estimado, vlr_contratado, vlr_pago
)
SELECT
    $1:idLicitacao::NUMBER,
    $1:ano::INT,
    $1:anoProcesso::INT,
    $1:numero::NUMBER,
    $1:numProcesso::NUMBER,
    TRY_TO_DATE($1:dataAutorizacao::STRING),
    TRY_TO_DATE($1:dataPublicacao::STRING),
    $1:modalidade::STRING,
    $1:tipo::STRING,
    $1:situacao::STRING,
    $1:objeto::STRING,
    $1:numContratos::NUMBER,
    $1:numItens::NUMBER,
    $1:numPropostas::NUMBER,
    $1:numUnidades::NUMBER,
    $1:vlrEstimado::FLOAT,
    $1:vlrContratado::FLOAT,
    $1:vlrPago::FLOAT
FROM @camara/licitacoes/parquet/;