USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

-- 1. Cria tabela final, se não existir
CREATE OR REPLACE TABLE raw.eventos (
    id NUMBER,
    descricao STRING,
    descricao_tipo STRING,
    data_hora_inicio TIMESTAMP_NTZ,
    data_hora_fim TIMESTAMP_NTZ,
    local_camara_nome STRING,
    local_externo STRING,
    situacao STRING,
    uri STRING,
    url_documento_pauta STRING
);

-- 2. Insere dados diretamente do Stage
INSERT INTO raw.eventos (
    id, descricao, descricao_tipo, data_hora_inicio, data_hora_fim, local_camara_nome, 
    local_externo, situacao, uri, url_documento_pauta
)
SELECT
    $1:id::NUMBER,
    $1:descricao::STRING,
    $1:descricaoTipo::STRING,
    TRY_TO_TIMESTAMP($1:dataHoraInicio::STRING),
    TRY_TO_TIMESTAMP($1:dataHoraFim::STRING),
    $1:localCamara_nome::STRING,
    $1:localExterno::STRING,
    $1:situacao::STRING,
    $1:uri::STRING,
    $1:urlDocumentoPauta::STRING
FROM @camara/porposicoes_tramitacoes/parquet/;
