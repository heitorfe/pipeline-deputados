-- 1. Contexto
USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

CREATE OR REPLACE TABLE raw.despesas (
    deputado_id INT,
    ano INT,
    cnpj_cpf_fornecedor STRING,
    cod_documento NUMBER,
    cod_lote NUMBER,
    cod_tipo_documento NUMBER,
    data_documento TIMESTAMP_NTZ,
    mes INT,
    nome_fornecedor STRING,
    num_documento STRING,
    num_ressarcimento STRING,
    parcela NUMBER,
    tipo_despesa STRING,
    tipo_documento STRING,
    url_documento STRING,
    valor_documento FLOAT,
    valor_glosa FLOAT,
    valor_liquido FLOAT
);

INSERT INTO raw.despesas (
    deputado_id, ano, cnpj_cpf_fornecedor, cod_documento, cod_lote, cod_tipo_documento, data_documento,
    mes, nome_fornecedor, num_documento, num_ressarcimento, parcela, tipo_despesa,
    tipo_documento, url_documento, valor_documento, valor_glosa, valor_liquido
)
SELECT
    $1:deputado_id::INT,
    $1:ano::INT,
    $1:cnpjCpfFornecedor::STRING,
    $1:codDocumento::NUMBER,
    $1:codLote::NUMBER,
    $1:codTipoDocumento::NUMBER,
    TRY_TO_TIMESTAMP($1:dataDocumento::STRING),
    $1:mes::INT,
    $1:nomeFornecedor::STRING,
    $1:numDocumento::STRING,
    $1:numRessarcimento::STRING,
    $1:parcela::NUMBER,
    $1:tipoDespesa::STRING,
    $1:tipoDocumento::STRING,
    $1:urlDocumento::STRING,
    $1:valorDocumento::FLOAT,
    $1:valorGlosa::FLOAT,
    $1:valorLiquido::FLOAT
FROM @camara/despesas/parquet/;