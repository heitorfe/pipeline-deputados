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
    valor_liquido FLOAT,
    num_subcota INT,
    num_especificacao_subcota INT
);

INSERT INTO raw.despesas (
    deputado_id, ano, cnpj_cpf_fornecedor, cod_documento, cod_lote, cod_tipo_documento, data_documento,
    mes, nome_fornecedor, num_documento, num_ressarcimento, parcela, tipo_despesa,
    tipo_documento, url_documento, valor_documento, valor_glosa, valor_liquido, num_subcota, num_especificacao_subcota
)
SELECT
    $1:nuDeputadoId::INT,
    $1:numAno::INT,
    $1:txtCNPJCPF::STRING,
    $1:ideDocumento::NUMBER,
    $1:numLote::NUMBER,
    $1:indTipoDocumento::NUMBER,
    TRY_TO_TIMESTAMP($1:datEmissao::STRING),
    $1:numMes::INT,
    $1:txtFornecedor::STRING,
    $1:txtNumero::STRING,
    $1:numRessarcimento::STRING,
    $1:numParcela::NUMBER,
    $1:txtDescricao::STRING,
    $1:indTipoDocumento::STRING,
    $1:url_documento::STRING,
    $1:vlrDocumento::FLOAT,
    $1:vlrGlosa::FLOAT,
    $1:vlrLiquido::FLOAT,
    $1:numSubCota::INT,
    $1:numEspecificacaoSubCota::INT
FROM @camara/despesas/parquet/
(FILE_FORMAT => 'parquet_format');
