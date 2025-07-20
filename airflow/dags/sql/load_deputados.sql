-- Set defaults
-- USE WAREHOUSE COMPUTE_WH;
-- USE DATABASE CAMARA;
-- USE SCHEMA RAW;

-- COPY INTO deputados
COPY INTO CAMARA.RAW.DEPUTADOS_STAGE
FROM @camara/deputados/deputados.json
FILE_FORMAT = (TYPE = JSON);


CREATE OR REPLACE TABLE CAMARA.RAW.DEPUTADOS AS
SELECT
    elemento.value:id::NUMBER        AS id,
    elemento.value:nome::STRING      AS nome,
    elemento.value:siglaPartido::STRING AS partido,
    elemento.value:siglaUf::STRING   AS uf,
    elemento.value:urlFoto::STRING   AS url_foto
FROM CAMARA.RAW.DEPUTADOS_STAGE,
LATERAL FLATTEN(INPUT => DATA:dados) elemento;


COPY INTO CAMARA.RAW.DESPESAS_STAGE
FROM @camara/despesas
FILE_FORMAT = (TYPE = JSON);


MERGE INTO CAMARA.RAW.DESPESAS AS target
USING (
    SELECT
        REGEXP_SUBSTR(METADATA$FILENAME, 'deputado=([0-9]+)', 1, 1, 'e', 1) AS deputado_id,
        TRY_TO_NUMBER(elemento.value:codDocumento::STRING) AS cod_documento,
        TRY_TO_NUMBER(elemento.value:ano::STRING) AS ano,
        TRY_TO_NUMBER(elemento.value:mes::STRING) AS mes,
        elemento.value:cnpjCpfFornecedor::STRING AS cnpj_fornecedor,
        TRY_TO_NUMBER(elemento.value:codLote::STRING) AS cod_lote,
        TO_DATE(LEFT(elemento.value:dataDocumento::STRING, 10), 'YYYY-MM-DD') AS data_documento,
        elemento.value:nomeFornecedor::STRING AS nome_fornecedor,
        elemento.value:numDocumento::STRING AS num_documento, -- MANTIDO COMO STRING
        elemento.value:tipoDespesa::STRING AS tipo_despesa,
        elemento.value:tipoDocumento::STRING AS tipo_documento,
        elemento.value:urlDocumento::STRING AS url_documento,
        TRY_TO_NUMBER(elemento.value:valorDocumento::STRING) AS valor_documento,
        TRY_TO_NUMBER(elemento.value:valorLiquido::STRING) AS valor_liquido,
        TRY_TO_NUMBER(elemento.value:valorGlosa::STRING) AS valor_glosa,
        ROW_NUMBER() OVER (
            PARTITION BY elemento.value:codDocumento::STRING
            ORDER BY elemento.value:dataDocumento DESC
        ) AS rn
    FROM @camara/despesas/ (FILE_FORMAT => json_format),
    LATERAL FLATTEN(INPUT => PARSE_JSON($1):dados) elemento
) src
ON target.cod_documento = src.cod_documento
WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
    deputado_id      = src.deputado_id,
    ano              = src.ano,
    mes              = src.mes,
    cnpj_fornecedor  = src.cnpj_fornecedor,
    cod_lote         = src.cod_lote,
    data_documento   = src.data_documento,
    nome_fornecedor  = src.nome_fornecedor,
    num_documento    = src.num_documento,
    tipo_despesa     = src.tipo_despesa,
    tipo_documento   = src.tipo_documento,
    url_documento    = src.url_documento,
    valor_documento  = src.valor_documento,
    valor_liquido    = src.valor_liquido,
    valor_glosa      = src.valor_glosa
WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
    deputado_id, cod_documento, ano, mes, cnpj_fornecedor, cod_lote, data_documento,
    nome_fornecedor, num_documento, tipo_despesa, tipo_documento, url_documento,
    valor_documento, valor_liquido, valor_glosa
) VALUES (
    src.deputado_id, src.cod_documento, src.ano, src.mes, src.cnpj_fornecedor, src.cod_lote,
    src.data_documento, src.nome_fornecedor, src.num_documento, src.tipo_despesa, src.tipo_documento,
    src.url_documento, src.valor_documento, src.valor_liquido, src.valor_glosa
);
