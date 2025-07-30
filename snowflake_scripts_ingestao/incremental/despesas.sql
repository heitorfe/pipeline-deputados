-- Conecte-se ao seu banco de dados e schema
USE DATABASE CAMARA;
USE SCHEMA raw;

USE ROLE ACCOUNTADMIN;  -- ou ACCOUNTADMIN
GRANT EXECUTE TASK 
  ON ACCOUNT 
  TO ROLE role_ingestao;
  
USE ROLE role_ingestao;

-- Cria a tabela que receberá os dados
CREATE OR REPLACE TABLE despesas_deputados_raw (
    ano INT,
    mes INT,
    tipoDespesa VARCHAR,
    codDocumento INT,
    tipoDocumento VARCHAR,
    codTipoDocumento INT,
    dataDocumento DATE,
    numDocumento VARCHAR,
    valorDocumento FLOAT,
    urlDocumento VARCHAR,
    nomeFornecedor VARCHAR,
    cnpjCpfFornecedor VARCHAR,
    valorLiquido FLOAT,
    valorGlosa FLOAT,
    numRessarcimento VARCHAR,
    codLote INT,
    parcela INT,
    deputado_id INT -- Coluna que você adicionou no script
);

-- as a user with ACCOUNTADMIN (or a custom role you've been granted)
USE ROLE ACCOUNTADMIN;

-- create (or replace) your integration
CREATE OR REPLACE STORAGE INTEGRATION s3_integration_camara
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxx:role/snowflake-listen'
  STORAGE_ALLOWED_LOCATIONS = ('s3://learnsnowflakedbt-heitor/camara/despesas/incremental/');

-- then grant usage on it to your regular role (if that role needs to reference it)
GRANT USAGE ON INTEGRATION s3_integration_camara TO ROLE role_ingestao;
USE ROLE role_ingestao;

-- Cria um formato de arquivo para Parquet
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET;
  
  -- Cria o stage externo que aponta para o S3
CREATE OR REPLACE STAGE stage_despesas_s3
  STORAGE_INTEGRATION = s3_integration_camara
  URL = 's3://learnsnowflakedbt-heitor/camara/despesas/incremental/'
  FILE_FORMAT = parquet_format;



/* Update policy with EXTERNAL_ID and USER_ARN */
-- DESC INTEGRATION s3_integration_camara; 


CREATE OR REPLACE PIPE pipe_despesas_s3
  AUTO_INGEST = TRUE
AS
COPY INTO despesas_deputados_raw
FROM @stage_despesas_s3
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE STREAM stream_despesas_raw ON TABLE despesas_deputados_raw;

-- PASSO 4: CRIAR A TASK QUE EXECUTA O MERGE
CREATE OR REPLACE TASK task_merge_despesas
  WAREHOUSE = COMPUTE_WH -- Especifique o Warehouse que a task deve usar
  SCHEDULE = 'USING CRON 0/5 * * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('stream_despesas_raw')
AS
MERGE INTO raw.despesas AS target
USING stream_despesas_raw AS source
  ON target.deputado_id = source.deputado_id AND target.cod_Documento = source.codDocumento
WHEN MATCHED THEN
  -- Se a despesa já existe, atualiza os valores
  UPDATE SET
    target.ano = source.ano,
    target.mes = source.mes,
    target.tipo_despesa = source.tipoDespesa,
    target.tipo_Documento = source.tipoDocumento,
    target.cod_Tipo_Documento = source.codTipoDocumento,
    target.data_Documento = source.dataDocumento,
    target.num_Documento = source.numDocumento,
    target.valor_Documento = source.valorDocumento,
    target.url_Documento = source.urlDocumento,
    target.nome_Fornecedor = source.nomeFornecedor,
    target.cnpj_Cpf_Fornecedor = source.cnpjCpfFornecedor,
    target.valor_Liquido = source.valorLiquido,
    target.valor_Glosa = source.valorGlosa,
    target.num_Ressarcimento = source.numRessarcimento,
    target.cod_Lote = source.codLote,
    target.parcela = source.parcela
WHEN NOT MATCHED THEN
  -- Se for uma nova despesa, insere
  INSERT (
    deputado_id, ano, mes, tipo_Despesa, cod_Documento, tipo_Documento, cod_Tipo_Documento,
    data_Documento, num_Documento, valor_Documento, url_Documento, nome_Fornecedor,
    cnpj_Cpf_Fornecedor, valor_Liquido, valor_Glosa, num_Ressarcimento, cod_Lote, parcela
  )
  VALUES (
    source.deputado_id, source.ano, source.mes, source.tipoDespesa, source.codDocumento, source.tipoDocumento, source.codTipoDocumento,
    source.dataDocumento, source.numDocumento, source.valorDocumento, source.urlDocumento, source.nomeFornecedor,
    source.cnpjCpfFornecedor, source.valorLiquido, source.valorGlosa, source.numRessarcimento, source.codLote, source.parcela
  );



  -- PASSO 5: HABILITAR A TASK (Tasks são criadas suspensas por padrão)
  ALTER TASK task_merge_despesas RESUME;
--ALTER TASK task_merge_despesas SUSPEND;


DESC PIPE pipe_despesas_s3;
DESCRIBE TASK raw.task_merge_despesas;


SELECT SYSTEM$PIPE_STATUS('pipe_despesas_s3');

-- Ver os arquivos carregados na última hora
SELECT
    file_name,
    last_load_time,
    status,
    row_count,
    row_parsed,
    first_error_message
FROM
    TABLE(information_schema.copy_history(
        table_name => 'despesas_deputados_raw',
        start_time => DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ));

--ALTER PIPE raw.pipe_despesas_s3 REFRESH;

SELECT COUNT(*), cod_documento,deputado_id FROM Despesas  GROUP BY cod_documento,deputado_id HAVING COUNT(*) > 1;

SELECT * FROM despesas where cod_documento = 5317638;
    
-- Get counts from despesas raw
SELECT COUNT(*) FROM raw.despesas_deputados_raw ;
SELECT  ano, mes, COUNT(*) as n FROM raw.despesas_deputados_raw GROUP BY ano, mes ORDER BY ano, mes;
SELECT * FROM despesas_deputados_raw limit 5;

-- Get counts from despesas
SELECT  ano, mes, COUNT(*) as n FROM raw.despesas 
WHERE ano = 2025 and mes >=6
GROUP BY ano, mes ORDER BY ano, mes;


--SELECT MAX(data_documento) FROM despesas limit 5;

 /*
Before
2025	6	13865
2025	7	3997
 
After
2025	6	14246
2025	7	4748

After 2
2025	6	14313
2025	7	4962

Depois
*/


-- SELECT COUNT(*), urldocumento FROM raw.despesas_deputados_raw GROUP BY urldocumento having count(*) >= 1;
