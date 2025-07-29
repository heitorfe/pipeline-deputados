-- Conecte-se ao seu banco de dados e schema
USE DATABASE SEU_BANCO_DE_DADOS;
USE SCHEMA SUA_SCHEMA;

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

CREATE OR REPLACE STORAGE INTEGRATION s3_integration_camara
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxx:role/snowflake-listen' -- Substitua pelo ARN que você criará no próximo passo
  STORAGE_ALLOWED_LOCATIONS = ('s3://learnsnowflakedbt-heitor/camara/despesas/incremental/');

  DESC INTEGRATION s3_integration_camara;


  -- Cria um formato de arquivo para Parquet
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET;

-- Cria o stage externo que aponta para o S3
CREATE OR REPLACE STAGE stage_despesas_s3
  STORAGE_INTEGRATION = s3_integration_camara
  URL = 's3://learnsnowflakedbt-heitor/camara/despesas/incremental/'
  FILE_FORMAT = parquet_format;

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
  SCHEDULE = '30 3 * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('stream_despesas_raw')
AS
MERGE INTO raw.despesas AS target
USING stream_despesas_raw AS source
  ON target.deputado_id = source.deputado_id AND target.codDocumento = source.codDocumento
WHEN MATCHED THEN
  -- Se a despesa já existe, atualiza os valores
  UPDATE SET
    target.ano = source.ano,
    target.mes = source.mes,
    target.tipoDespesa = source.tipoDespesa,
    target.tipoDocumento = source.tipoDocumento,
    target.codTipoDocumento = source.codTipoDocumento,
    target.dataDocumento = source.dataDocumento,
    target.numDocumento = source.numDocumento,
    target.valorDocumento = source.valorDocumento,
    target.urlDocumento = source.urlDocumento,
    target.nomeFornecedor = source.nomeFornecedor,
    target.cnpjCpfFornecedor = source.cnpjCpfFornecedor,
    target.valorLiquido = source.valorLiquido,
    target.valorGlosa = source.valorGlosa,
    target.numRessarcimento = source.numRessarcimento,
    target.codLote = source.codLote,
    target.parcela = source.parcela
WHEN NOT MATCHED THEN
  -- Se for uma nova despesa, insere
  INSERT (
    deputado_id, ano, mes, tipoDespesa, codDocumento, tipoDocumento, codTipoDocumento,
    dataDocumento, numDocumento, valorDocumento, urlDocumento, nomeFornecedor,
    cnpjCpfFornecedor, valorLiquido, valorGlosa, numRessarcimento, codLote, parcela
  )
  VALUES (
    source.deputado_id, source.ano, source.mes, source.tipoDespesa, source.codDocumento, source.tipoDocumento, source.codTipoDocumento,
    source.dataDocumento, source.numDocumento, source.valorDocumento, source.urlDocumento, source.nomeFornecedor,
    source.cnpjCpfFornecedor, source.valorLiquido, source.valorGlosa, source.numRessarcimento, source.codLote, source.parcela
  );

-- PASSO 5: HABILITAR A TASK (Tasks são criadas suspensas por padrão)
ALTER TASK task_merge_despesas RESUME;