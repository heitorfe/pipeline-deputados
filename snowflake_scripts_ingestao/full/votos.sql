USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS raw.votos (
    id_votacao STRING,
    deputado_id NUMBER,
    tipo_voto STRING,
    data_registro_voto TIMESTAMP_NTZ
);

INSERT INTO raw.votos (
    id_votacao, deputado_id, tipo_voto, data_registro_voto
)
SELECT
    $1:id_votacao::STRING,
    $1:deputado_id::NUMBER,
    $1:tipoVoto::STRING,
    TRY_TO_TIMESTAMP($1:dataRegistroVoto::STRING)
FROM @camara/votos/parquet/
(FILE_FORMAT => 'parquet_format');