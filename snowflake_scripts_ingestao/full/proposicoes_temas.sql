USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;


-- 1. Cria tabela final
CREATE TABLE IF NOT EXISTS raw.proposicoes_temas (
    sigla_tipo STRING,
    numero NUMBER,
    ano INT,
    cod_tema NUMBER,
    tema STRING,
    relevancia NUMBER,
    uri_proposicao STRING
);

-- 2. Insere dados diretamente do Stage
INSERT INTO raw.proposicoes_temas (
    sigla_tipo, numero, ano, cod_tema, tema, relevancia, uri_proposicao
)
SELECT
    $1:siglaTipo::STRING,
    $1:numero::NUMBER,
    $1:ano::INT,
    $1:codTema::NUMBER,
    $1:tema::STRING,
    $1:relevancia::NUMBER,
    $1:uriProposicao::STRING
FROM @camara/proposicoes_temas/parquet/;
