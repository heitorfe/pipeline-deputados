USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;


-- 1. Cria tabela final, se não existir
CREATE OR REPLACE TABLE raw.proposicoes_autores (
    id_proposicao NUMBER,
    id_deputado_autor NUMBER,
    nome_autor STRING,
    tipo_autor STRING,
    sigla_partido_autor STRING,
    sigla_uf_autor STRING,
    cod_tipo_autor NUMBER,
    proponente BOOLEAN,
    ordem_assinatura NUMBER,
    uri_autor STRING,
    uri_partido_autor STRING,
    uri_proposicao STRING
);

-- 2. Insere dados diretamente do Stage
INSERT INTO raw.proposicoes_autores (
    id_proposicao, id_deputado_autor, nome_autor, tipo_autor,
    sigla_partido_autor, sigla_uf_autor, cod_tipo_autor, proponente,
    ordem_assinatura, uri_autor, uri_partido_autor, uri_proposicao
)
SELECT
    $1:idProposicao::NUMBER,
    $1:idDeputadoAutor::NUMBER,
    $1:nomeAutor::STRING,
    $1:tipoAutor::STRING,
    $1:siglaPartidoAutor::STRING,
    $1:siglaUFAutor::STRING,
    $1:codTipoAutor::NUMBER,
    IFF($1:proponente::NUMBER = 1, TRUE, FALSE),
    $1:ordemAssinatura::NUMBER,
    $1:uriAutor::STRING,
    $1:uriPartidoAutor::STRING,
    $1:uriProposicao::STRING
FROM @camara/proposicoes_autores/parquet/;

