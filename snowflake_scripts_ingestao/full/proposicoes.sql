USE ROLE role_ingestao;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CAMARA;
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS raw.proposicoes (
    id NUMBER,
    sigla_tipo STRING,
    numero NUMBER,
    ano INT,
    descricao_tipo STRING,
    ementa STRING,
    ementa_detalhada STRING,
    keywords STRING,
    data_apresentacao TIMESTAMP_NTZ,
    status_ambito STRING,
    status_apreciacao STRING,
    status_cod_situacao NUMBER,
    status_cod_tipo_tramitacao STRING,
    status_data_hora TIMESTAMP_NTZ,
    status_descricao_situacao STRING,
    status_descricao_tramitacao STRING,
    status_despacho STRING,
    status_regime STRING,
    status_sigla_orgao STRING,
    status_uri_orgao STRING,
    status_uri_ultimo_relator STRING,
    uri STRING,
    uri_autores STRING,
    uri_orgao_numerador STRING,
    url_inteiro_teor STRING
);

INSERT INTO raw.proposicoes (
    id, sigla_tipo, numero, ano, descricao_tipo, ementa, ementa_detalhada, keywords,
    data_apresentacao, status_ambito, status_apreciacao, status_cod_situacao, status_cod_tipo_tramitacao,
    status_data_hora, status_descricao_situacao, status_descricao_tramitacao, status_despacho,
    status_regime, status_sigla_orgao, status_uri_orgao, status_uri_ultimo_relator,
    uri, uri_autores, uri_orgao_numerador, url_inteiro_teor
)
SELECT
    $1:id::NUMBER,
    $1:siglaTipo::STRING,
    $1:numero::NUMBER,
    $1:ano::INT,
    $1:descricaoTipo::STRING,
    $1:ementa::STRING,
    $1:ementaDetalhada::STRING,
    $1:keywords::STRING,
    TRY_TO_TIMESTAMP($1:dataApresentacao::STRING),
    $1:statusProposicao:ambito::STRING,
    $1:statusProposicao:apreciacao::STRING,
    $1:statusProposicao:codSituacao::NUMBER,
    $1:statusProposicao:codTipoTramitacao::STRING,
    TRY_TO_TIMESTAMP($1:statusProposicao:dataHora::STRING),
    $1:statusProposicao:descricaoSituacao::STRING,
    $1:statusProposicao:descricaoTramitacao::STRING,
    $1:statusProposicao:despacho::STRING,
    $1:statusProposicao:regime::STRING,
    $1:statusProposicao:siglaOrgao::STRING,
    $1:statusProposicao:uriOrgao::STRING,
    $1:statusProposicao:uriUltimoRelator::STRING,
    $1:uri::STRING,
    $1:uriAutores::STRING,
    $1:uriOrgaoNumerador::STRING,
    $1:urlInteiroTeor::STRING
FROM @camara/votos/parquet/
(FILE_FORMAT => 'parquet_format');