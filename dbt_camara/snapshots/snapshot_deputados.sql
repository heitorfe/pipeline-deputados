{% snapshot snapshot_deputados %}

{{
    config(
      target_database='CAMARA',
      target_schema='ANALYTICS',
      unique_key='deputado_id',
      strategy='timestamp',
      updated_at='data_atualizacao',
    )
}}

-- Snapshot SCD Type 2 para Deputados baseado em detalhes e histórico
WITH deputados_detalhes_base AS (
    SELECT 
        sdd.deputado_id,
        sdd.cpf,
        sdd.nome_civil,
        sdd.data_nascimento,
        sdd.municipio_nascimento,
        sdd.uf_nascimento,
        sdd.sexo,
        sdd.redes_sociais,
        sdd.ultimo_status_data,
        sdd.ultimo_status_nome AS nome_deputado,
        sdd.ultimo_status_nome_eleitoral AS nome_eleitoral,
        sdd.ultimo_status_sigla_partido AS sigla_partido,
        sdd.ultimo_status_sigla_uf AS sigla_uf,
        sdd.ultimo_status_id_legislatura AS id_legislatura,
        sdd.ultimo_status_condicao_eleitoral AS condicao_eleitoral,
        sdd.ultimo_status_situacao AS situacao,
        sdd.ultimo_status_url_foto AS url_foto,
        sdd.ultimo_status_email_gabinete AS email_gabinete,
        sdd.ultimo_status_telefone_gabinete AS telefone_gabinete,
        sdd.ultimo_status_nome_gabinete AS nome_gabinete,
        sdd.uri,
        'detalhes' AS fonte_origem,
        sdd.data_carga,
        CURRENT_TIMESTAMP() AS data_atualizacao
    FROM {{ ref('stg_deputados_detalhes') }} sdd
    WHERE sdd.deputado_id IS NOT NULL
),

deputados_historico_recente AS (
    SELECT 
        sdh.deputado_id,
        NULL AS cpf,
        sdh.nome_deputado AS nome_civil,  -- Corrigido: usar nome_deputado
        NULL AS data_nascimento,
        NULL AS municipio_nascimento,
        NULL AS uf_nascimento,
        NULL AS sexo,
        NULL AS redes_sociais,
        DATE(sdh.data_inicio_vigencia) AS ultimo_status_data,
        sdh.nome_deputado,
        sdh.nome_eleitoral,
        sdh.sigla_partido,
        sdh.sigla_uf,
        sdh.id_legislatura,
        sdh.condicao_eleitoral,
        sdh.situacao,
        sdh.url_foto,
        NULL AS email_gabinete,
        NULL AS telefone_gabinete,
        NULL AS nome_gabinete,
        sdh.uri AS uri,  -- Corrigido: usar uri ao invés de uri_deputado
        'historico' AS fonte_origem,
        sdh.data_carga,
        sdh.data_inicio_vigencia AS data_atualizacao
    FROM {{ ref('stg_deputado_historico') }} sdh
    WHERE sdh.deputado_id IS NOT NULL
      -- Pegar apenas os registros mais recentes de cada deputado do histórico
      AND sdh.data_inicio_vigencia = (
          SELECT MAX(data_inicio_vigencia) 
          FROM {{ ref('stg_deputado_historico') }} sdh2
          WHERE sdh2.deputado_id = sdh.deputado_id
      )
),

deputados_consolidados AS (
    -- Priorizar dados dos detalhes quando disponíveis
    SELECT 
        deputado_id,
        cpf,
        nome_civil,
        data_nascimento,
        municipio_nascimento,
        uf_nascimento,
        sexo,
        redes_sociais,
        ultimo_status_data,
        nome_deputado,
        nome_eleitoral,
        COALESCE(sigla_partido, 'SEM PARTIDO') AS sigla_partido,
        COALESCE(sigla_uf, uf_nascimento, 'N/A') AS sigla_uf,
        COALESCE(id_legislatura, 0) AS id_legislatura,
        COALESCE(condicao_eleitoral, 'N/A') AS condicao_eleitoral,
        COALESCE(situacao, 'N/A') AS situacao,
        url_foto,
        email_gabinete,
        telefone_gabinete,
        nome_gabinete,
        uri,
        fonte_origem,
        data_carga,
        data_atualizacao
    FROM deputados_detalhes_base
    
    UNION ALL
    
    -- Incluir deputados que existem apenas no histórico
    SELECT 
        dhr.deputado_id,
        dhr.cpf,
        dhr.nome_civil,
        dhr.data_nascimento,
        dhr.municipio_nascimento,
        dhr.uf_nascimento,
        dhr.sexo,
        dhr.redes_sociais,
        dhr.ultimo_status_data,
        dhr.nome_deputado,
        dhr.nome_eleitoral,
        dhr.sigla_partido,
        dhr.sigla_uf,
        dhr.id_legislatura,
        dhr.condicao_eleitoral,
        dhr.situacao,
        dhr.url_foto,
        dhr.email_gabinete,
        dhr.telefone_gabinete,
        dhr.nome_gabinete,
        dhr.uri,
        dhr.fonte_origem,
        dhr.data_carga,
        dhr.data_atualizacao
    FROM deputados_historico_recente dhr
    WHERE dhr.deputado_id NOT IN (
        SELECT deputado_id FROM deputados_detalhes_base
    )
)

SELECT 
    deputado_id,
    cpf,
    nome_civil,
    data_nascimento,
    municipio_nascimento,
    uf_nascimento,
    sexo,
    redes_sociais,
    ultimo_status_data,
    nome_deputado,
    nome_eleitoral,
    sigla_partido,
    sigla_uf,
    id_legislatura,
    condicao_eleitoral,
    situacao,
    url_foto,
    email_gabinete,
    telefone_gabinete,
    nome_gabinete,
    uri,
    fonte_origem,
    
    -- Campos calculados
    CASE 
        WHEN data_nascimento IS NOT NULL 
        THEN DATEDIFF('year', data_nascimento, CURRENT_DATE())
        ELSE NULL 
    END AS idade_atual,
    
    CASE 
        WHEN sexo = 'M' THEN 'MASCULINO'
        WHEN sexo = 'F' THEN 'FEMININO'
        ELSE 'NÃO INFORMADO'
    END AS sexo_descricao,
    
    data_carga,
    data_atualizacao
FROM deputados_consolidados

{% endsnapshot %}
