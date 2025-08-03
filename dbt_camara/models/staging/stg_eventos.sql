{{ config(
    materialized='view'
) }}

-- Staging model para eventos da Câmara dos Deputados
SELECT DISTINCT
    CAST(id AS INTEGER) AS id_evento,
    UPPER(TRIM(descricao)) AS descricao,
    UPPER(TRIM(descricao_tipo)) AS descricao_tipo,
    
    -- Normalizar datas para formato YYYY-MM-DD
    CAST(data_hora_inicio AS TIMESTAMP) AS data_hora_inicio,
    CAST(data_hora_fim AS TIMESTAMP) AS data_hora_fim,
    CAST(DATE(data_hora_inicio) AS DATE) AS data_inicio,
    CAST(DATE(data_hora_fim) AS DATE) AS data_fim,
    
    -- Extrair componentes temporais
    EXTRACT(YEAR FROM data_hora_inicio) AS ano_inicio,
    EXTRACT(MONTH FROM data_hora_inicio) AS mes_inicio,
    EXTRACT(DAY FROM data_hora_inicio) AS dia_inicio,
    EXTRACT(HOUR FROM data_hora_inicio) AS hora_inicio,
    
    -- Calcular duração do evento em horas
    CASE 
        WHEN data_hora_fim IS NOT NULL AND data_hora_inicio IS NOT NULL
        THEN DATEDIFF('hour', data_hora_inicio, data_hora_fim)
        ELSE NULL
    END AS duracao_horas,
    
    -- Classificar período do dia
    CASE 
        WHEN EXTRACT(HOUR FROM data_hora_inicio) BETWEEN 6 AND 11 THEN 'MANHÃ'
        WHEN EXTRACT(HOUR FROM data_hora_inicio) BETWEEN 12 AND 17 THEN 'TARDE'
        WHEN EXTRACT(HOUR FROM data_hora_inicio) BETWEEN 18 AND 23 THEN 'NOITE'
        ELSE 'MADRUGADA'
    END AS periodo_dia,
    
    -- Informações de local
    UPPER(TRIM(local_camara_nome)) AS local_camara_nome,
    UPPER(TRIM(local_externo)) AS local_externo,
    
    -- Verificar se é evento interno ou externo
    CASE 
        WHEN local_camara_nome IS NOT NULL THEN 'INTERNO'
        WHEN local_externo IS NOT NULL THEN 'EXTERNO'
        ELSE 'NÃO_INFORMADO'
    END AS tipo_local,
    
    -- Status e URLs
    UPPER(TRIM(situacao)) AS situacao,
    TRIM(uri) AS uri,
    TRIM(url_documento_pauta) AS url_documento_pauta,
    
    -- Verificar se tem documentação
    CASE 
        WHEN url_documento_pauta IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS tem_documento_pauta,
    
    -- Colunas técnicas
    CURRENT_TIMESTAMP() AS data_carga,
    'camara_eventos' AS fonte_dado

FROM {{ source('camara_raw', 'eventos') }}
WHERE id IS NOT NULL 
  AND data_hora_inicio IS NOT NULL
  AND data_hora_inicio >= '2000-01-01'
  -- Filtrar eventos futuros irreais (mais de 1 ano no futuro)
  AND data_hora_inicio <= DATEADD('year', 1, CURRENT_DATE())
ORDER BY data_hora_inicio DESC
