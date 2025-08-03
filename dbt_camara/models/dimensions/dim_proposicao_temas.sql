{{
  config(
    materialized='table',
    unique_key='cod_tema'
  )
}}

-- Dimensão que contém os temas das proposições legislativas
-- Realiza agregação para garantir unicidade por cod_tema
SELECT 
    cod_tema,
    -- Pega o tema mais frequente para cada código
    MODE(tema) as tema,
    -- Pega a relevância máxima para cada código
    MAX(relevancia) as relevancia,
    COUNT(*) as total_registros_origem
FROM {{ source('camara_raw', 'proposicoes_temas') }}
WHERE cod_tema IS NOT NULL
  AND tema IS NOT NULL
  AND relevancia IS NOT NULL
GROUP BY cod_tema
ORDER BY cod_tema