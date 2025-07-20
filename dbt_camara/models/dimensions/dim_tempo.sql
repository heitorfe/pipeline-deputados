{{ config(
    materialized='table'
) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['ano', 'mes']) }} AS sk_tempo,
    ano,
    mes,
    CASE mes
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro' 
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS nome_mes,
    CASE 
        WHEN mes IN (1,2,3) THEN 1
        WHEN mes IN (4,5,6) THEN 2  
        WHEN mes IN (7,8,9) THEN 3
        WHEN mes IN (10,11,12) THEN 4
    END AS trimestre,
    CURRENT_TIMESTAMP() AS data_carga
FROM (
    SELECT DISTINCT ano, mes 
    FROM {{ ref('stg_despesas') }}
    WHERE ano IS NOT NULL AND mes IS NOT NULL
) t
